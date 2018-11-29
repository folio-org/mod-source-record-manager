package org.folio.services;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dao.JobExecutionDao;
import org.folio.dao.JobExecutionDaoImpl;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.services.parsers.ParsedResult;
import org.folio.services.parsers.RecordFormat;
import org.folio.services.parsers.SourceRecordParser;
import org.folio.services.parsers.SourceRecordParserBuilder;
import org.folio.util.OkapiConnectionParams;
import org.folio.util.RestUtil;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ChangeEngineServiceImpl implements ChangeEngineService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEngineServiceImpl.class);
  private static final int DEF_CHUNCK_NUMBER = 20;
  private static final String HTTP_ERROR_MESSAGE = "Response HTTP code not equals 200. Response code: ";
  public static final String RECORD_SERVICE_URL = "/source-storage/record";
  public static final String SNAPSHOT_SERVICE_URL = "/source-storage/snapshot";

  private Vertx vertx;
  private String tenantId;
  private JobExecutionDao jobExecutionDao;

  public ChangeEngineServiceImpl(Vertx vertx, String tenantId) {
    this.vertx = vertx;
    this.tenantId = tenantId;
    this.jobExecutionDao = new JobExecutionDaoImpl(vertx, tenantId);
  }

  @Override
  public Future<JobExecution> parseSourceRecordsForJobExecution(JobExecution job, OkapiConnectionParams params) {
    AtomicInteger counter = new AtomicInteger(0);
    Future<JobExecution> future = Future.future();
    List<Future> updatedRecordsFuture = new ArrayList<>();
    updateJobExecutionStatus(job.getId(), JobExecution.Status.PARSING_IN_PROGRESS)
      .compose(jobExecution -> updateSnapshotStatus(params, jobExecution))
      .setHandler(updateResult -> {
        if (updateResult.failed()) {
          LOGGER.error("Error during updating status for jobExecution and Snapshot", updateResult.cause());
          future.fail(updateResult.cause());
        } else {
          RestUtil.doRequest(params, buildQueryForRecordsLoad(job.getId(), 0), HttpMethod.GET, null)
            .setHandler(countResult -> {
              if (countResult.failed()) {
                LOGGER.error("Error during requesting number of records for jobExecution with id: " + job.getId());
                future.fail(countResult.cause());
              } else {
                JsonObject recordCollection = countResult.result().getJson();
                if (recordCollection == null || recordCollection.getInteger("totalRecords") == null) {
                  String errorMessage = "Error during getting records for count records for job execution with id: " + job.getId();
                  LOGGER.error(errorMessage);
                  future.fail(errorMessage);
                } else {
                  Integer totalRecords = recordCollection.getInteger("totalRecords");
                  while (counter.get() <= totalRecords && totalRecords > 0) {
                    int offset = counter.get();
                    counter.getAndAdd(DEF_CHUNCK_NUMBER);
                    RestUtil.doRequest(params, RECORD_SERVICE_URL + buildQueryForRecordsLoad(job.getId(), offset), HttpMethod.GET, null)
                      .setHandler(loadChunksResult -> {
                        if (loadChunksResult.failed()) {
                          LOGGER.error("Error during getting records for parse for job execution with id: " + job.getId(), loadChunksResult.cause());
                          future.fail(loadChunksResult.cause());
                        } else {
                          JsonObject chunks = countResult.result().getJson();
                          if (chunks == null || chunks.getJsonArray("records") == null) {
                            String errorMessage = "Error during getting records for parse for job execution with id: " + job.getId();
                            LOGGER.error(errorMessage);
                            future.fail(errorMessage);
                          } else {
                            JsonArray jsonArray = chunks.getJsonArray("records");
                            List<JsonObject> records = new ArrayList<>();
                            for (int i = 0; i < jsonArray.size(); i++) {
                              records.add(jsonArray.getJsonObject(i));
                            }
                            parseRecords(records, job)
                              .forEach(record -> updatedRecordsFuture.add(updateRecord(params, job, record)));
                          }
                        }
                      });
                  }
                  CompositeFuture.all(updatedRecordsFuture).setHandler(result -> {
                    if (result.failed()) {
                      LOGGER.error("Error during update parsed records at storage", result.cause());
                      future.fail(result.cause());
                    } else {
                      updateJobExecutionStatus(job.getId(), JobExecution.Status.PARSING_FINISHED)
                        .compose(jobExecution -> updateSnapshotStatus(params, jobExecution))
                        .setHandler(statusUpdate -> {
                          if (statusUpdate.failed()) {
                            LOGGER.error("Error during update jobExecution and snapshot after parsing", result.cause());
                            future.fail(statusUpdate.cause());
                          } else {
                            future.complete(job);
                          }
                        });
                    }
                  });
                }
              }
            });
        }
      });
    return future;
  }

  private List<JsonObject> parseRecords(List<JsonObject> records, JobExecution execution) {
    SourceRecordParser parser = SourceRecordParserBuilder.buildParser(getRecordFormatByJobExecution(execution));
    if (parser == null) {
      String parserErrorMessage = "Parser for JobExecution's records do not exist. JobExecution id: " + execution.getId();
      LOGGER.error(parserErrorMessage);
      throw new UnsupportedOperationException(parserErrorMessage);
    }
    for (JsonObject record : records) {
      JsonObject sourceRecordObject = record.getJsonObject("sourceRecord");
      if (sourceRecordObject == null
        || sourceRecordObject.getString("source") == null
        || sourceRecordObject.getString("source").isEmpty()) {
        continue;
      }
      ParsedResult parsedResult = parser.parseRecord(sourceRecordObject.getString("source"));
      if (parsedResult.isHasError()) {
        record.put("errorRecord", new JsonObject()
          .put("content", sourceRecordObject.getValue("source"))
          .put("description", parsedResult.getErrors().encode())
        );
      } else {
        record.put("parsedRecord", new JsonObject()
          .put("content", parsedResult.getParsedRecord().encode())
        );
      }
    }
    return records;
  }

  private RecordFormat getRecordFormatByJobExecution(JobExecution execution) {
    return RecordFormat.MARC;
  }

  private Future<JobExecution> updateJobExecutionStatus(String jobExecutionId, JobExecution.Status status) {
    return jobExecutionDao.getJobExecutionById(jobExecutionId)
      .compose(optionalJob -> optionalJob
        .map(job -> jobExecutionDao.updateJobExecution(job.withStatus(status)))
        .orElseThrow(NotFoundException::new)
      );
  }

  private Future<JobExecution> updateSnapshotStatus(OkapiConnectionParams params, JobExecution jobExecution) {
    Future<JobExecution> future = Future.future();
    String url = SNAPSHOT_SERVICE_URL + "/" + jobExecution.getId();
    RestUtil.doRequest(params, url, HttpMethod.GET, null)
      .setHandler(getSnapshot -> {
        if (validateAsyncResult(getSnapshot, future)) {
          JsonObject snapshot = getSnapshot.result().getJson().put("status", jobExecution.getStatus().name());
          RestUtil.doRequest(params, url, HttpMethod.PUT, snapshot.encode()).setHandler(updateResult -> {
            if (validateAsyncResult(updateResult, future)) {
              future.complete(jobExecution);
            }
          });
        }
      });
    return future;
  }

  private Future<JobExecution> updateRecord(OkapiConnectionParams params, JobExecution jobExecution, JsonObject record) {
    Future<JobExecution> future = Future.future();
    RestUtil.doRequest(params, RECORD_SERVICE_URL + "/" + record.getString("id"), HttpMethod.PUT, record.encode())
      .setHandler(updateResult -> {
        if (validateAsyncResult(updateResult, future)) {
          future.complete(jobExecution);
        }
      });
    return future;
  }

  private boolean validateAsyncResult(AsyncResult<RestUtil.WrappedResponse> asyncResult, Future future) {
    if (asyncResult.failed()) {
      LOGGER.error("Error during HTTP request to source-storage", asyncResult.cause());
      future.fail(asyncResult.cause());
      return false;
    } else if (asyncResult.result() == null) {
      LOGGER.error("Error during get response from source-storage");
      future.fail(new BadRequestException());
      return false;
    } else if (asyncResult.result().getCode() == 404) {
      LOGGER.error(HTTP_ERROR_MESSAGE + asyncResult.result().getCode());
      future.fail(new NotFoundException());
      return false;
    } else if (asyncResult.result().getCode() == 500) {
      LOGGER.error(HTTP_ERROR_MESSAGE + asyncResult.result().getCode());
      future.fail(new InternalServerErrorException());
      return false;
    } else if (asyncResult.result().getCode() != 200) {
      LOGGER.error(HTTP_ERROR_MESSAGE + asyncResult.result().getCode());
      future.fail(new BadRequestException());
      return false;
    }
    return true;
  }

  private String buildQueryForRecordsLoad(String jobId, int offset) {
    String query = "snapshotId==" + jobId;
    StringBuilder queryParams = new StringBuilder(RECORD_SERVICE_URL);
    try {
      queryParams.append("?")
        .append("query=")
        .append(URLEncoder.encode(query, "UTF-8"))
        .append("&")
        .append("offset=")
        .append(offset)
        .append("&")
        .append("limit=")
        .append(DEF_CHUNCK_NUMBER);
    } catch (Exception e) {
      LOGGER.error("Error during build query for records count", e);
    }
    return queryParams.toString();
  }
}
