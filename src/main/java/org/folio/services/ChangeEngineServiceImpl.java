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

public class ChangeEngineServiceImpl implements ChangeEngineService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEngineServiceImpl.class);
  private static final int DEF_CHUNK_NUMBER = 20;
  private static final String HTTP_ERROR_MESSAGE = "Response HTTP code not equals 200. Response code: ";
  private static final String RECORD_SERVICE_URL = "/source-storage/record";
  private static final String SNAPSHOT_SERVICE_URL = "/source-storage/snapshot";

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
                  int chunksNumber = getChunksNumber(totalRecords);
                  for (int i = 0; i < chunksNumber; i++) {
                    final int offset = i == 0 ? 0 : DEF_CHUNK_NUMBER * i;
                    RestUtil.doRequest(params, RECORD_SERVICE_URL + buildQueryForRecordsLoad(job.getId(), offset), HttpMethod.GET, null)
                      .setHandler(loadChunksResult -> {
                        if (loadChunksResult.failed()) {
                          LOGGER.error("Error during getting records for parse for job execution with id: " + job.getId(), loadChunksResult.cause());
                          future.fail(loadChunksResult.cause());
                        } else {
                          JsonObject chunks = loadChunksResult.result().getJson();
                          if (chunks == null || chunks.getJsonArray("records") == null) {
                            String errorMessage = "Error during getting records for parse for job execution with id: " + job.getId();
                            LOGGER.error(errorMessage);
                            future.fail(errorMessage);
                          } else {
                            JsonArray jsonArray = chunks.getJsonArray("records");
                            List<JsonObject> records = new ArrayList<>();
                            for (int j = 0; j < jsonArray.size(); j++) {
                              records.add(jsonArray.getJsonObject(j));
                            }
                            parseRecords(records, job)
                              .forEach(record -> updatedRecordsFuture.add(updateRecord(params, job, record)));
                            if ((offset > 0 ? (offset / DEF_CHUNK_NUMBER) : 1) == chunksNumber) {
                              CompositeFuture.all(updatedRecordsFuture)
                                .setHandler(result -> {
                                  if (result.failed()) {
                                    jobExecutionDao.getJobExecutionById(job.getId())
                                      .setHandler(getJobExecutionResult -> {
                                        if (getJobExecutionResult.succeeded() && getJobExecutionResult.result().isPresent()) {
                                          jobExecutionDao.updateJobExecution(getJobExecutionResult.result().get()
                                            .withErrorStatus(JobExecution.ErrorStatus.RECORD_UPDATE_ERROR)
                                            .withStatus(JobExecution.Status.ERROR))
                                            .setHandler(updateJobStatusResult -> {
                                              String message = "Can't update record in storage. jobId: " + job.getId();
                                              LOGGER.error(message, result.cause());
                                              future.fail(result.cause());
                                            });
                                        } else {
                                          String message = "Can't load job execution for status updating. Error on snapshot update ID: "
                                            + job.getId();
                                          LOGGER.error(message, getJobExecutionResult.cause());
                                          future.fail(getJobExecutionResult.cause());
                                        }
                                      });
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
                        }
                      });
                  }
                }
              }
            });
        }
      });
    return future;
  }

  /**
   * Parse list of source records
   *
   * @param records   - list of source records for parsing
   * @param execution - job execution of record's parsing process
   * @return - list of records with parsed or error data
   */
  private List<JsonObject> parseRecords(List<JsonObject> records, JobExecution execution) {
    SourceRecordParser parser = SourceRecordParserBuilder.buildParser(getRecordFormatByJobExecution(execution));
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

  /**
   * STUB implementation until job profile is't exist
   *
   * @param execution - job execution object
   * @return - Records format for jobExecution's records
   */
  private RecordFormat getRecordFormatByJobExecution(JobExecution execution) {
    return RecordFormat.MARC;
  }

  /**
   * Load jobExecution from DB and update it status
   *
   * @param jobExecutionId - id of jobExecution
   * @param status         - new status for jobExecution
   * @return - updated job execution with new status
   */
  private Future<JobExecution> updateJobExecutionStatus(String jobExecutionId, JobExecution.Status status) {
    return jobExecutionDao.getJobExecutionById(jobExecutionId)
      .compose(optionalJob -> optionalJob
        .map(job -> jobExecutionDao.updateJobExecution(job.withStatus(status)))
        .orElseThrow(NotFoundException::new)
      );
  }

  /**
   * Update snapshot status in record-storage. If this operation is failed - update jobExecution status
   *
   * @param params       - okapi params for connecting record-storage
   * @param jobExecution - jobExecution that relates to snapshot
   */
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
            } else {
              jobExecutionDao.getJobExecutionById(jobExecution.getId())
                .setHandler(getJobExecutionResult -> {
                  if (getJobExecutionResult.succeeded() && getJobExecutionResult.result().isPresent()) {
                    jobExecutionDao.updateJobExecution(getJobExecutionResult.result().get()
                      .withErrorStatus(JobExecution.ErrorStatus.SNAPSHOT_UPDATE_ERROR)
                      .withStatus(JobExecution.Status.ERROR))
                      .setHandler(updateJobStatusResult -> {
                        String message = "Can't update snapshot status. ID: " + jobExecution.getId();
                        LOGGER.error(message);
                        future.fail(message);
                      });
                  } else {
                    String message = "Can't load job execution for status updating. Error on snapshot update ID: "
                      + jobExecution.getId();
                    LOGGER.error(message);
                    future.fail(message);
                  }
                });
            }
          });
        }
      });
    return future;
  }

  /**
   * Update record entity in record-storage
   *
   * @param params       - okapi params for connecting record-storage
   * @param jobExecution - job execution related to records
   * @param record       - record json object
   */
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

  /**
   * Validate http response and fail future if need
   *
   * @param asyncResult - http response callback
   * @param future      - future of callback
   * @return - boolean value is response ok
   */
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
    } else if (asyncResult.result().getCode() == 200 || asyncResult.result().getCode() == 201) {
      return true;
    }
    LOGGER.error(HTTP_ERROR_MESSAGE + asyncResult.result().getCode());
    future.fail(new BadRequestException());
    return false;
  }

  /**
   * Build query for loading records
   *
   * @param jobId  - job execution id
   * @param offset - offset for loading records
   * @return - url query
   */
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
        .append(DEF_CHUNK_NUMBER);
    } catch (Exception e) {
      LOGGER.error("Error during build query for records count", e);
    }
    return queryParams.toString();
  }

  /**
   * @param totalRecords - number of records need to be loaded by chunks
   * @return - number of chunks to load all records
   */
  private int getChunksNumber(Integer totalRecords) {
    if (totalRecords == null || totalRecords < 1) {
      return 0;
    }
    if (totalRecords <= DEF_CHUNK_NUMBER) {
      return 1;
    }
    int chunks = totalRecords / DEF_CHUNK_NUMBER;
    if (totalRecords % DEF_CHUNK_NUMBER > 0) {
      chunks++;
    }
    return chunks;
  }
}
