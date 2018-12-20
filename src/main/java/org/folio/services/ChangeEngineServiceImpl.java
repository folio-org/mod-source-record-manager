package org.folio.services;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.parsers.ParsedResult;
import org.folio.services.parsers.RecordFormat;
import org.folio.services.parsers.SourceRecordParser;
import org.folio.services.parsers.SourceRecordParserBuilder;
import org.folio.util.OkapiConnectionParams;
import org.folio.util.RestUtil;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

public class ChangeEngineServiceImpl implements ChangeEngineService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEngineServiceImpl.class);
  private static final int DEF_CHUNK_NUMBER = 20;

  private static final String RECORD_SERVICE_URL = "/source-storage/record";

  private Vertx vertx;
  private String tenantId;
  private JobExecutionService jobExecutionService;

  public ChangeEngineServiceImpl(Vertx vertx, String tenantId) {
    this.vertx = vertx;
    this.tenantId = tenantId;
    this.jobExecutionService = new JobExecutionServiceImpl(vertx, tenantId);
  }

  @Override
  public Future<JobExecution> parseSourceRecordsForJobExecution(JobExecution job, OkapiConnectionParams params) {
    Future<JobExecution> future = Future.future();
    List<Future> updatedRecordsFuture = new ArrayList<>();
    jobExecutionService.updateJobExecutionStatus(job.getId(), new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS), params)
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
                            RecordCollection recordList = chunks.mapTo(RecordCollection.class);
                            parseRecords(recordList.getRecords(), job)
                              .forEach(record -> updatedRecordsFuture.add(updateRecord(params, job, record)));
                            if ((offset > 0 ? (offset / DEF_CHUNK_NUMBER) : 1) == chunksNumber) {
                              CompositeFuture.all(updatedRecordsFuture)
                                .setHandler(result -> {
                                  StatusDto statusDto = new StatusDto();
                                  if (result.failed()) {
                                    statusDto
                                      .withStatus(StatusDto.Status.ERROR)
                                      .withErrorStatus(StatusDto.ErrorStatus.RECORD_UPDATE_ERROR);
                                    LOGGER.error("Can't update record in storage. jobId: " + job.getId(), result.cause());
                                  } else {
                                    statusDto
                                      .withStatus(StatusDto.Status.PARSING_FINISHED);
                                  }
                                  jobExecutionService.updateJobExecutionStatus(job.getId(), statusDto, params).setHandler(r -> {
                                    if (r.failed()) {
                                      LOGGER.error("Error during update jobExecution and snapshot after parsing", result.cause());
                                      future.fail(r.cause());
                                    } else {
                                      future.complete(job);
                                    }
                                  });
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
  private List<Record> parseRecords(List<Record> records, JobExecution execution) {
    if (records == null || records.isEmpty()) {
      return new ArrayList<>(0);
    }
    SourceRecordParser parser = SourceRecordParserBuilder.buildParser(getRecordFormatByJobExecution(execution));
    for (Record record : records) {
      SourceRecord sourceRecordObject = record.getSourceRecord();
      if (sourceRecordObject == null
        || sourceRecordObject.getSource() == null
        || sourceRecordObject.getSource().isEmpty()) {
        continue;
      }
      ParsedResult parsedResult = parser.parseRecord(sourceRecordObject.getSource());
      if (parsedResult.isHasError()) {
        record.setErrorRecord(new ErrorRecord()
          .withContent(sourceRecordObject.getSource())
          .withDescription(parsedResult.getErrors().encode()));
      } else {
        record.setParsedRecord(new ParsedRecord().withContent(parsedResult.getParsedRecord().encode()));
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
   * Update record entity in record-storage
   *
   * @param params       - okapi params for connecting record-storage
   * @param jobExecution - job execution related to records
   * @param record       - record json object
   */
  private Future<JobExecution> updateRecord(OkapiConnectionParams params, JobExecution jobExecution, Record record) {
    Future<JobExecution> future = Future.future();
    RestUtil.doRequest(params, RECORD_SERVICE_URL + "/" + record.getId(), HttpMethod.PUT, record)
      .setHandler(updateResult -> {
        if (RestUtil.validateAsyncResult(updateResult, future)) {
          future.complete(jobExecution);
        }
      });
    return future;
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
