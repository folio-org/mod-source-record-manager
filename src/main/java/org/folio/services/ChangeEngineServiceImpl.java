package org.folio.services;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.handler.impl.HttpStatusException;
import org.folio.HttpStatus;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.client.SourceStorageClient;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.parsers.ParsedResult;
import org.folio.services.parsers.RecordFormat;
import org.folio.services.parsers.SourceRecordParser;
import org.folio.services.parsers.SourceRecordParserBuilder;

import java.util.ArrayList;
import java.util.List;

public class ChangeEngineServiceImpl implements ChangeEngineService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEngineServiceImpl.class);
  private static final int DEF_CHUNK_NUMBER = 20;

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
    jobExecutionService.updateJobExecutionStatus(job.getId(), new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS), params)
      .setHandler(updateResult -> {
        if (updateResult.failed()) {
          LOGGER.error("Error during updating status for jobExecution and Snapshot", updateResult.cause());
          future.fail(updateResult.cause());
        } else {
          receiveTotalRecordsCountByJobId(job.getId(), params)
            .compose(recordsCount -> parseRecordsByJob(job, params, recordsCount))
            .setHandler(ar -> {
              if (ar.failed()) {
                String errorMessage = String.format("Error during parse records for job execution with id: %s, cause: %s", job.getId(), ar.cause().getMessage());
                LOGGER.error(errorMessage);
                future.fail(errorMessage);
              } else {
                future.complete(job);
              }
            });
        }
      });
    return future;
  }

  private Future<Integer> receiveTotalRecordsCountByJobId(String jobId, OkapiConnectionParams params) {
    Future<Integer> future = Future.future();
    SourceStorageClient client = new SourceStorageClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.getSourceStorageRecord(buildQueryByJobId(jobId), 0, DEF_CHUNK_NUMBER, null, countResult -> {
        if (countResult.statusCode() != HttpStatus.HTTP_OK.toInt()) {
          LOGGER.error("Error during requesting number of records for jobExecution with id: {}", jobId);
          future.fail(new HttpStatusException(countResult.statusCode(), "Error during requesting number of records for jobExecution with id: " + jobId));
        } else {
          countResult.bodyHandler(buffer -> {
            JsonObject recordCollection = buffer.toJsonObject();
            if (recordCollection == null || recordCollection.getInteger("totalRecords") == null) {
              String errorMessage = "Error during getting records for count records for job execution with id: " + jobId;
              LOGGER.error(errorMessage);
              future.fail(errorMessage);
            } else {
              future.complete(recordCollection.getInteger("totalRecords"));
            }
          });
        }
      });
    } catch (Exception e) {
      LOGGER.error("Error during requesting number of records for jobExecution with id: {}", jobId, e, e.getMessage());
      future.fail(e);
    }
    return future;
  }

  private Future<JobExecution> parseRecordsByJob(JobExecution job, OkapiConnectionParams params, int recordsCount) { //NOSONAR
    Future<JobExecution> future = Future.future();
    List<Future> updatedRecordsFuture = new ArrayList<>();
    SourceStorageClient client = new SourceStorageClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    int chunksNumber = getChunksNumber(recordsCount);
    try {
      for (int i = 0; i < chunksNumber; i++) {
        final int offset = i == 0 ? 0 : DEF_CHUNK_NUMBER * i;
        client.getSourceStorageRecord(buildQueryByJobId(job.getId()), offset, DEF_CHUNK_NUMBER, null, loadChunksResponse -> {
          if (loadChunksResponse.statusCode() != HttpStatus.HTTP_OK.toInt()) {
            LOGGER.error("Error during getting records for parse for job execution with id: {}", job.getId());
            future.fail(new HttpStatusException(loadChunksResponse.statusCode(), "Error during getting records for parse for job execution with id: " + job.getId()));
          } else {
            loadChunksResponse.bodyHandler(chunksBuffer -> {
              JsonObject chunks = chunksBuffer.toJsonObject();
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
                        LOGGER.error("Can't update record in storage. jobId: {}", job.getId(), result.cause());
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
            });
          }
        });
      }
    } catch (Exception e) {
      LOGGER.error("Error during getting records for parse for job execution with id: {}", job.getId(), e, e.getMessage());
      future.fail(e);
    }
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
      RawRecord sourceRecordObject = record.getRawRecord();
      if (sourceRecordObject == null
        || sourceRecordObject.getContent() == null
        || sourceRecordObject.getContent().isEmpty()) {
        continue;
      }
      ParsedResult parsedResult = parser.parseRecord(sourceRecordObject.getContent());
      if (parsedResult.isHasError()) {
        record.setErrorRecord(new ErrorRecord()
          .withContent(sourceRecordObject.getContent())
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
    SourceStorageClient client = new SourceStorageClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.putSourceStorageRecordById(record.getId(), record, response -> {
        if (response.statusCode() == HttpStatus.HTTP_OK.toInt()) {
          future.complete(jobExecution);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Error during update record with id: {}", record.getId(), e, e.getMessage());
      future.fail(e);
    }
    return future;
  }

  /**
   * Build query for loading records by job id
   *
   * @param jobId - job execution id
   * @return - query
   */
  private String buildQueryByJobId(String jobId) {
    return "snapshotId==" + jobId;
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
