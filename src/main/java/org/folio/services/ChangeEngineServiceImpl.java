package org.folio.services;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.HttpStatus;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.client.SourceStorageClient;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.parsers.ParsedResult;
import org.folio.services.parsers.RawRecordParser;
import org.folio.services.parsers.RawRecordParserBuilder;
import org.folio.services.parsers.RecordFormat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ChangeEngineServiceImpl implements ChangeEngineService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEngineServiceImpl.class);

  private JobExecutionService jobExecutionService;

  public ChangeEngineServiceImpl(Vertx vertx, String tenantId) {
    this.jobExecutionService = new JobExecutionServiceImpl(vertx, tenantId);
  }

  @Override
  public Future<RawRecordsDto> parseRawRecordsChunkForJobExecution(RawRecordsDto chunk, JobExecution job, OkapiConnectionParams params) {
    Future<RawRecordsDto> future = Future.future();
    List<Future> createRecordsFuture = new ArrayList<>();
    parseRecords(chunk.getRecords(), job)
      .forEach(record -> createRecordsFuture.add(postRecord(params, job, record)));
    CompositeFuture.all(createRecordsFuture)
      .setHandler(result -> {
        StatusDto statusDto = new StatusDto();
        if (result.failed()) {
          statusDto
            .withStatus(StatusDto.Status.ERROR)
            .withErrorStatus(StatusDto.ErrorStatus.RECORD_UPDATE_ERROR);
          LOGGER.error("Can't create record in storage. jobId: {}", job.getId(), result.cause());
          jobExecutionService.updateJobExecutionStatus(job.getId(), statusDto, params)
            .setHandler(r -> {
              if (r.failed()) {
                LOGGER.error("Error during update jobExecution and snapshot status", result.cause());
              }
              future.fail(result.cause());
            });
        }
        future.complete(chunk);
      });
    return future;
  }

  /**
   * Parse list of source records
   *
   * @param rawRecords - list of raw records for parsing
   * @param execution  - job execution of record's parsing process
   * @return - list of records with parsed or error data
   */
  private List<Record> parseRecords(List<String> rawRecords, JobExecution execution) {
    if (rawRecords == null || rawRecords.isEmpty()) {
      return Collections.emptyList();
    }
    List<Record> records = new ArrayList<>(rawRecords.size());
    RawRecordParser parser = RawRecordParserBuilder.buildParser(getRecordFormatByJobExecution(execution));
    for (String rawRecord : rawRecords) {
      if (rawRecord.isEmpty()) {
        continue;
      }
      ParsedResult parsedResult = parser.parseRecord(rawRecord);
      Record record = new Record()
        .withSnapshotId(execution.getId())
        .withRawRecord(new RawRecord()
          .withContent(rawRecord));
      if (parsedResult.isHasError()) {
        record.setErrorRecord(new ErrorRecord()
          .withContent(rawRecord)
          .withDescription(parsedResult.getErrors().encode()));
      } else {
        record.setParsedRecord(new ParsedRecord()
          .withContent(parsedResult.getParsedRecord().encode()));
      }
      records.add(record);
    }
    return records;
  }

  /**
   * STUB implementation until job profile is't exist
   *
   * @param execution - job execution object
   * @return - Records format for jobExecution's records
   */
  private RecordFormat getRecordFormatByJobExecution(JobExecution execution) { //NOSONAR
    return RecordFormat.MARC;
  }

  /**
   * Update record entity in record-storage
   *
   * @param params       - okapi params for connecting record-storage
   * @param jobExecution - job execution related to records
   * @param record       - record json object
   */
  private Future<JobExecution> postRecord(OkapiConnectionParams params, JobExecution jobExecution, Record record) {
    Future<JobExecution> future = Future.future();
    SourceStorageClient client = new SourceStorageClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.postSourceStorageRecords(null, record, response -> {
        if (response.statusCode() == HttpStatus.HTTP_CREATED.toInt()) {
          future.complete(jobExecution);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Error during POST new record", e, e.getMessage());
      future.fail(e);
    }
    return future;
  }
}
