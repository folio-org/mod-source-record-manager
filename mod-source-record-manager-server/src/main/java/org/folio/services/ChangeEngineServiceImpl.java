package org.folio.services;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.folio.HttpStatus;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.client.SourceStorageClient;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.parsers.ParsedResult;
import org.folio.services.parsers.RecordParser;
import org.folio.services.parsers.RecordParserBuilder;
import org.folio.services.parsers.RecordFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.folio.rest.RestVerticle.MODULE_SPECIFIC_ARGS;

@Service
public class ChangeEngineServiceImpl implements ChangeEngineService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEngineServiceImpl.class);
  private static final int THRESHOLD_CHUNK_SIZE =
    Integer.parseInt(MODULE_SPECIFIC_ARGS.getOrDefault("chunk.processing.threshold.chunk.size", "100"));

  private JobExecutionSourceChunkDao jobExecutionSourceChunkDao;
  private JobExecutionService jobExecutionService;

  public ChangeEngineServiceImpl(@Autowired JobExecutionSourceChunkDao jobExecutionSourceChunkDao,
                                 @Autowired JobExecutionService jobExecutionService) {
    this.jobExecutionSourceChunkDao = jobExecutionSourceChunkDao;
    this.jobExecutionService = jobExecutionService;
  }

  @Override
  public Future<List<Record>> parseRawRecordsChunkForJobExecution(RawRecordsDto chunk, JobExecution jobExecution, String sourceChunkId, OkapiConnectionParams params) {
    Future<List<Record>> future = Future.future();
    List<Future> createRecordsFuture = new ArrayList<>();
    List<Record> parsedRecords = parseRecords(chunk.getRecords(), jobExecution, sourceChunkId, params.getTenantId());
    parsedRecords.forEach(record -> createRecordsFuture.add(postRecord(params, jobExecution, record)));
    CompositeFuture.all(createRecordsFuture)
      .setHandler(result -> {
        StatusDto statusDto = new StatusDto();
        if (result.failed()) {
          statusDto
            .withStatus(StatusDto.Status.ERROR)
            .withErrorStatus(StatusDto.ErrorStatus.RECORD_UPDATE_ERROR);
          LOGGER.error("Can't create record in storage. JobExecution ID: {}", jobExecution.getId(), result.cause());
          jobExecutionService.updateJobExecutionStatus(jobExecution.getId(), statusDto, params)
            .setHandler(r -> {
              if (r.failed()) {
                LOGGER.error("Error during update jobExecution and snapshot status", result.cause());
              }
              future.fail(result.cause());
            });
          jobExecutionSourceChunkDao.getById(sourceChunkId, params.getTenantId())
            .compose(optional -> optional
              .map(sourceChunk -> jobExecutionSourceChunkDao.update(sourceChunk.withState(JobExecutionSourceChunk.State.ERROR), params.getTenantId()))
              .orElseThrow(() -> new NotFoundException(String.format(
                "Couldn't update failed jobExecutionSourceChunk status to ERROR, jobExecutionSourceChunk with id %s was not found", sourceChunkId))));
        } else {
          future.complete(parsedRecords);
        }
      });
    return future;
  }

  /**
   * Parse list of source records
   *
   * @param rawRecords    - list of raw records for parsing
   * @param jobExecution  - job execution of record's parsing process
   * @param sourceChunkId - id of the JobExecutionSourceChunk
   * @param tenantId      - tenant id
   * @return - list of records with parsed or error data
   */
  private List<Record> parseRecords(List<String> rawRecords, JobExecution jobExecution, String sourceChunkId, String tenantId) {
    if (CollectionUtils.isEmpty(rawRecords)) {
      return Collections.emptyList();
    }
    RecordParser parser = RecordParserBuilder.buildParser(getRecordFormatByJobExecution(jobExecution), rawRecords.get(0));
    MutableInt counter = new MutableInt();
    // if number of records is more than THRESHOLD_CHUNK_SIZE update the progress every 20% of processed records,
    // otherwise update it once after all the records are processed
    int partition = rawRecords.size() > THRESHOLD_CHUNK_SIZE ? rawRecords.size() / 5 : rawRecords.size();
    return rawRecords.stream()
      .map(rawRecord -> {
        ParsedResult parsedResult = parser.parseRecord(rawRecord);
        Record record = new Record()
          .withId(UUID.randomUUID().toString())
          .withMatchedId(getMatchedIdFromParsedResult(parsedResult))
          .withRecordType(Record.RecordType.valueOf(jobExecution.getJobProfileInfo().getDataType().value()))
          .withSnapshotId(jobExecution.getId())
          .withRawRecord(new RawRecord().withContent(rawRecord));
        if (parsedResult.isHasError()) {
          record.setErrorRecord(new ErrorRecord()
            .withContent(rawRecord)
            .withDescription(parsedResult.getErrors().encode()));
        } else {
          record.setParsedRecord(new ParsedRecord().withContent(parsedResult.getParsedRecord().encode()));
        }
        return record;
      })
      .peek(stat -> { //NOSONAR
        if (counter.incrementAndGet() % partition == 0) {
          LOGGER.info("Parsed {} records out of {}", counter.intValue(), rawRecords.size());
          jobExecutionSourceChunkDao.getById(sourceChunkId, tenantId)
            .compose(optional -> optional
              .map(sourceChunk -> jobExecutionSourceChunkDao.update(sourceChunk.withProcessedAmount(sourceChunk.getProcessedAmount() + counter.intValue()), tenantId))
              .orElseThrow(() -> new NotFoundException(String.format(
                "Couldn't update jobExecutionSourceChunk progress, jobExecutionSourceChunk with id %s was not found", sourceChunkId))));
        }
      }).collect(Collectors.toList());
  }

  /**
   * Get records format for parser from Job Execution
   *
   * @param jobExecution - job execution object
   * @return - Records format for jobExecution's records
   */
  private RecordFormat getRecordFormatByJobExecution(JobExecution jobExecution) {
    return RecordFormat.getByDataType(jobExecution.getJobProfileInfo().getDataType());
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
        } else {
          String message = String.format("Couldn't create new Record with JobExecution id %s in source-record-storage, response code %s", jobExecution.getId(), response.statusCode());
          LOGGER.error(message);
          future.fail(message);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Error during POST new record", e, e.getMessage());
      future.fail(e);
    }
    return future;
  }

  private String getMatchedIdFromParsedResult(ParsedResult parsedResult) { //NOSONAR
    // STUB implementation
    return UUID.randomUUID().toString();
  }
}
