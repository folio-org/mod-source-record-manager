package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.folio.rest.jaxrs.model.JobExecutionLogDto;
import org.folio.rest.jaxrs.model.JobExecutionSummaryDto;
import org.folio.rest.jaxrs.model.JobLogEntryDtoCollection;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.RecordProcessingLogDto;

import java.util.List;
import java.util.Optional;

/**
 * DAO interface for the JournalRecord entity
 */
public interface JournalRecordDao {

  /**
   * Saves JournalRecord entity to DB
   *
   * @param journalRecord journalRecord entity to save
   * @param tenantId      tenant id
   * @return future with created journalRecord id
   */
  Future<String> save(JournalRecord journalRecord, String tenantId);

  /**
   * Saves chunk of JournalRecord entities to DB
   *
   * @param journalRecords  List of JournalRecord entities to save
   * @param tenantId        tenant id
   * @return future with created JournalRecord entities
   */
  Future<List<RowSet<Row>>> saveBatch(List<JournalRecord> journalRecords, String tenantId);

  /**
   * Searches for JournalRecord entities by jobExecutionId and sorts them using specified sort criteria and direction
   *
   * @param jobExecutionId job execution id
   * @param sortBy         sort criteria
   * @param order          sort direction
   * @param tenantId       tenant id
   * @return future with list of journalRecord entities
   */
  Future<List<JournalRecord>> getByJobExecutionId(String jobExecutionId, String sortBy, String order, String tenantId);

  /**
   * Deletes journal records associated with job execution by specified jobExecutionId
   *
   * @param jobExecutionId job execution id
   * @param tenantId       tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> deleteByJobExecutionId(String jobExecutionId, String tenantId);

  /**
   * Returns JobExecutionLogDto entity for job execution with specified id
   *
   * @param jobExecutionId job execution id
   * @param tenantId       tenant id
   * @return future with JobExecutionLogDto entity
   */
  Future<JobExecutionLogDto> getJobExecutionLogDto(String jobExecutionId, String tenantId);

  /**
   * Searches for JobLogEntryDto entities by jobExecutionId and sorts them using specified sort criteria and direction
   *
   * @param jobExecutionId job execution id
   * @param sortBy         sorting criteria
   * @param order          sorting direction
   * @param limit          limit
   * @param offset         offset
   * @param tenantId       tenantId
   * @return future with JobLogEntryDto collection
   */
  Future<JobLogEntryDtoCollection> getJobLogEntryDtoCollection(String jobExecutionId, String sortBy, String order, int limit, int offset, String tenantId);

  /**
   * Searches for RecordProcessingLogDto entities by jobExecutionId and recordId
   *
   * @param jobExecutionId job execution id
   * @param recordId       record id
   * @param tenantId       tenant id
   * @return future with RecordProcessingLogDto collection
   */
  Future<RecordProcessingLogDto> getRecordProcessingLogDto(String jobExecutionId, String recordId, String tenantId);

  /**
   * Returns JobExecutionSummaryDto for job execution by {@code jobExecutionId}
   *
   * @param jobExecutionId job execution id
   * @param tenantId       tenantId
   * @return future with {@link Optional} of JobExecutionSummaryDto
   */
  Future<Optional<JobExecutionSummaryDto>> getJobExecutionSummaryDto(String jobExecutionId, String tenantId);
}
