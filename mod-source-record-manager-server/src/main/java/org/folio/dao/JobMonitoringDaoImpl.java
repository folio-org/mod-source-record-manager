package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.JobMonitoring;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;
import static org.folio.rest.persist.PostgresClient.convertToPsqlStandard;

@Repository
public class JobMonitoringDaoImpl implements JobMonitoringDao {
  private static final Logger LOGGER = LogManager.getLogger();
  private static final String TABLE = "job_monitoring";
  private static final String INSERT_SQL = "INSERT INTO %s.%s (id, job_execution_id, last_event_timestamp, notification_sent) VALUES ($1, $2, $3, $4)";
  private static final String SELECT_BY_JOB_EXECUTION_ID_QUERY = "SELECT id, job_execution_id, last_event_timestamp, notification_sent FROM %s.%s WHERE job_execution_id = $1";
  private static final String UPDATE_BY_JOB_EXECUTION_ID_QUERY = "UPDATE %s.%s SET last_event_timestamp = $1, notification_sent = $2 WHERE job_execution_id = $3";
  private static final String DELETE_BY_JOB_EXECUTION_ID_QUERY = "DELETE FROM %s.%s WHERE job_execution_id = $1";

  @Autowired
  private PostgresClientFactory pgClientFactory;

  @Override
  public Future<Optional<JobMonitoring>> getByJobExecutionId(String jobExecutionId, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String query = format(SELECT_BY_JOB_EXECUTION_ID_QUERY, convertToPsqlStandard(tenantId), TABLE);
    Tuple queryParams = Tuple.of(UUID.fromString(jobExecutionId));
    pgClientFactory.createInstance(tenantId).select(query, queryParams, promise);
    return promise.future().map(this::mapRowToJobMonitoring);
  }

  private Optional<JobMonitoring> mapRowToJobMonitoring(RowSet<Row> resultSet) {
    Iterator<Row> iterator = resultSet.iterator();
    if (!iterator.hasNext()) {
      return Optional.empty();
    } else {
      Row row = iterator.next();
      JobMonitoring jobMonitoring = new JobMonitoring();
      jobMonitoring.setId(row.getUUID("id").toString());
      jobMonitoring.setJobExecutionId(row.getUUID("job_execution_id").toString());
      jobMonitoring.setLastEventTimestamp(Date.from(row.getLocalDateTime("last_event_timestamp").toInstant(ZoneOffset.UTC)));
      jobMonitoring.setNotificationSent(row.getBoolean("notification_sent"));
      return Optional.of(jobMonitoring);
    }
  }

  @Override
  public Future<String> save(JobMonitoring jobMonitoring, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String query = format(INSERT_SQL, convertToPsqlStandard(tenantId), TABLE);
      Tuple queryParams = Tuple.of(
        UUID.fromString(jobMonitoring.getId()),
        UUID.fromString(jobMonitoring.getJobExecutionId()),
        LocalDateTime.ofInstant(Instant.ofEpochMilli(jobMonitoring.getLastEventTimestamp().getTime()), ZoneOffset.UTC),
        jobMonitoring.getNotificationSent()
      );
      pgClientFactory.createInstance(tenantId).execute(query, queryParams, promise);
    } catch (Exception e) {
      LOGGER.error("Error saving JobMonitoring entity", e);
      promise.fail(e);
    }
    return promise.future().map(jobMonitoring.getId()).onFailure(e -> LOGGER.error("Error saving JobMonitoring entity", e));
  }

  @Override
  public Future<Boolean> updateByJobExecutionId(String jobExecutionId, Date lastEventTimeStamp, boolean notificationSent, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String query = format(UPDATE_BY_JOB_EXECUTION_ID_QUERY, convertToPsqlStandard(tenantId), TABLE);
    Tuple queryParams = Tuple.of(
      LocalDateTime.ofInstant(Instant.ofEpochMilli(lastEventTimeStamp.getTime()), ZoneOffset.UTC),
      notificationSent,
      jobExecutionId
    );
    pgClientFactory.createInstance(tenantId).execute(query, queryParams, promise);
    return promise.future().map(updateResult -> updateResult.rowCount() == 1);
  }

  @Override
  public Future<Boolean> deleteByJobExecutionId(String jobExecutionId, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String query = format(DELETE_BY_JOB_EXECUTION_ID_QUERY, convertToPsqlStandard(tenantId), TABLE);
    Tuple queryParams = Tuple.of(UUID.fromString(jobExecutionId));
    pgClientFactory.createInstance(tenantId).execute(query, queryParams, promise);
    return promise.future().map(updateResult -> updateResult.rowCount() == 1);
  }
}
