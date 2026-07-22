package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.IncomingRecord;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;
import static org.folio.rest.persist.PostgresClient.convertToPsqlStandard;

@Log4j2
@Repository
@RequiredArgsConstructor
public class IncomingRecordDaoImpl implements IncomingRecordDao {

  public static final String INCOMING_RECORDS_TABLE = "incoming_records";
  private static final String GET_BY_ID_SQL = "SELECT * FROM %s.%s WHERE id = $1";
  private static final String INSERT_SQL = "INSERT INTO %s.%s (id, job_execution_id, incoming_record) VALUES ($1, $2, $3)";

  private final PostgresClientFactory pgClientFactory;

  @Override
  public Future<Optional<IncomingRecord>> getById(String id, String tenantId) {
    log.debug("getById:: Get IncomingRecord by id = {} from the {} table", id, INCOMING_RECORDS_TABLE);
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String query = format(GET_BY_ID_SQL, convertToPsqlStandard(tenantId), INCOMING_RECORDS_TABLE);
      pgClientFactory.createInstance(tenantId).selectRead(query, Tuple.of(UUID.fromString(id)), promise::handle);
    } catch (Exception e) {
      log.warn("getById:: Error getting IncomingRecord by id", e);
      promise.fail(e);
    }
    return promise.future().map(rowSet -> rowSet.rowCount() == 0 ? Optional.empty()
      : Optional.of(mapRowToIncomingRecord(rowSet.iterator().next())));
  }

  @Override
  public Future<List<RowSet<Row>>> saveBatch(List<IncomingRecord> incomingRecords, String tenantId) {
    log.debug("saveBatch:: Save IncomingRecord entity to the {} table", INCOMING_RECORDS_TABLE);
    Promise<List<RowSet<Row>>> promise = Promise.promise();
    try {
      String query = format(INSERT_SQL, convertToPsqlStandard(tenantId), INCOMING_RECORDS_TABLE);
      List<Tuple> tuples = incomingRecords.stream().map(this::prepareInsertQueryParameters).toList();
      log.debug("IncomingRecordDaoImpl:: Save query = {}; tuples = {}", query, tuples);
      pgClientFactory.createInstance(tenantId).execute(query, tuples, promise::handle);
    } catch (Exception e) {
      log.warn("saveBatch:: Error saving IncomingRecord entity", e);
      promise.fail(e);
    }
    return promise.future().onFailure(e -> log.warn("saveBatch:: Error saving IncomingRecord entity", e));
  }

  private IncomingRecord mapRowToIncomingRecord(Row row) {
    JsonObject jsonObject = row.getJsonObject("incoming_record");
    return new IncomingRecord().withId(String.valueOf(row.getUUID("id")))
      .withJobExecutionId(String.valueOf(row.getUUID("job_execution_id")))
      .withRecordType(IncomingRecord.RecordType.fromValue(jsonObject.getString("recordType")))
      .withOrder(jsonObject.getInteger("order"))
      .withRawRecordContent(jsonObject.getString("rawRecordContent"))
      .withParsedRecordContent(jsonObject.getString("parsedRecordContent"));
  }

  private Tuple prepareInsertQueryParameters(IncomingRecord incomingRecord) {
    return Tuple.of(UUID.fromString(incomingRecord.getId()), UUID.fromString(incomingRecord.getJobExecutionId()),
      JsonObject.mapFrom(incomingRecord));
  }
}
