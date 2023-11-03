package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.IncomingRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

import static java.lang.String.format;
import static org.folio.rest.persist.PostgresClient.convertToPsqlStandard;

@Repository
public class IncomingRecordDaoImpl implements IncomingRecordDao {

  private static final Logger LOGGER = LogManager.getLogger();
  public static final String INCOMING_RECORDS_TABLE = "incoming_records";
  private static final String INSERT_SQL = "INSERT INTO %s.%s (id, job_execution_id, incoming_record) VALUES ($1, $2, $3)";

  @Autowired
  private PostgresClientFactory pgClientFactory;

  @Override
  public Future<List<RowSet<Row>>> saveBatch(List<IncomingRecord> incomingRecords, String tenantId) {
    LOGGER.info("saveBatch:: Save IncomingRecord entity to the {} table", INCOMING_RECORDS_TABLE);
    Promise<List<RowSet<Row>>> promise = Promise.promise();
    try {
      String query = format(INSERT_SQL, convertToPsqlStandard(tenantId), INCOMING_RECORDS_TABLE);
      List<Tuple> tuples = incomingRecords.stream().map(this::prepareInsertQueryParameters).toList();
      LOGGER.debug("IncomingRecordDaoImpl:: Save query = {}; tuples = {}", query, tuples);
      pgClientFactory.createInstance(tenantId).execute(query, tuples, promise);
    } catch (Exception e) {
      LOGGER.warn("saveBatch:: Error saving IncomingRecord entity", e);
      promise.fail(e);
    }
    return promise.future().onFailure(e -> LOGGER.warn("saveBatch:: Error saving IncomingRecord entity", e));
  }

  private Tuple prepareInsertQueryParameters(IncomingRecord incomingRecord) {
    return Tuple.of(UUID.fromString(incomingRecord.getId()), UUID.fromString(incomingRecord.getJobExecutionId()),
      JsonObject.mapFrom(incomingRecord));
  }
}
