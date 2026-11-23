package org.folio.services;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_HOLDING;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.getControlFieldValue;

import com.google.common.collect.Lists;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.impl.KafkaHeaderImpl;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.client.SourceStorageBatchClient;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.entity.ConsortiumConfiguration;
import org.folio.services.util.EventHandlingUtil;
import org.folio.services.util.RecordConversionUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Verifies that MARC Holdings records link (via the 004 field) to an existing MARC Bib record, looking the
 * linked ids up in source-record-storage (and in the central tenant for consortia). Holdings that fail
 * the check are turned into error records and reported with a DI_ERROR event.
 */
@Log4j2
@Component
public class MarcHoldingsVerifier {

  static final String HOLDINGS_004_TAG_ERROR_MESSAGE =
    "The 004 tag of the Holdings doesn't has a link to the Bibliographic record";
  private static final String MESSAGE_KEY = "message";
  private static final String TAG_004 = "004";

  private final ConsortiumDataCache consortiumDataCache;
  private final KafkaConfig kafkaConfig;
  private final DistributionKeyGenerator distributionKeyGenerator;
  private final int batchSize;

  public MarcHoldingsVerifier(@Autowired ConsortiumDataCache consortiumDataCache,
                              @Autowired KafkaConfig kafkaConfig,
                              @Autowired DistributionKeyGenerator distributionKeyGenerator,
                              @Value("${marc.holdings.batch.size:100}") int batchSize) {
    this.consortiumDataCache = consortiumDataCache;
    this.kafkaConfig = kafkaConfig;
    this.distributionKeyGenerator = distributionKeyGenerator;
    this.batchSize = batchSize;
  }

  /**
   * Returns the records without the invalid MARC Holdings; the invalid ones are reported with DI_ERROR events.
   * Non-holdings chunks are returned unchanged.
   */
  public Future<List<Record>> filterValidRecords(JobExecution jobExecution, ConnectionParams params, List<Record> records) {
    Promise<List<Record>> promiseFilteredRecords = Promise.promise();

    List<Future<List<String>>> listFuture = executeInBatches(records, batch -> getInvalidMarcBibIdsForConsortium(batch, params));
    filterMarcHoldingsBy004Field(records, listFuture, params, jobExecution, promiseFilteredRecords);
    return promiseFilteredRecords.future();
  }

  /**
   * Marks a holdings record without a usable 004 field as an error record.
   */
  void markMissing004Link(Record record, Object rawContent) {
    log.warn(HOLDINGS_004_TAG_ERROR_MESSAGE);
    record.setParsedRecord(null);
    record.setErrorRecord(new ErrorRecord()
      .withContent(rawContent)
      .withDescription(new JsonObject().put(MESSAGE_KEY, HOLDINGS_004_TAG_ERROR_MESSAGE).encode()));
  }

  private List<Future<List<String>>> executeInBatches(List<Record> recordList,
                                        Function<List<String>, Future<List<String>>> batchOperation) {
    // filter list on MARC_HOLDINGS
    var marcHoldingsIdsToVerify = recordList.stream()
      .filter(recordItem -> recordItem.getRecordType() == MARC_HOLDING)
      .map(recordItem -> getControlFieldValue(recordItem, TAG_004))
      .filter(StringUtils::isNotBlank)
      .toList();
    // split on batches and create list of Futures
    List<List<String>> batches = Lists.partition(marcHoldingsIdsToVerify, batchSize);
    return batches.stream().map(batchOperation).toList();
  }

  private void filterMarcHoldingsBy004Field(List<Record> records, List<Future<List<String>>> batchList, ConnectionParams okapiParams,
                                            JobExecution jobExecution, Promise<List<Record>> promise) {
    Future.all(batchList)
      .onComplete(as -> {
        if (IterableUtils.matchesAll(records, record -> record.getRecordType() == MARC_HOLDING)) {
          var invalidMarcBibIds = batchList
            .stream()
            .map(Future::result)
            .flatMap(Collection::stream)
            .toList();
          log.info("filterMarcHoldingsBy004Field:: MARC_BIB invalid list ids: {}", invalidMarcBibIds);
          var validMarcBibRecords = records.stream()
            .filter(record -> {
              var controlFieldValue = getControlFieldValue(record, TAG_004);
              return isValidMarcHoldings(jobExecution, okapiParams, invalidMarcBibIds, record, controlFieldValue);
            }).toList();
          log.info("filterMarcHoldingsBy004Field:: Total marc holdings records: {}, invalid marc bib ids: {}, valid marc bib records: {}",
            records.size(), invalidMarcBibIds.size(), validMarcBibRecords.size());
          promise.complete(validMarcBibRecords);
        } else {
          promise.complete(records);
        }
      });
  }

  private Future<List<String>> getInvalidMarcBibIdsForConsortium(List<String> marcBibIds,
                                                                 ConnectionParams okapiParams) {
    if (CollectionUtils.isEmpty(marcBibIds)) {
      return Future.succeededFuture(Collections.emptyList());
    }
    return verifyMarcHoldings004Field(marcBibIds, okapiParams)
      .compose(invalidIdsInMember -> {
        if (invalidIdsInMember.isEmpty()) {
          return Future.succeededFuture(Collections.emptyList());
        } else {
          return consortiumDataCache.getConsortiumData(okapiParams)
            .compose(consortiumConfiguration ->
              getCentralTenantInvalidIds(okapiParams, invalidIdsInMember, consortiumConfiguration));
        }
      });
  }

  private Future<List<String>> getCentralTenantInvalidIds(ConnectionParams okapiParams,
                                                          List<String> invalidIdsInMember,
                                                          Optional<ConsortiumConfiguration> consortiumConfiguration) {
    if (consortiumConfiguration.isPresent()) {
      var headers = prepareCentralTenantConnectionParams(okapiParams, consortiumConfiguration.get());
      var params = new ConnectionParams(headers);
      return verifyMarcHoldings004Field(invalidIdsInMember, params);
    }
    return Future.succeededFuture(invalidIdsInMember);
  }

  private Map<String, String> prepareCentralTenantConnectionParams(ConnectionParams okapiParams,
                                                                   ConsortiumConfiguration consortiumConfiguration) {
    var headers = new HashMap<>(okapiParams.getHeaders());
    headers.keySet().stream()
      .filter(key -> key.equalsIgnoreCase(XOkapiHeaders.TENANT))
      .findFirst()
      .ifPresent(key -> headers.put(key, consortiumConfiguration.centralTenantId()));
    return headers;
  }

  private Future<List<String>> verifyMarcHoldings004Field(List<String> marcBibIds, ConnectionParams okapiParams) {
    Promise<List<String>> promise = Promise.promise();
    var sourceStorageBatchClient = getSourceStorageBatchClient(okapiParams);
    try {
      sourceStorageBatchClient.postSourceStorageBatchVerifiedRecords(marcBibIds, asyncResult -> {
        log.info("verifyMarcHoldings004Field:: Verify list of marc bib ids: {} ", marcBibIds);
        List<String> invalidMarcBibIds = new ArrayList<>();
        if (asyncResult.succeeded() && asyncResult.result().statusCode() == 200) {
          var body = asyncResult.result().body();
          log.info("verifyMarcHoldings004Field:: Response from SRS with invalid MARC Bib ids: {}", body);
          var object = new JsonObject(body);
          var ids = object.getJsonArray("invalidMarcBibIds");
          invalidMarcBibIds = ids.getList();
          log.info("verifyMarcHoldings004Field:: List of marc bib ids: {}", invalidMarcBibIds);
        } else {
          log.info("verifyMarcHoldings004Field:: The marc holdings not found in the SRS: {}",
            asyncResult.succeeded()
              ? asyncResult.result() + " and status code: " + asyncResult.result().statusCode()
              : "request failed: " + asyncResult.cause());
        }
        promise.complete(invalidMarcBibIds);
      });
    } catch (Exception e) {
      log.warn("verifyMarcHoldings004Field:: Error during call post request to SRS", e);
      promise.complete(Collections.emptyList());
    }
    return promise.future();
  }

  private boolean isValidMarcHoldings(JobExecution jobExecution, ConnectionParams okapiParams,
                                      List<String> invalidMarcBibIds, Record record, String controlFieldValue) {
    if (isBlank(controlFieldValue) || invalidMarcBibIds.contains(controlFieldValue)) {
      // avoid populating error if there is already populated via 999ff-field error.
      if (record.getErrorRecord() != null && record.getErrorRecord().getDescription().contains("999ff")) {
        return true;
      }
      populateError(record, jobExecution, okapiParams);
      return false;
    }
    return true;
  }

  private void populateError(Record record, JobExecution jobExecution, ConnectionParams okapiParams) {
    var eventPayload = getDataImportPayload(record, jobExecution, okapiParams);
    eventPayload.getContext().put(EventHandlingUtil.RECORD_ID_HEADER, record.getId());
    var key = distributionKeyGenerator.next();
    markMissing004Link(record, record.getRawRecord().getContent());
    var kafkaHeaders = KafkaHeaderUtils.kafkaHeadersFromMap(okapiParams.getHeaders());
    kafkaHeaders.add(new KafkaHeaderImpl(EventHandlingUtil.RECORD_ID_HEADER, record.getId()));

    EventHandlingUtil.sendEventToKafka(okapiParams.getTenantId(), Json.encode(eventPayload), DI_ERROR.value(), kafkaHeaders, kafkaConfig, key)
      .onFailure(
        th -> log.warn("populateError:: Error publishing DI_ERROR event for MARC Holdings record with id {}", record.getId(), th));
  }

  private DataImportEventPayload getDataImportPayload(Record record, JobExecution jobExecution,
                                                      ConnectionParams okapiParams) {
    EntityType sourceRecordKey = RecordConversionUtil.getEntityType(record);
    return new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withProfileSnapshot(jobExecution.getJobProfileSnapshotWrapper())
      .withJobExecutionId(record.getSnapshotId())
      .withOkapiUrl(okapiParams.getConnectionUrl())
      .withTenant(okapiParams.getTenantId())
      .withToken(okapiParams.getToken())
      .withContext(new HashMap<>(Map.of(
        sourceRecordKey.value(), Json.encode(record),
        "ERROR", HOLDINGS_004_TAG_ERROR_MESSAGE)));
  }

  private SourceStorageBatchClient getSourceStorageBatchClient(ConnectionParams okapiParams) {
    var token = okapiParams.getToken();
    var okapiUrl = okapiParams.getConnectionUrl();
    var tenantId = okapiParams.getTenantId();
    return new SourceStorageBatchClient(okapiUrl, tenantId, token);
  }
}
