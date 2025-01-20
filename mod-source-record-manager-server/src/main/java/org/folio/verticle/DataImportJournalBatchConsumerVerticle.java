package org.folio.verticle;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.flowables.ConnectableFlowable;
import io.reactivex.rxjava3.flowables.GroupedFlowable;
import io.vertx.core.Promise;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import io.vertx.rxjava3.core.AbstractVerticle;
import io.vertx.rxjava3.core.RxHelper;
import io.vertx.rxjava3.core.buffer.Buffer;
import io.vertx.rxjava3.core.eventbus.MessageConsumer;
import io.vertx.rxjava3.impl.AsyncResultCompletable;
import io.vertx.rxjava3.impl.AsyncResultSingle;
import io.vertx.rxjava3.kafka.client.consumer.KafkaConsumer;
import io.vertx.rxjava3.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.rxjava3.kafka.client.producer.KafkaHeader;
import java.time.Duration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.SubscriptionDefinition;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.services.journal.BatchJournalService;
import org.folio.services.journal.BatchableJournalRecord;
import org.folio.services.journal.JournalRecordMapperException;
import org.folio.services.journal.JournalUtil;
import org.folio.util.DataImportEventPayloadWithoutCurrentNode;
import org.folio.util.JournalEvent;
import org.folio.util.SharedDataUtil;
import org.folio.verticle.consumers.util.EventTypeHandlerSelector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.folio.kafka.services.KafkaEnvironmentProperties.environment;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_COMPLETED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_AUTHORITY_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_AUTHORITY_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDING_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDING_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDING_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDING_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_ITEM_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_ITEM_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_ITEM_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_ITEM_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVOICE_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_LOG_SRS_MARC_AUTHORITY_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_LOG_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ORDER_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_RECORD_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_RECORD_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_RECORD_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDING_RECORD_CREATED;
import static org.folio.services.RecordsPublishingServiceImpl.RECORD_ID_HEADER;
import static org.folio.services.util.EventHandlingUtil.constructModuleName;
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;

/**
 * Verticle to write events into journal log. It combines two streams
 * - kafka consumer for specific events defined in {@link DataImportJournalBatchConsumerVerticle#getEvents()}
 * - vert.x event bus for events generated other parts of SRM
 * Marked with SCOPE_PROTOTYPE to support deploying more than 1 instance.
 * @see org.folio.rest.impl.InitAPIImpl
 */
@Component
@Scope(SCOPE_PROTOTYPE)
public class DataImportJournalBatchConsumerVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LogManager.getLogger();

  public static final String DATA_IMPORT_JOURNAL_BATCH_KAFKA_HANDLER_UUID = "ca0c6c56-e74e-4921-b4c9-7b2de53c43ec";

  private static final int MAX_NUM_EVENTS = 100;

  @Autowired
  @Qualifier("newKafkaConfig")
  private KafkaConfig kafkaConfig;

  @Autowired
  EventTypeHandlerSelector eventTypeHandlerSelector;
  @Autowired
  BatchJournalService batchJournalService;

  private KafkaConsumer<String, byte[]> kafkaConsumer;
  private MessageConsumer<Collection<BatchableJournalRecord>> eventBusConsumer;

  private Scheduler scheduler;

  private final CompositeDisposable disposables = new CompositeDisposable();

  @Override
  public void start(Promise<Void> startPromise) {
    scheduler = RxHelper.scheduler(vertx);

    Completable initializedKafkaConsumer = initializeKafkaConsumer();
    Completable initializedEventBusConsumer = initializeEventBusConsumer();
    Completable.mergeArray(initializedEventBusConsumer, initializedKafkaConsumer)
      .doOnComplete(() -> {
        LOGGER.info("Data Import Journal Batch Consumer has started");
        startPromise.complete();
      })
      .doOnError(th -> {
        LOGGER.error("Uncaught exception during initialization of consumers", th);
        startPromise.fail(th);
      })
      .subscribe();

    // Listen to both Kafka events and EventBus messages, merging their streams
    disposables.add(Flowable.merge(listenKafkaEvents(), listenEventBusMessages())
      .window(2, TimeUnit.SECONDS, scheduler, MAX_NUM_EVENTS, true)
      // Save the journal records for each window
      .flatMapCompletable(flowable -> saveJournalRecords(flowable.replay(MAX_NUM_EVENTS))
        .onErrorResumeNext(error -> {
          if (error instanceof RebalanceInProgressException) {
            LOGGER.warn("Rebalance in progress, retrying...", error);
            return Completable.timer(1, TimeUnit.SECONDS) // Retry after a delay
              .andThen(saveJournalRecords(flowable.replay(MAX_NUM_EVENTS)));
          } else {
            LOGGER.error("Error saving journal records, continuing with next batch", error);
            return Completable.complete();
          }
        }))
      .subscribeOn(scheduler)
      .observeOn(scheduler)
      .subscribe()
    );
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    try {
      // Dispose of all subscriptions
      disposables.dispose();

      // Close Kafka consumer
      if (kafkaConsumer != null) {
        kafkaConsumer.close();
      }

      // Close event bus consumer
      if (eventBusConsumer != null) {
        eventBusConsumer.unregister();
      }

      stopPromise.complete();
    } catch (Exception e) {
      LOGGER.error("Error while stopping journal batch verticle", e);
      stopPromise.fail(e);
    }
  }

  public List<String> getEvents() {
    return List.of(
      DI_SRS_MARC_BIB_RECORD_MODIFIED.value(),
      DI_SRS_MARC_BIB_RECORD_UPDATED.value(),
      DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.value(),
      DI_SRS_MARC_AUTHORITY_RECORD_NOT_MATCHED.value(),
      DI_SRS_MARC_HOLDINGS_RECORD_NOT_MATCHED.value(),
      DI_SRS_MARC_HOLDINGS_RECORD_MATCHED.value(),
      DI_INVENTORY_INSTANCE_CREATED.value(),
      DI_INVENTORY_INSTANCE_UPDATED.value(),
      DI_INVENTORY_INSTANCE_NOT_MATCHED.value(),
      DI_INVENTORY_INSTANCE_MATCHED.value(),
      DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value(),
      DI_INVENTORY_HOLDING_CREATED.value(),
      DI_INVENTORY_HOLDING_UPDATED.value(),
      DI_INVENTORY_HOLDING_NOT_MATCHED.value(),
      DI_INVENTORY_HOLDING_MATCHED.value(),
      DI_INVENTORY_ITEM_CREATED.value(),
      DI_INVENTORY_ITEM_UPDATED.value(),
      DI_INVENTORY_ITEM_NOT_MATCHED.value(),
      DI_INVENTORY_ITEM_MATCHED.value(),
      DI_INVENTORY_AUTHORITY_UPDATED.value(),
      DI_INVENTORY_AUTHORITY_NOT_MATCHED.value(),
      DI_INVOICE_CREATED.value(),
      DI_LOG_SRS_MARC_BIB_RECORD_CREATED.value(),
      DI_SRS_MARC_HOLDING_RECORD_CREATED.value(),
      DI_SRS_MARC_HOLDINGS_RECORD_UPDATED.value(),
      DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value(),
      DI_LOG_SRS_MARC_AUTHORITY_RECORD_CREATED.value(),
      DI_ORDER_CREATED_READY_FOR_POST_PROCESSING.value(),
      DI_COMPLETED.value(),
      DI_ERROR.value()
    );
  }

  private Completable initializeKafkaConsumer() {
    KafkaConfig kafkaConfigWithDeserializer = kafkaConfig.toBuilder()
      .consumerValueDeserializerClass("org.apache.kafka.common.serialization.ByteArrayDeserializer")
      .build();

    Map<String, String> consumerProps = kafkaConfigWithDeserializer.getConsumerProps();
    // this is set so that this consumer can start where the non-batch consumer left off, when no previous offset is found.
    consumerProps.put(KafkaConfig.KAFKA_CONSUMER_AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "900000");
    consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");
    consumerProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "20000");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaTopicNameHelper.formatGroupName("DATA_IMPORT_JOURNAL_BATCH",
      environment() + "_" + constructModuleName() + "_" + getClass().getSimpleName()));
    if(SharedDataUtil.getIsTesting(vertx.getDelegate())) {
      // this will allow the consumer to retrieve messages faster during tests
      consumerProps.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
    }

    kafkaConsumer = KafkaConsumer.create(vertx, consumerProps);

    // generate patterns for each event type
    Pattern[] subPatterns = getEvents()
      .stream()
      .map(event -> {
        SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper
          .createSubscriptionDefinition(kafkaConfigWithDeserializer.getEnvId(),
            KafkaTopicNameHelper.getDefaultNameSpace(),
            event);
        return Pattern.compile(subscriptionDefinition.getSubscriptionPattern());
      })
      .toArray(Pattern[]::new);
    // combine all event subscription pattern into one pattern
    Pattern pattern = combinePatterns(subPatterns);

    // KafkaConsumer.subscribe(Pattern) is not exposed for the rxified api
    // it should have been resolved here https://github.com/vert-x3/vertx-kafka-client/issues/156,
    // the code below is the equivalent if the method was exposed. when the issue is resolved, regular APIs can be
    // used.
    io.reactivex.rxjava3.core.Completable ret = AsyncResultCompletable.toCompletable(completionHandler ->
      kafkaConsumer.getDelegate().subscribe(pattern, completionHandler)
    );
    ret = ret.cache();
    ret.subscribe(io.vertx.rxjava3.CompletableHelper.nullObserver());
    return ret;
  }

  private Completable initializeEventBusConsumer() {
    eventBusConsumer =  new MessageConsumer<>(JournalUtil.getJournalMessageConsumer(vertx.getDelegate()));
    return Completable.complete();
  }

  private Flowable<Pair<Optional<Bundle>, Collection<BatchableJournalRecord>>> listenKafkaEvents() {
    if (kafkaConsumer == null) {
      throw new IllegalStateException("KafkaConsumer not initialized");
    }
    return kafkaConsumer.toFlowable()
      .map(consumerRecord -> {
        try {
          Map<String, String> map = kafkaHeadersToMap(consumerRecord.headers());
          OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(map, vertx.getDelegate());
          String recordId = okapiConnectionParams.getHeaders().get(RECORD_ID_HEADER);
          JournalEvent event = DatabindCodec.mapper().readValue(consumerRecord.value(), JournalEvent.class);

          LOGGER.debug("handle:: Event was received with recordId: {} event type: {}", recordId, event.getEventType());
          // Successfully create and return a Bundle object containing the record and event details
          return Optional.of(new Bundle(consumerRecord, event, okapiConnectionParams));
        } catch (Exception e) {
          LOGGER.error("Error processing Kafka event with exception: {}", e.getMessage());
          // Return empty Optional to skip this record and continue processing
          return Optional.<Bundle>empty();
        }
      })
      .filter(Optional::isPresent)
      .map(Optional::get)
      .flatMapSingle(bundle -> createJournalRecords(bundle)
        .map(records -> Pair.of(Optional.of(bundle), records))
        .onErrorReturnItem(Pair.of(Optional.empty(), Collections.emptyList()))
      );
  }

  private Flowable<Pair<Optional<Bundle>, Collection<BatchableJournalRecord>>> listenEventBusMessages() {
    if (eventBusConsumer == null) {
      throw new IllegalStateException("journal eventbus consumer not initialized");
    }

    return eventBusConsumer
      .bodyStream()
      .toFlowable()
      // Flatten the iterable list of messages
      .flatMapIterable(list -> list)
      // Window the messages in 2-second intervals, with a maximum of MAX_NUM_EVENTS per window
      .window(2, TimeUnit.SECONDS, scheduler, MAX_NUM_EVENTS, true)
      .flatMap(window -> window
        // Group messages by tenant ID
        .groupBy(BatchableJournalRecord::getTenantId)
        .flatMapSingle(Flowable::toList)
        .map(journalRecords -> Pair.of(Optional.empty(), journalRecords))
      );
  }

  private Completable saveJournalRecords(ConnectableFlowable<Pair<Optional<Bundle>, Collection<BatchableJournalRecord>>> flowable) {
    LOGGER.debug("saveJournalRecords:: starting to save records.");
    Completable completable = flowable
      // Filter out pairs with empty records
      .filter(pair -> !pair.getRight().isEmpty())
      // Group records by tenant ID
      .groupBy(pair -> {
        Optional<BatchableJournalRecord> first = pair.getRight().stream().findFirst();
        return first.map(BatchableJournalRecord::getTenantId);
      })
      // Process each group of records
      .flatMapCompletable(groupedRecords -> groupedRecords.toList()
        .flatMapCompletable(pairs -> {
            try {
              // Map and collect journal records, setting deterministic identifiers
              Collection<JournalRecord> journalRecords = pairs
                .stream()
                .flatMap(pair -> pair.getRight().stream().map(BatchableJournalRecord::getJournalRecord))
                .map(this::setDeterministicIdentifer)
                .toList();
              // If no records or tenant ID is missing, complete without action
              if (journalRecords.isEmpty() || groupedRecords.getKey().isEmpty()) return Completable.complete();

              LOGGER.info("saveJournalRecords:: Saving {} journal record(s) for tenantId={}", journalRecords.size(), groupedRecords.getKey().get());
              // Save the batch of journal records and handle the response
              return AsyncResultCompletable.toCompletable(completionHandler -> batchJournalService.saveBatchWithResponse(
                  journalRecords,
                  groupedRecords.getKey().get(),
                  completionHandler))
                .onErrorResumeNext(error -> {
                  LOGGER.error("saveJournalRecords:: Error saving batch for tenant {}", groupedRecords.getKey().get(), error);
                  return Completable.complete(); // Continue processing other batches
                });
            } catch (Exception e) {
              LOGGER.error("saveJournalRecords:: Error processing grouped records for tenantId={}", groupedRecords.getKey().orElse("unknown"), e);
              return Completable.complete();
            }
          }
        )
      ).doFinally(() -> {
        // Extract bundles and commit
        Flowable<Bundle> bundleFlowable = flowable
          .map(Pair::getLeft)
          .filter(Optional::isPresent)
          .map(Optional::get);

        disposables.add(
          commitKafkaEvents(bundleFlowable)
            .onErrorResumeNext(error -> {
              LOGGER.error("Error committing Kafka events, will retry on next batch", error);
              return Completable.complete();
            })
            .subscribe(
              () -> LOGGER.debug("Kafka events committed"),
              error -> LOGGER.error("Fatal error committing Kafka events", error)
            )
        );
      })
      .doOnError(throwable -> LOGGER.error("Error occurred while processing journal events", throwable));

    // Connect the ConnectableFlowable to start emitting items
    flowable.connect();

    return completable;
  }

  private Single<Collection<BatchableJournalRecord>> createJournalRecords(Bundle bundle) throws JsonProcessingException, JournalRecordMapperException {
    LOGGER.debug("createJournalRecords :: start to handle bundle.");
    DataImportEventPayloadWithoutCurrentNode eventPayload = bundle.event().getEventPayload();
    String tenantId = bundle.okapiConnectionParams.getTenantId();
    return AsyncResultSingle.toSingle(eventTypeHandlerSelector.getHandler(eventPayload)
        .transform(batchJournalService.getJournalService(), eventPayload, tenantId),
      col -> col.stream().map(res -> new BatchableJournalRecord(res, tenantId)).toList());
  }

  private Completable commitKafkaEvents(Flowable<Bundle> bundles) {  // Changed return type to Completable
    return bundles
      .groupBy(bundle -> new TopicPartition(bundle.record.topic(), bundle.record.partition()))
      .flatMapSingle(this::calculateMaxOffsets)
      .flatMapCompletable(this::commitOffset)
      .doOnError(error -> LOGGER.error("Error committing Kafka offsets", error))
      .onErrorComplete();
  }

  private Single<Map<TopicPartition, OffsetAndMetadata>> calculateMaxOffsets(GroupedFlowable<TopicPartition, Bundle> groupedBundles) {
    return groupedBundles.toList()
      .map(bundles -> {
        long maxOffset = bundles.stream()
          .mapToLong(bundle -> bundle.record.offset())
          .max()
          .orElseThrow(() -> new IllegalStateException("No offsets found in bundle group"));

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(2);
        offsets.put(groupedBundles.getKey(), new OffsetAndMetadata(maxOffset + 1, null));
        return offsets;
      });
  }

  private Completable commitOffset(Map<TopicPartition, OffsetAndMetadata> offsets) {
    LOGGER.info("Committing offsets: {}", offsets);
    return AsyncResultCompletable.toCompletable(handler ->
        kafkaConsumer.getDelegate().commit(offsets, handler)
      )
      .onErrorResumeNext(error -> {
        if (error instanceof RebalanceInProgressException) {
          LOGGER.warn("Rebalance in progress. Retrying offset commit...");
          return kafkaConsumer.rxPoll(Duration.ofMillis(100))
            .flatMapCompletable(records -> commitOffset(offsets));
        }
        return Completable.complete();
      })
      .doOnComplete(() -> LOGGER.info("commitOffset:: Successfully committed offsets: {}", offsets))
      .doOnError(error -> LOGGER.error("commitOffset:: Failed to commit offsets: {}", offsets, error));
  }

  private Map<String, String> kafkaHeadersToMap(List<KafkaHeader> kafkaHeaders) {
    return kafkaHeaders.stream()
      .collect(Collectors.groupingBy(
        KafkaHeader::key,
        Collectors.reducing(StringUtils.EMPTY,
          header -> {
            Buffer value = header.value();
            return value == null ? "" : value.toString();
          },
          (a, b) -> StringUtils.isNotBlank(a) ? a : b)
      ));
  }

  private Pattern combinePatterns(Pattern... patterns) {
    StringBuilder combinedPatternBuilder = new StringBuilder();

    for (int i = 0; i < patterns.length; i++) {
      combinedPatternBuilder.append(patterns[i].pattern());
      if (i < patterns.length - 1) {
        combinedPatternBuilder.append("|");
      }
    }

    return Pattern.compile(combinedPatternBuilder.toString());
  }

  private record Bundle(KafkaConsumerRecord<String, byte[]> record, JournalEvent event,
                        OkapiConnectionParams okapiConnectionParams) {
  }

  private JournalRecord setDeterministicIdentifer(JournalRecord journalRecord) {
    String recordStr = DATA_IMPORT_JOURNAL_BATCH_KAFKA_HANDLER_UUID + // namespace
      journalRecord.getJobExecutionId() +
      journalRecord.getSourceId() +
      journalRecord.getSourceRecordOrder() +
      journalRecord.getEntityType() +
      journalRecord.getEntityId() +
      journalRecord.getActionType() +
      journalRecord.getActionStatus() +
      journalRecord.getTitle() +
      journalRecord.getTenantId();

    // use UUIDv3 to derive deterministic UUIDs with the same input record
    UUID uuidv3 = UUID.nameUUIDFromBytes(recordStr.getBytes());
    journalRecord.setId(uuidv3.toString());
    return journalRecord;
  }


}
