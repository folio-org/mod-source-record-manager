package org.folio.verticle;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.flowables.GroupedFlowable;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.producer.impl.KafkaHeaderImpl;
import io.vertx.rxjava3.core.AbstractVerticle;
import io.vertx.rxjava3.core.RxHelper;
import io.vertx.rxjava3.core.eventbus.EventBus;
import io.vertx.rxjava3.core.eventbus.Message;
import io.vertx.rxjava3.core.eventbus.MessageConsumer;
import io.vertx.rxjava3.impl.AsyncResultSingle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.JobExecutionProgressDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.JobExecutionDtoCollection;
import org.folio.rest.jaxrs.model.JobExecutionProgress;
import org.folio.rest.jaxrs.model.Progress;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.JobExecutionService;
import org.folio.services.Status;
import org.folio.services.progress.BatchableJobExecutionProgress;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.lang.String.format;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_JOB_COMPLETED;
import static org.folio.rest.jaxrs.model.JobExecution.Status.CANCELLED;
import static org.folio.rest.jaxrs.model.JobExecution.Status.COMMITTED;
import static org.folio.services.progress.JobExecutionProgressUtil.BATCH_JOB_PROGRESS_ADDRESS;
import static org.folio.services.util.EventHandlingUtil.sendEventToKafka;
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;


/**
 * A Verticle that handles the processing and updating of job execution progress.
 */
@Component
@Scope(SCOPE_PROTOTYPE)
public class JobExecutionProgressVerticle extends AbstractVerticle {
  private static final Logger LOGGER = LogManager.getLogger();
  private static final int MAX_NUM_EVENTS = 100;
  private static final int MAX_DISTRIBUTION = 100;
  private static final String USER_ID_HEADER = "userId";
  private static final String JOB_EXECUTION_ID_HEADER = "jobExecutionId";
  public static final EnumSet<JobExecution.UiStatus> COMPLETED_STATUSES = EnumSet.of(
    JobExecution.UiStatus.RUNNING_COMPLETE,
    JobExecution.UiStatus.ERROR,
    JobExecution.UiStatus.CANCELLED,
    JobExecution.UiStatus.DISCARDED
  );

  private final JobExecutionProgressDao jobExecutionProgressDao;
  private final JobExecutionService jobExecutionService;
  private static final AtomicInteger indexer = new AtomicInteger();
  private Scheduler scheduler;

  private final KafkaConfig kafkaConfig;

  @Autowired
  public JobExecutionProgressVerticle(JobExecutionProgressDao jobExecutionProgressDao,
                                      JobExecutionService jobExecutionService,
                                      @Qualifier("newKafkaConfig") KafkaConfig kafkaConfig) {
    this.jobExecutionProgressDao = jobExecutionProgressDao;
    this.jobExecutionService = jobExecutionService;
    this.kafkaConfig = kafkaConfig;
  }


  /**
   * Starts the verticle, initializes the RxJava scheduler and event bus consumer.
   */
  @Override
  public void start(Promise<Void> startPromise) {
    this.scheduler = RxHelper.scheduler(vertx);

    EventBus eb = vertx.eventBus();
    MessageConsumer<BatchableJobExecutionProgress> jobExecutionProgressConsumer = eb.localConsumer(BATCH_JOB_PROGRESS_ADDRESS);
    consumeJobExecutionProgress(jobExecutionProgressConsumer);

    LOGGER.info("JobExecutionProgressVerticle started");
    startPromise.complete();
  }

  /**
   * Consumes job execution progress messages, groups them, and processes them.
   *
   * @param jobExecutionProgressConsumer  the consumer for job execution progress messages
   */
  private void consumeJobExecutionProgress(MessageConsumer<BatchableJobExecutionProgress> jobExecutionProgressConsumer) {
    jobExecutionProgressConsumer
      .toFlowable()
      // break up messages into chunks. This is crucial for groupBy operations downstream, else those operations will
      // not complete since the end of the stream is never reached.
      .window(1, TimeUnit.SECONDS, scheduler, MAX_NUM_EVENTS)
      .flatMapCompletable(flowable ->
        groupByTenantIdAndJobExecutionId(flowable)
          .map(groupedMessages -> reduceManyJobExecutionProgressObjectsIntoSingleJobExecutionProgress(groupedMessages.toList(), groupedMessages.getKey().jobExecutionId()))
          .flatMapCompletable(this::saveJobExecutionProgress)
      )
      .subscribeOn(scheduler)
      .observeOn(scheduler)
      .subscribe();
  }

  /**
   * Groups job execution progress messages by tenant ID and job execution ID.
   *
   * @param messageFlowable  a flowable of job execution progress messages
   * @return a flowable of grouped job execution progress messages
   */
  private Flowable<GroupedFlowable<JobExecutionProgressGroupKey, Message<BatchableJobExecutionProgress>>> groupByTenantIdAndJobExecutionId(Flowable<Message<BatchableJobExecutionProgress>> messageFlowable) {
    return messageFlowable
      .groupBy(msg -> new JobExecutionProgressGroupKey(msg.body().getParams().getTenantId(), msg.body().getJobExecutionProgress().getJobExecutionId()));
  }

  /**
   * Reduces multiple job execution progress objects into a single job execution progress object.
   *
   * @param batch  a single containing a list of job execution progress messages
   * @param jobExecutionId  the job execution ID
   * @return a maybe containing the reduced job execution progress object
   */
  private Maybe<BatchableJobExecutionProgress> reduceManyJobExecutionProgressObjectsIntoSingleJobExecutionProgress(
    Single<List<Message<BatchableJobExecutionProgress>>> batch,
    String jobExecutionId) {
    return batch
      .flatMapMaybe(list -> {
          JobExecutionProgress reduced = list.stream()
            .reduce(new JobExecutionProgress(),
              (acc, msg) -> {
                acc.setCurrentlyFailed(acc.getCurrentlyFailed() + msg.body().getJobExecutionProgress().getCurrentlyFailed());
                acc.setCurrentlySucceeded(acc.getCurrentlySucceeded() + msg.body().getJobExecutionProgress().getCurrentlySucceeded());
                return acc;
              },
              (progress1, progress2) -> {
                // add up success and error counts for the two JobExecutionProgress objects
                progress1.setCurrentlyFailed(progress1.getCurrentlyFailed() + progress2.getCurrentlyFailed());
                progress1.setCurrentlySucceeded(progress1.getCurrentlySucceeded() + progress2.getCurrentlySucceeded());
                return progress1;
              })
            .withJobExecutionId(jobExecutionId);
          Optional<Message<BatchableJobExecutionProgress>> messageOptional = list.stream().findFirst();
          return messageOptional.map(batchableJobExecutionProgressMessage ->
              Maybe.just(new BatchableJobExecutionProgress(batchableJobExecutionProgressMessage.body().getParams(), reduced)))
            .orElseGet(Maybe::empty);
        }
      );
  }

  /**
   * Saves the job execution progress.
   *
   * @param jobExecutionProgress  a maybe containing the job execution progress to save
   * @return a completable representing the save operation
   */
  private Completable saveJobExecutionProgress(Maybe<BatchableJobExecutionProgress> jobExecutionProgress) {
    return jobExecutionProgress
      .flatMapCompletable(batchableJobExecutionProgress -> {
          String jobExecutionId = batchableJobExecutionProgress.getJobExecutionProgress().getJobExecutionId();
          Integer currentlySucceeded = batchableJobExecutionProgress.getJobExecutionProgress().getCurrentlySucceeded();
          Integer currentlyFailed = batchableJobExecutionProgress.getJobExecutionProgress().getCurrentlyFailed();
          String tenantId = batchableJobExecutionProgress.getParams().getTenantId();
          LOGGER.info("Updating job execution progress for jobExecutionId={} tenantId={}", jobExecutionId, tenantId);
          return AsyncResultSingle.toSingle(jobExecutionProgressDao
                .updateCompletionCounts(jobExecutionId, currentlySucceeded, currentlyFailed, tenantId),
              Function.identity())
            .doOnError(throwable -> {
              LOGGER.error("Something happened during batch update of job progress tenantId={} jobExecutionId={}",
                tenantId,
                jobExecutionId,
                throwable);
              updateJobStatusToError(jobExecutionId, batchableJobExecutionProgress.getParams());
            })
            .doOnSuccess(progress -> {
              LOGGER.info("Updated job execution progress for jobExecutionId={} tenantId={}", jobExecutionId, tenantId);
              updateJobExecutionIfAllRecordsProcessed(progress.getJobExecutionId(),
                progress,
                batchableJobExecutionProgress.getParams());
            })
            .onErrorComplete()
            .ignoreElement();
        }
      )
      .onErrorComplete();
  }

  /**
   * Updates the job execution if all records are processed.
   *
   * @param jobExecutionId  the job execution ID
   * @param progress  the job execution progress
   * @param params  the Okapi connection parameters
   * @return a future containing a boolean indicating the success of the update
   */
  private Future<Boolean> updateJobExecutionIfAllRecordsProcessed(String jobExecutionId, JobExecutionProgress progress, OkapiConnectionParams params) {
    if (progress.getTotal() <= progress.getCurrentlySucceeded() + progress.getCurrentlyFailed()) {
      return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
        .compose(jobExecutionOptional -> jobExecutionOptional
          .map(jobExecution -> updateJobExecutionWithSnapshotStatus(jobExecution, progress, params)
            .compose(updatedExecution -> {
              if (updatedExecution.getSubordinationType().equals(JobExecution.SubordinationType.COMPOSITE_CHILD)) {
                LOGGER.info("COMPOSITE_CHILD subordination type for job {}. Processing...", updatedExecution.getId());

                return jobExecutionService.getJobExecutionById(updatedExecution.getParentJobId(), params.getTenantId())
                  .compose(parentJobOptional ->
                    parentJobOptional
                      .map(parentExecution -> {
                        if (COMPLETED_STATUSES.contains(parentExecution.getUiStatus())) {
                          LOGGER.info("Parent job {} already has completed status. Skipping update.", parentExecution.getId());
                          return Future.succeededFuture(parentExecution);
                        } else {
                          return jobExecutionService.getJobExecutionCollectionByParentId(parentExecution.getId(), 0, Integer.MAX_VALUE, params.getTenantId())
                            .map(JobExecutionDtoCollection::getJobExecutions)
                            .map(children ->
                              children.stream()
                                .filter(child -> child.getSubordinationType().equals(JobExecutionDto.SubordinationType.COMPOSITE_CHILD))
                                .allMatch(child ->
                                  Arrays.asList(
                                    JobExecutionDto.UiStatus.RUNNING_COMPLETE,
                                    JobExecutionDto.UiStatus.CANCELLED,
                                    JobExecutionDto.UiStatus.ERROR,
                                    JobExecutionDto.UiStatus.DISCARDED
                                  ).contains(child.getUiStatus())
                                )
                            )
                            .compose(allChildrenCompleted -> {
                              if (Boolean.TRUE.equals(allChildrenCompleted) && (!COMMITTED.equals(parentExecution.getStatus())))  {
                                LOGGER.info("All children for job {} have completed!", parentExecution.getId());

                                parentExecution.withStatus(JobExecution.Status.COMMITTED)
                                  .withUiStatus(JobExecution.UiStatus.RUNNING_COMPLETE)
                                  .withCompletedDate(new Date());

                                return jobExecutionService.updateJobExecutionWithSnapshotStatusAsync(parentExecution, params)
                                  .compose(updatedJobExecution -> {
                                    sendDiJobCompletedEvent(updatedJobExecution, params);
                                    return Future.succeededFuture(updatedJobExecution);
                                  });
                              }
                              return Future.succeededFuture(parentExecution);
                            });
                        }
                      })
                      .orElse(Future.failedFuture(format("Couldn't find parent job execution with id %s", updatedExecution.getParentJobId())))
                  );
              } else {
                if (updatedExecution.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_SINGLE) ||
                  updatedExecution.getSubordinationType().equals(JobExecution.SubordinationType.CHILD)) {
                  LOGGER.info("{}  subordination type for job {}. Processing...", updatedExecution.getSubordinationType(), updatedExecution.getId());
                  sendDiJobCompletedEvent(updatedExecution, params);
                }
                return Future.succeededFuture(updatedExecution);
              }
            })
            .map(true))
          .orElse(Future.failedFuture(format("Couldn't find JobExecution for update status and progress with jobExecutionId='%s'", jobExecutionId))));
    }
    return Future.succeededFuture(false);
  }

  private Future<JobExecution> updateJobExecutionWithSnapshotStatus(JobExecution jobExecution, JobExecutionProgress progress,
                                                                    OkapiConnectionParams params) {
    if (COMPLETED_STATUSES.contains(jobExecution.getUiStatus())) {
      LOGGER.info("updateJobExecutionWithSnapshotStatus:: JobExecution with jobExecutionId='{}' is already completed with {} status, skipping job update",
        jobExecution.getId(), jobExecution.getStatus());
      return Future.succeededFuture(jobExecution);
    }

    JobExecution.Status statusToUpdate;
    if (jobExecution.getStatus() == CANCELLED && jobExecution.getUiStatus() == JobExecution.UiStatus.CANCELLED) {
      statusToUpdate = CANCELLED;
    } else if (progress.getCurrentlyFailed() == 0) {
      statusToUpdate = COMMITTED;
    } else {
      statusToUpdate = JobExecution.Status.ERROR;
    }
    jobExecution.withStatus(statusToUpdate)
      .withUiStatus(JobExecution.UiStatus.fromValue(Status.valueOf(statusToUpdate.name()).getUiStatus()))
      .withCompletedDate(new Date())
      .withProgress(new Progress()
        .withJobExecutionId(jobExecution.getId())
        .withCurrent(progress.getCurrentlySucceeded() + progress.getCurrentlyFailed())
        .withTotal(progress.getTotal()));

    return jobExecutionService.updateJobExecutionWithSnapshotStatus(jobExecution, params);
  }

  private void sendDiJobCompletedEvent(JobExecution jobExecution, OkapiConnectionParams params) {
    var kafkaHeaders = KafkaHeaderUtils.kafkaHeadersFromMultiMap(params.getHeaders());
    kafkaHeaders.removeIf(header -> JOB_EXECUTION_ID_HEADER.equals(header.key()));
    kafkaHeaders.add(new KafkaHeaderImpl(JOB_EXECUTION_ID_HEADER, jobExecution.getId()));
    kafkaHeaders.add(new KafkaHeaderImpl(USER_ID_HEADER, jobExecution.getUserId()));
    var key = String.valueOf(indexer.incrementAndGet() % MAX_DISTRIBUTION);
    sendEventToKafka(params.getTenantId(), Json.encode(jobExecution), DI_JOB_COMPLETED.value(), kafkaHeaders, kafkaConfig, key)
      .onSuccess(event -> LOGGER.info("sendDiJobCompletedEvent:: DI_JOB_COMPLETED event published, jobExecutionId={}", jobExecution.getId()))
      .onFailure(event -> LOGGER.warn("sendDiJobCompletedEvent:: Error publishing DI_JOB_COMPLETED event, jobExecutionId={}", jobExecution.getId(), event));
  }

  private Future<JobExecution> updateJobStatusToError(String jobExecutionId, OkapiConnectionParams params) {
    return jobExecutionService.updateJobExecutionStatus(jobExecutionId, new StatusDto()
      .withStatus(StatusDto.Status.ERROR)
      .withErrorStatus(StatusDto.ErrorStatus.FILE_PROCESSING_ERROR), params);
  }

  /**
   * A record representing a key for grouping job execution progress messages.
   *
   * @param tenantId  the tenant ID
   * @param jobExecutionId  the job execution ID
   */
  private record JobExecutionProgressGroupKey(String tenantId, String jobExecutionId) {
  }
}
