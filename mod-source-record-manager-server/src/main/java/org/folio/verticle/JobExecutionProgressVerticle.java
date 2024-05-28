package org.folio.verticle;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.flowables.GroupedFlowable;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.lang.String.format;
import static org.folio.rest.jaxrs.model.JobExecution.Status.CANCELLED;
import static org.folio.rest.jaxrs.model.JobExecution.Status.COMMITTED;
import static org.folio.services.progress.JobExecutionProgressUtil.BATCH_JOB_PROGRESS_ADDRESS;
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;


/**
 * A Verticle that handles the processing and updating of job execution progress.
 */
@Component
@Scope(SCOPE_PROTOTYPE)
public class JobExecutionProgressVerticle extends AbstractVerticle {
  private static final Logger LOGGER = LogManager.getLogger();
  private static final int MAX_NUM_EVENTS = 100;

  private final JobExecutionProgressDao jobExecutionProgressDao;
  private final JobExecutionService jobExecutionService;
  private Scheduler scheduler;

  public JobExecutionProgressVerticle(@Autowired JobExecutionProgressDao jobExecutionProgressDao,
                                      @Autowired JobExecutionService jobExecutionService) {
    this.jobExecutionProgressDao = jobExecutionProgressDao;
    this.jobExecutionService = jobExecutionService;
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
          .flatMapCompletable(progressMaybe -> saveJobExecutionProgress(progressMaybe))
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
    if (progress.getTotal().equals(progress.getCurrentlySucceeded() + progress.getCurrentlyFailed())) {
      return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
        .compose(jobExecutionOptional -> jobExecutionOptional
          .map(jobExecution -> {
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
                .withJobExecutionId(jobExecutionId)
                .withCurrent(progress.getCurrentlySucceeded() + progress.getCurrentlyFailed())
                .withTotal(progress.getTotal()));

            return jobExecutionService.updateJobExecutionWithSnapshotStatus(jobExecution, params)
              .compose(updatedExecution -> {
                if (updatedExecution.getSubordinationType().equals(JobExecution.SubordinationType.COMPOSITE_CHILD)) {

                  return jobExecutionService.getJobExecutionById(updatedExecution.getParentJobId(), params.getTenantId())
                    .map(v -> v.orElseThrow(() -> new IllegalStateException("Could not find parent job execution")))
                    .compose(parentExecution ->
                      jobExecutionService.getJobExecutionCollectionByParentId(parentExecution.getId(), 0, Integer.MAX_VALUE, params.getTenantId())
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
                          if (Boolean.TRUE.equals(allChildrenCompleted)) {
                            LOGGER.info("All children for job {} have completed!", parentExecution.getId());
                            parentExecution.withStatus(JobExecution.Status.COMMITTED)
                              .withUiStatus(JobExecution.UiStatus.RUNNING_COMPLETE)
                              .withCompletedDate(new Date());
                            return jobExecutionService.updateJobExecutionWithSnapshotStatus(parentExecution, params);
                          }
                          return Future.succeededFuture(parentExecution);
                        })
                    );
                } else {
                  return Future.succeededFuture(updatedExecution);
                }
              })
              .map(true);
          })
          .orElse(Future.failedFuture(format("Couldn't find JobExecution for update status and progress with id '%s'", jobExecutionId))));
    }
    return Future.succeededFuture(false);
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
