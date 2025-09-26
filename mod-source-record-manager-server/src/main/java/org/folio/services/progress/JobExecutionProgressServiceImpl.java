package org.folio.services.progress;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageProducer;
import org.folio.dao.JobExecutionDao;
import org.folio.dao.JobExecutionProgressDao;
import org.folio.dao.util.DbUtil;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.JobExecutionProgress;
import org.folio.rest.jaxrs.model.Progress;
import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.function.UnaryOperator;

import static java.lang.String.format;
import static org.folio.services.progress.JobExecutionProgressUtil.getBatchJobProgressProducer;

@Service
public class JobExecutionProgressServiceImpl implements JobExecutionProgressService {

  @Autowired
  private JobExecutionProgressDao jobExecutionProgressDao;

  @Autowired
  private PostgresClientFactory pgClientFactory;
  @Autowired
  private JobExecutionDao jobExecutionDao;

  private final MessageProducer<BatchableJobExecutionProgress> jobExecutionProgressMessageProducer;

  public JobExecutionProgressServiceImpl(@Autowired Vertx vertx) {
    this.jobExecutionProgressMessageProducer = getBatchJobProgressProducer(vertx);
  }

  @Override
  public Future<JobExecutionProgress> getByJobExecutionId(String jobExecutionId, String tenantId) {
    return jobExecutionProgressDao.getByJobExecutionId(jobExecutionId, tenantId)
      .map(progress -> progress.orElseThrow(() ->
        new NotFoundException(format("JobExecutionProgress for job with job_execution_id %s was not found", jobExecutionId))));
  }

  @Override
  public Future<JobExecutionProgress> initializeJobExecutionProgress(String jobExecutionId, Integer totalRecords, String tenantId) {
    Progress jobProgress = new Progress().withJobExecutionId(jobExecutionId)
      .withCurrent(0)
      .withTotal(totalRecords);

    PostgresClient pgClient = pgClientFactory.createInstance(tenantId);
    return DbUtil.executeInTransaction(pgClient, connectionAr ->
      jobExecutionProgressDao.initializeJobExecutionProgress(connectionAr, jobExecutionId, totalRecords, tenantId)
        .compose(progress -> jobExecutionDao.updateJobExecutionProgress(connectionAr, jobProgress, tenantId).map(progress))
    );
  }

  @Override
  public Future<JobExecutionProgress> updateJobExecutionProgress(String jobExecutionId, UnaryOperator<JobExecutionProgress> progressMutator, String tenantId) {
    return jobExecutionProgressDao.updateByJobExecutionId(jobExecutionId, progressMutator, tenantId);
  }

  @Override
  public Future<Void> updateCompletionCounts(String jobExecutionId, int successCountDelta, int errorCountDelta, OkapiConnectionParams params) {
    JobExecutionProgress jobExecutionProgress = new JobExecutionProgress().withJobExecutionId(jobExecutionId)
      .withCurrentlySucceeded(successCountDelta)
      .withCurrentlyFailed(errorCountDelta);
    BatchableJobExecutionProgress batchableJobExecutionProgress = new BatchableJobExecutionProgress(params, jobExecutionProgress);
    return jobExecutionProgressMessageProducer.write(batchableJobExecutionProgress);
  }
}
