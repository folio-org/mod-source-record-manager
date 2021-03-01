package org.folio.services.progress;

import io.vertx.core.Future;
import org.folio.dao.JobExecutionProgressDao;
import org.folio.rest.jaxrs.model.JobExecutionProgress;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.function.UnaryOperator;

import static java.lang.String.format;

@Service
public class JobExecutionProgressServiceImpl implements JobExecutionProgressService {

  @Autowired
  private JobExecutionProgressDao jobExecutionProgressDao;

  @Override
  public Future<JobExecutionProgress> getByJobExecutionId(String jobExecutionId, String tenantId) {
    return jobExecutionProgressDao.getByJobExecutionId(jobExecutionId, tenantId)
      .map(progress -> progress.orElseThrow(() ->
        new NotFoundException(format("JobExecutionProgress for jobExecution with id '%s' was not found", jobExecutionId))));
  }

  @Override
  public Future<JobExecutionProgress> initializeJobExecutionProgress(String jobExecutionId, Integer totalRecords, String tenantId) {
    return jobExecutionProgressDao.initializeJobExecutionProgress(jobExecutionId, totalRecords, tenantId);
  }

  @Override
  public Future<JobExecutionProgress> updateJobExecutionProgress(String jobExecutionId, UnaryOperator<JobExecutionProgress> progressMutator, String tenantId) {
    return jobExecutionProgressDao.updateByJobExecutionId(jobExecutionId, progressMutator, tenantId);
  }
}
