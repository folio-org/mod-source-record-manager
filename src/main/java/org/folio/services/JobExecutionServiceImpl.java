package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.folio.dao.JobExecutionDao;
import org.folio.dao.JobExecutionDaoImpl;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollectionDto;
import org.folio.services.converters.JobExecutionToDtoConverter;

/**
 * Implementation of the JobExecutionService, calls JobExecutionDao to access JobExecution metadata.
 * @see JobExecutionService
 * @see JobExecutionDao
 * @see JobExecution
 */
public class JobExecutionServiceImpl implements JobExecutionService {

  private JobExecutionDao jobExecutionDao;
  private JobExecutionToDtoConverter jobExecutionToDtoConverter;

  public JobExecutionServiceImpl(Vertx vertx, String tenantId) {
    this.jobExecutionDao = new JobExecutionDaoImpl(vertx, tenantId);
    this.jobExecutionToDtoConverter = new JobExecutionToDtoConverter();
  }

  public Future<JobExecutionCollectionDto> getCollectionDtoByQuery(String query, int offset, int limit) {
    return jobExecutionDao.getJobExecutions(query, offset, limit)
      .map(jobExecutionCollection -> new JobExecutionCollectionDto()
        .withJobExecutionDtos(jobExecutionToDtoConverter.convert(jobExecutionCollection.getJobExecutions()))
        .withTotalRecords(jobExecutionCollection.getTotalRecords()));
  }

}
