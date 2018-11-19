package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.folio.dao.JobExecutionDao;
import org.folio.dao.JobExecutionDaoImpl;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.jaxrs.model.JobExecutionCollectionDto;
import org.folio.services.converters.JobExecutionToDtoConverter;

import java.util.Optional;
import java.util.UUID;

/**
 * Implementation of the JobExecutionService, calls JobExecutionDao to access JobExecution metadata.
 * @see JobExecutionService
 * @see JobExecutionDao
 * @see JobExecution
 */
public class JobExecutionServiceImpl implements JobExecutionService {

  private JobExecutionDao dao;
  private JobExecutionToDtoConverter jobExecutionToDtoConverter;

  public JobExecutionServiceImpl(Vertx vertx, String tenantId) {
    this.dao = new JobExecutionDaoImpl(vertx, tenantId);
    this.jobExecutionToDtoConverter = new JobExecutionToDtoConverter();
  }

  @Override
  public Future<JobExecutionCollection> getByQuery(String query, int offset, int limit) {
    return dao.getByQuery(query, offset, limit)
      .map(results -> new JobExecutionCollection()
        .withJobExecutions(results.getResults())
        .withTotalRecords(results.getResultInfo().getTotalRecords()));
  }

  public Future<JobExecutionCollectionDto> getCollectionDtoByQuery(String query, int offset, int limit) {
    return getByQuery(query, offset, limit)
      .map(jobExecutions -> new JobExecutionCollectionDto()
        .withJobExecutionDtos(jobExecutionToDtoConverter.convert(jobExecutions.getJobExecutions()))
        .withTotalRecords(jobExecutions.getTotalRecords()));
  }

  @Override
  public Future<Optional<JobExecution>> getById(String id) {
    return dao.getById(id);
  }

  @Override
  public Future<JobExecution> save(JobExecution jobExecution) {
    String jobExecutionId = UUID.randomUUID().toString();
    jobExecution.setId(jobExecutionId);
    return dao.save(jobExecutionId, jobExecution);
  }

  @Override
  public Future<JobExecution> update(JobExecution jobExecution) {
    return dao.update(jobExecution.getId(), jobExecution);
  }

  @Override
  public Future<Boolean> delete(String id) {
    return dao.delete(id);
  }
}
