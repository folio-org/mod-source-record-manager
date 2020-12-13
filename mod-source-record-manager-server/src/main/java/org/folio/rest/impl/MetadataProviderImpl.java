package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.model.MetadataProviderJobLogEntriesJobExecutionIdGetOrder;
import org.folio.rest.jaxrs.model.MetadataProviderJournalRecordsJobExecutionIdGetOrder;
import org.folio.rest.jaxrs.resource.MetadataProvider;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.JobExecutionService;
import org.folio.services.JournalRecordService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import java.util.Map;

public class MetadataProviderImpl implements MetadataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataProviderImpl.class);
  @Autowired
  private JobExecutionService jobExecutionService;
  @Autowired
  private JournalRecordService journalRecordService;
  private String tenantId;

  public MetadataProviderImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void getMetadataProviderJobExecutions(String query, int offset, int limit, Map<String, String> okapiHeaders,
                                               Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        jobExecutionService.getJobExecutionsWithoutParentMultiple(query, offset, limit, tenantId)
          .map(GetMetadataProviderJobExecutionsResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getMetadataProviderLogsByJobExecutionId(String jobExecutionId, Map<String, String> okapiHeaders,
                                                      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        jobExecutionService.getJobExecutionById(jobExecutionId, tenantId)
          .map(jobExecutionOptional -> jobExecutionOptional.orElseThrow(() ->
            new NotFoundException(String.format("JobExecution with id '%s' was not found", jobExecutionId))))
          .compose(jobExecution -> journalRecordService.getJobExecutionLogDto(jobExecutionId, tenantId))
          .map(GetMetadataProviderLogsByJobExecutionIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getMetadataProviderJournalRecordsByJobExecutionId(String jobExecutionId, String sortBy, MetadataProviderJournalRecordsJobExecutionIdGetOrder order,
    Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    vertxContext.runOnContext(v -> {
      try {
        jobExecutionService.getJobExecutionById(jobExecutionId, tenantId)
          .map(jobExecutionOptional -> jobExecutionOptional.orElseThrow(() ->
            new NotFoundException(String.format("JobExecution with id '%s' was not found", jobExecutionId))))
          .compose(jobExecution -> journalRecordService.getJobExecutionJournalRecords(jobExecutionId, sortBy, order.name(), tenantId))
          .map(GetMetadataProviderJournalRecordsByJobExecutionIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getMetadataProviderJobLogEntriesByJobExecutionId(String jobExecutionId, String sortBy, MetadataProviderJobLogEntriesJobExecutionIdGetOrder order,
    int offset, int limit, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    vertxContext.runOnContext(v -> {
      try {
        journalRecordService.getJobLogEntryDtoCollection(jobExecutionId, sortBy, order.name(), limit, offset, tenantId)
          .map(GetMetadataProviderJobLogEntriesByJobExecutionIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.error("Failed to retrieve JobLogEntryDto entities by JobExecution id", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }


}
