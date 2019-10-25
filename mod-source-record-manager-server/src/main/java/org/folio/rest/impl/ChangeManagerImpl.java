package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.resource.ChangeManager;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.ChunkProcessingService;
import org.folio.services.JobExecutionService;
import org.folio.spring.SpringContextUtil;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import java.util.Map;

import static java.lang.String.format;

public class ChangeManagerImpl implements ChangeManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeManagerImpl.class);
  @Autowired
  private JobExecutionService jobExecutionService;
  @Autowired
  private ChunkProcessingService chunkProcessingService;
  private String tenantId;

  public ChangeManagerImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void postChangeManagerJobExecutions(InitJobExecutionsRqDto initJobExecutionsRqDto,
                                             Map<String, String> okapiHeaders,
                                             Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
        jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
          .map(initJobExecutionsRsDto ->
            (Response) PostChangeManagerJobExecutionsResponse.respond201WithApplicationJson(initJobExecutionsRsDto))
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.error("Error during initializing JobExecution entities", e, e.getMessage());
        asyncResultHandler.handle(Future.failedFuture(e));
      }
    });
  }

  @Override
  public void putChangeManagerJobExecutionsById(String id, String lang, JobExecution entity, Map<String, String> okapiHeaders,
                                                Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        entity.setId(id);
        OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
        jobExecutionService.updateJobExecutionWithSnapshotStatus(entity, params)
          .map(updatedEntity -> (Response) PutChangeManagerJobExecutionsByIdResponse.respond200WithApplicationJson(updatedEntity))
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.error("Failed to update JobExecution", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getChangeManagerJobExecutionsById(String id, String lang, Map<String, String> okapiHeaders,
                                                Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        jobExecutionService.getJobExecutionById(id, tenantId)
          .map(optionalJobExecution -> optionalJobExecution.orElseThrow(() ->
            new NotFoundException(format("JobExecution with id '%s' was not found", id))))
          .map(GetChangeManagerJobExecutionsByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteChangeManagerJobExecutionsById(String id, String lang, Map<String, String> okapiHeaders,
                                                   Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    asyncResultHandler.handle(Future.succeededFuture(
      PostChangeManagerJobExecutionsRecordsByIdResponse.respond500WithTextPlain("Method is not implemented")));
  }


  @Override
  public void getChangeManagerJobExecutionsChildrenById(String id, int limit, String query, int offset,
                                                        Map<String, String> okapiHeaders,
                                                        Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        jobExecutionService.getJobExecutionCollectionByParentId(id, query, offset, limit, tenantId)
          .map(GetChangeManagerJobExecutionsChildrenByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.error("Failed to retrieve JobExecutions by parent id", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putChangeManagerJobExecutionsStatusById(String id, StatusDto entity, Map<String, String> okapiHeaders,
                                                      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
        jobExecutionService.updateJobExecutionStatus(id, entity, params)
          .map(PutChangeManagerJobExecutionsStatusByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.error("Failed to update status for JobExecution", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putChangeManagerJobExecutionsJobProfileById(String id, JobProfileInfo entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        jobExecutionService.setJobProfileToJobExecution(id, entity, tenantId)
          .map(PutChangeManagerJobExecutionsStatusByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.error("Failed to set JobProfile for JobExecution", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postChangeManagerJobExecutionsRecordsById(String id, RawRecordsDto entity, Map<String, String> okapiHeaders,
                                                        Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
        chunkProcessingService.processChunk(entity, id, params)
          .map(processed -> PostChangeManagerJobExecutionsRecordsByIdResponse.respond204())
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.error("Failed to process chunk of RawRecords with JobExecution id {}", id, e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteChangeManagerJobExecutionsRecordsById(String id, Map<String, String> okapiHeaders,
                                                          Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
        jobExecutionService.deleteJobExecutionAndSRSRecords(id, params)
          .map(deleted -> DeleteChangeManagerJobExecutionsRecordsByIdResponse.respond204())
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.error("Failed to delete records for JobExecution id {}", id, e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }
}
