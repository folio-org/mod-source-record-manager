package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.rest.jaxrs.model.FileExtension;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.resource.ChangeManager;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.FileExtensionService;
import org.folio.services.FileExtensionServiceImpl;
import org.folio.services.JobExecutionService;
import org.folio.services.JobExecutionServiceImpl;
import org.folio.util.ExceptionHelper;
import org.folio.util.OkapiConnectionParams;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import java.util.Map;

public class ChangeManagerImpl implements ChangeManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeManagerImpl.class);
  private JobExecutionService jobExecutionService;
  private FileExtensionService fileExtensionService;

  public ChangeManagerImpl(Vertx vertx, String tenantId) {
    String calculatedTenantId = TenantTool.calculateTenantId(tenantId);
    this.jobExecutionService = new JobExecutionServiceImpl(vertx, calculatedTenantId);
    this.fileExtensionService = new FileExtensionServiceImpl(vertx, calculatedTenantId);
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
  public void putChangeManagerJobExecutionById(String id, JobExecution entity, Map<String, String> okapiHeaders,
                                               Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        entity.setId(id);
        jobExecutionService.updateJobExecution(entity)
          .map(updatedEntity -> (Response) PutChangeManagerJobExecutionByIdResponse.respond200WithApplicationJson(updatedEntity))
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.error("Failed to update JobExecution", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getChangeManagerJobExecutionById(String id, Map<String, String> okapiHeaders,
                                               Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        jobExecutionService.getJobExecutionById(id)
          .map(optionalJobExecution -> optionalJobExecution.orElseThrow(() ->
            new NotFoundException(String.format("JobExecution with id '%s' was not found", id))))
          .map(GetChangeManagerJobExecutionByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getChangeManagerJobExecutionChildrenById(String id, Map<String, String> okapiHeaders,
                                                       Handler<AsyncResult<Response>> asyncResultHandler,
                                                       Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        jobExecutionService.getJobExecutionCollectionByParentId(id)
          .map(GetChangeManagerJobExecutionChildrenByIdResponse::respond200WithApplicationJson)
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
  public void putChangeManagerJobExecutionStatusById(String id, StatusDto entity, Map<String, String> okapiHeaders,
                                                      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
        jobExecutionService.updateJobExecutionStatus(id, entity, params)
          .map(PutChangeManagerJobExecutionStatusByIdResponse::respond200WithApplicationJson)
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
  public void postChangeManagerRecordsByJobExecutionId(String jobExecutionId, RawRecordsDto entity,
                                                       Map<String, String> okapiHeaders,
                                                       Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    // TODO replace stub response
    asyncResultHandler.handle(Future.succeededFuture(
      PostChangeManagerRecordsByJobExecutionIdResponse.respond500WithTextPlain("Method is not implemented")));
  }

  @Override
  public void getChangeManagerFileExtension(String query, int offset, int limit, Map<String, String> okapiHeaders,
                                            Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        fileExtensionService.getFileExtensions(query, offset, limit)
          .map(GetChangeManagerFileExtensionResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.error("Failed to get all file extensions", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postChangeManagerFileExtension(FileExtension entity, Map<String, String> okapiHeaders,
                                             Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        fileExtensionService.addFileExtension(entity)
          .map((Response) PostChangeManagerFileExtensionResponse
            .respond201WithApplicationJson(entity))
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.error("Failed to create file extension", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getChangeManagerFileExtensionById(String id, Map<String, String> okapiHeaders,
                                                Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        fileExtensionService.getFileExtensionById(id)
          .map(optionalFileExtension -> optionalFileExtension.orElseThrow(() ->
            new NotFoundException(String.format("FileExtension with id '%s' was not found", id))))
          .map(GetChangeManagerFileExtensionByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.error("Failed to get file extension by id", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putChangeManagerFileExtensionById(String id, FileExtension entity, Map<String, String> okapiHeaders,
                                                Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        entity.setId(id);
        fileExtensionService.updateFileExtension(entity)
          .map(updatedEntity -> (Response) PutChangeManagerFileExtensionByIdResponse.respond200WithApplicationJson(updatedEntity))
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.error("Failed to update file extension", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteChangeManagerFileExtensionById(String id, Map<String, String> okapiHeaders,
                                                   Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        fileExtensionService.deleteFileExtension(id)
          .map(deleted -> deleted ?
            DeleteChangeManagerFileExtensionByIdResponse.respond204WithTextPlain(
              String.format("FileExtension with id '%s' was successfully deleted", id)) :
            DeleteChangeManagerFileExtensionByIdResponse.respond404WithTextPlain(
              String.format("FileExtension with id '%s' was not found", id)))
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.error("Failed to delete file extension", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }
}
