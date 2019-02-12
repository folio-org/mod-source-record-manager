package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.Error;
import org.folio.rest.jaxrs.model.Errors;
import org.folio.rest.jaxrs.model.FileExtension;
import org.folio.rest.jaxrs.resource.MetadataProvider;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.*;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.Map;

public class MetadataProviderImpl implements MetadataProvider {

  private static final int LANDING_PAGE_LOGS_LIMIT = 25;
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataProviderImpl.class);
  private static final String FILE_EXTENSION_DUPLICATE_ERROR_CODE = "fileExtension.duplication.invalid";
  private static final String FILE_EXTENSION_VALIDATE_ERROR_MESSAGE = "Failed to validate file extension";

  private JobExecutionService jobExecutionService;
  private FileExtensionService fileExtensionService;
  private Vertx vertx;

  public MetadataProviderImpl(Vertx vertx, String tenantId) {
    this.vertx = vertx;
    String calculatedTenantId = TenantTool.calculateTenantId(tenantId);
    this.jobExecutionService = new JobExecutionServiceImpl(vertx, calculatedTenantId);
    this.fileExtensionService = new FileExtensionServiceImpl(vertx, calculatedTenantId);
  }

  @Override
  public void getMetadataProviderLogs(boolean landingPage, String query, int offset, int limit,
                                      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                                      Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        jobExecutionService.getLogCollectionDtoByQuery(query, offset, landingPage ? LANDING_PAGE_LOGS_LIMIT : limit)
          .map(GetMetadataProviderLogsResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(
          ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getMetadataProviderJobExecutions(String query, int offset, int limit, Map<String, String> okapiHeaders,
                                               Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        jobExecutionService.getJobExecutionCollectionDtoByQuery(query, offset, limit)
          .map(GetMetadataProviderJobExecutionsResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(
          ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getMetadataProviderFileExtension(String query, int offset, int limit, Map<String, String> okapiHeaders,
                                               Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        fileExtensionService.getFileExtensions(query, offset, limit)
          .map(GetMetadataProviderFileExtensionResponse::respond200WithApplicationJson)
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
  public void postMetadataProviderFileExtension(FileExtension entity, Map<String, String> okapiHeaders,
                                                Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        validateFileExtension(entity).setHandler(errors -> {
          if (errors.failed()) {
            LOGGER.error(FILE_EXTENSION_VALIDATE_ERROR_MESSAGE, errors.cause());
            asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(errors.cause())));
          } else if (errors.result().getTotalRecords() > 0) {
            asyncResultHandler.handle(Future.succeededFuture(PutMetadataProviderFileExtensionByIdResponse.respond422WithApplicationJson(errors.result())));
          } else {
            fileExtensionService.addFileExtension(entity, new OkapiConnectionParams(okapiHeaders, vertx))
              .map((Response) PostMetadataProviderFileExtensionResponse
                .respond201WithApplicationJson(entity))
              .otherwise(ExceptionHelper::mapExceptionToResponse)
              .setHandler(asyncResultHandler);
          }
        });
      } catch (Exception e) {
        LOGGER.error("Failed to create file extension", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getMetadataProviderFileExtensionById(String id, Map<String, String> okapiHeaders,
                                                   Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        fileExtensionService.getFileExtensionById(id)
          .map(optionalFileExtension -> optionalFileExtension.orElseThrow(() ->
            new NotFoundException(String.format("FileExtension with id '%s' was not found", id))))
          .map(GetMetadataProviderFileExtensionByIdResponse::respond200WithApplicationJson)
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
  public void putMetadataProviderFileExtensionById(String id, FileExtension entity, Map<String, String> okapiHeaders,
                                                   Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        validateFileExtension(entity).setHandler(errors -> {
          entity.setId(id);
          if (errors.failed()) {
            LOGGER.error(FILE_EXTENSION_VALIDATE_ERROR_MESSAGE, errors.cause());
            asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(errors.cause())));
          } else if (errors.result().getTotalRecords() > 0) {
            asyncResultHandler.handle(Future.succeededFuture(PutMetadataProviderFileExtensionByIdResponse.respond422WithApplicationJson(errors.result())));
          } else {
            fileExtensionService.updateFileExtension(entity, new OkapiConnectionParams(okapiHeaders, vertx))
              .map(updatedEntity -> (Response) PutMetadataProviderFileExtensionByIdResponse.respond200WithApplicationJson(updatedEntity))
              .otherwise(ExceptionHelper::mapExceptionToResponse)
              .setHandler(asyncResultHandler);
          }
        });
      } catch (Exception e) {
        LOGGER.error("Failed to update file extension", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteMetadataProviderFileExtensionById(String id, Map<String, String> okapiHeaders,
                                                      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        fileExtensionService.deleteFileExtension(id)
          .map(deleted -> deleted ?
            DeleteMetadataProviderFileExtensionByIdResponse.respond204WithTextPlain(
              String.format("FileExtension with id '%s' was successfully deleted", id)) :
            DeleteMetadataProviderFileExtensionByIdResponse.respond404WithTextPlain(
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

  @Override
  public void getMetadataProviderFileExtensionRestoreDefault(Map<String, String> okapiHeaders,
                                                             Handler<AsyncResult<Response>> asyncResultHandler,
                                                             Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        fileExtensionService.restoreFileExtensions()
          .map(defaultCollection -> (Response) GetMetadataProviderFileExtensionRestoreDefaultResponse
            .respond200WithApplicationJson(defaultCollection))
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.error("Failed to restore file extensions", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getMetadataProviderDataTypes(Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
          fileExtensionService.getDataTypes()
            .map(GetMetadataProviderDataTypesResponse::respond200WithApplicationJson)
            .map(Response.class::cast)
            .otherwise(ExceptionHelper::mapExceptionToResponse)
            .setHandler(asyncResultHandler);
      }catch (Exception e) {
        LOGGER.error("Failed to get all data types", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  /**
   * Validate {@link FileExtension} before save or update
   *
   * @param extension - {@link FileExtension} object to create or update
   * @return - Future {@link Errors} object with list of validation errors
   */
  private Future<Errors> validateFileExtension(FileExtension extension) {
    Errors errors = new Errors();
    return fileExtensionService.isFileExtensionExistByName(extension).map(exist -> exist
      ? errors.withErrors(Collections.singletonList(new Error().withMessage(FILE_EXTENSION_DUPLICATE_ERROR_CODE))).withTotalRecords(errors.getErrors().size())
      : errors.withTotalRecords(0));
  }

}
