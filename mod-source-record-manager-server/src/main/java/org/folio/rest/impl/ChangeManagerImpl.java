package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.rest.jaxrs.model.DeleteJobExecutionsReq;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import java.util.Map;

import static java.lang.String.format;

@SuppressWarnings("java:S6813")
public class ChangeManagerImpl implements ChangeManager {

  private static final Logger log = LogManager.getLogger();
  private static final String CHUNK_ID_HEADER = "chunkId";

  @Autowired
  private JobExecutionService jobExecutionService;
  @Autowired
  @Qualifier("eventDrivenChunkProcessingService")
  private ChunkProcessingService eventDrivenChunkProcessingService;

  public ChangeManagerImpl() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  public void deleteChangeManagerJobExecutions(DeleteJobExecutionsReq entity, Map<String, String> headers, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        var tenantId = TenantTool.tenantId(headers);
        log.debug("deleteChangeManagerJobExecutions:: jobExecutionIds {}, tenantId {}", entity.getIds(), tenantId);
        jobExecutionService.softDeleteJobExecutionsByIds(entity.getIds(), tenantId)
          .map(DeleteChangeManagerJobExecutionsResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        log.warn("deleteChangeManagerJobExecutions:: Failed to delete JobExecutions by ids {}, ", entity.getIds(), e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postChangeManagerJobExecutions(InitJobExecutionsRqDto initJobExecutionsRqDto,
                                             Map<String, String> headers,
                                             Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        log.debug("postChangeManagerJobExecutions:: userId {}", initJobExecutionsRqDto.getUserId());
        ConnectionParams params = new ConnectionParams(headers);
        jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
          .map(initJobExecutionsRsDto ->
            (Response) PostChangeManagerJobExecutionsResponse.respond201WithApplicationJson(initJobExecutionsRsDto))
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        log.warn("postChangeManagerJobExecutions:: Error during initializing JobExecution entities", e);
        asyncResultHandler.handle(Future.failedFuture(e));
      }
    });
  }

  @Override
  public void putChangeManagerJobExecutionsById(String id, JobExecution entity, Map<String, String> headers,
                                                Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        entity.setId(id);
        log.debug("putChangeManagerJobExecutionsById:: jobExecutionId {}", entity.getId());
        ConnectionParams params = new ConnectionParams(headers);
        jobExecutionService.updateJobExecutionWithSnapshotStatus(entity, params)
          .map(updatedEntity -> (Response) PutChangeManagerJobExecutionsByIdResponse.respond200WithApplicationJson(updatedEntity))
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        log.warn("putChangeManagerJobExecutionsById:: Failed to update JobExecution", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getChangeManagerJobExecutionsById(String id, Map<String, String> headers,
                                                Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        var tenantId = TenantTool.tenantId(headers);
        log.debug("getChangeManagerJobExecutionsById:: jobExecutionId {}, tenantId {}", id, tenantId);
        jobExecutionService.getJobExecutionById(id, tenantId)
          .map(optionalJobExecution -> optionalJobExecution.orElseThrow(() ->
            new NotFoundException(format("JobExecution with id '%s' was not found", id))))
          .map(GetChangeManagerJobExecutionsByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        log.warn(getMessage("getChangeManagerJobExecutionsById:: Failed to get JobExecution by id", e, id));
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteChangeManagerJobExecutionsById(String id, Map<String, String> okapiHeaders,
                                                   Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    log.warn("deleteChangeManagerJobExecutionsById:: Method is not implemented");
    asyncResultHandler.handle(Future.succeededFuture(
      PostChangeManagerJobExecutionsRecordsByIdResponse.respond500WithTextPlain("Method is not implemented")));
  }


  @Override
  public void getChangeManagerJobExecutionsChildrenById(String id, int limit, String totalRecords, int offset,
                                                        Map<String, String> headers,
                                                        Handler<AsyncResult<Response>> asyncResultHandler,
                                                        Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        var tenantId = TenantTool.tenantId(headers);
        log.debug("getChangeManagerJobExecutionsChildrenById:: parentId {}, tenantId {}", id, tenantId);
        jobExecutionService.getJobExecutionCollectionByParentId(id, offset, limit, tenantId)
          .map(GetChangeManagerJobExecutionsChildrenByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        log.warn("getChangeManagerJobExecutionsChildrenById:: Failed to retrieve JobExecutions by parent id", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putChangeManagerJobExecutionsStatusById(String id, StatusDto entity, Map<String, String> headers,
                                                      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        log.debug("putChangeManagerJobExecutionsStatusById:: jobExecutionId {}", id);
        ConnectionParams params = new ConnectionParams(headers);
        jobExecutionService.updateJobExecutionStatus(id, entity, params)
          .map(PutChangeManagerJobExecutionsStatusByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        log.warn("putChangeManagerJobExecutionsStatusById:: Failed to update status for JobExecution", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putChangeManagerJobExecutionsJobProfileById(String id, JobProfileInfo entity, Map<String, String> headers, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        log.debug("putChangeManagerJobExecutionsJobProfileById:: jobExecutionId {}", id);
        ConnectionParams params = new ConnectionParams(headers);
        jobExecutionService.setJobProfileToJobExecution(id, entity, params)
          .map(PutChangeManagerJobExecutionsStatusByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        log.warn("putChangeManagerJobExecutionsJobProfileById:: Failed to set JobProfile for JobExecution", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postChangeManagerJobExecutionsRecordsById(String id, boolean acceptInstanceId, RawRecordsDto entity,
                                                        Map<String, String> headers,
                                                        Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        log.debug("putChangeManagerJobExecutionsJobProfileById:: jobExecutionId {}, rawRecordsId {}", id, entity.getId());
        headers.put(CHUNK_ID_HEADER, entity.getId());
        ConnectionParams params = new ConnectionParams(headers);
        eventDrivenChunkProcessingService.processChunk(entity, id, acceptInstanceId, params)
          .map(processed -> PostChangeManagerJobExecutionsRecordsByIdResponse.respond204())
          .map(Response.class::cast)
          .otherwise(ex -> {
            if (ex instanceof DuplicateEventException) {
              log.warn("postChangeManagerJobExecutionsRecordsById:: Failed to process chunk of RawRecords with JobExecutionId {} with RawRecordsId {}: {}", id, entity.getId(), ex.getMessage());
              return ExceptionHelper.mapExceptionToResponse(new BadRequestException(ex.getMessage()));
            } else {
              return ExceptionHelper.mapExceptionToResponse(ex);
            }
          })
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        log.warn(getMessage("postChangeManagerJobExecutionsRecordsById:: Failed to process chunk of RawRecords with JobExecution id {}", e, id));
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteChangeManagerJobExecutionsRecordsById(String id, Map<String, String> headers,
                                                          Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        log.debug("deleteChangeManagerJobExecutionsRecordsById:: jobExecutionId {}", id);
        ConnectionParams params = new ConnectionParams(headers);
        jobExecutionService.completeJobExecutionWithCancelledStatus(id, params)
          .map(deleted -> DeleteChangeManagerJobExecutionsRecordsByIdResponse.respond204())
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        log.warn(getMessage("deleteChangeManagerJobExecutionsRecordsById:: Failed to delete records for JobExecution id {}", e, id));
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  private ParameterizedMessage getMessage(String pattern, Exception e, String... params) {
    return new ParameterizedMessage(pattern, new Object[] {params}, e);
  }
}
