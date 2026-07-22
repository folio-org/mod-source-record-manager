package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import javax.ws.rs.BadRequestException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.rest.jaxrs.resource.MappingMetadata;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.MappingMetadataService;
import org.folio.services.util.QueryPathUtil;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.util.Map;

@SuppressWarnings("java:S6813")
public class MappingMetadataProviderImpl implements MappingMetadata {

  private static final Logger log = LogManager.getLogger();

  @Autowired
  private MappingMetadataService mappingMetadataService;

  public MappingMetadataProviderImpl() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  public void getMappingMetadataByJobExecutionId(String jobExecutionId, Map<String, String> headers,
                                                 Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      var tenantId = TenantTool.tenantId(headers);
      try {
        log.debug("getMappingMetadataByJobExecutionId:: jobExecutionId {}", jobExecutionId);
        ConnectionParams params = new ConnectionParams(headers);
        mappingMetadataService.getMappingMetadataDto(jobExecutionId, params)
          .map(GetMappingMetadataByJobExecutionIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        log.warn("getMappingMetadataByJobExecutionId:: Failed to retrieve MappingMetadataDto " +
          "entity for JobExecution with id {} for tenant {}", jobExecutionId, tenantId, e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getMappingMetadataTypeByRecordType(String recordType, Map<String, String> headers,
                                                 Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      var tenantId = TenantTool.tenantId(headers);
      try {
        log.debug("getMappingMetadataTypeByRecordType:: recordType {}", recordType);
        ConnectionParams params = new ConnectionParams(headers);
        mappingMetadataService.getMappingMetadataDtoByRecordType(
            QueryPathUtil.toRecordType(recordType).orElseThrow(() ->
              new BadRequestException("Only marc-bib, marc-holdings or marc-authority supported")), params)
          .map(GetMappingMetadataByJobExecutionIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        log.warn("getMappingMetadataTypeByRecordType:: Failed to retrieve MappingMetadataDto entity for recordType {} and tenant {}",
          recordType, tenantId, e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }
}
