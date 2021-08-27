package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.resource.MappingMetadata;
import org.folio.services.MappingMetadataService;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.util.Map;

public class MappingMetadataProviderImpl implements MappingMetadata {

  private static final Logger LOGGER = LogManager.getLogger();

  private String tenantId;

  @Autowired
  private MappingMetadataService mappingMetadataService;

  @Override
  public void getMappingMetadataByJobExecutionId(String jobExecutionId, String recordType, Map<String, String> okapiHeaders,
                                                 Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
        mappingMetadataService.getMappingMetadataDto(jobExecutionId, recordType, params)
          .map(GetMappingMetadataByJobExecutionIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.error("Failed to retrieve MappingMetadataDto entity for JobExecution with id {}", jobExecutionId, e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }
}
