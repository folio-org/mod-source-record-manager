package org.folio.rest.impl;

import static io.vertx.core.Future.succeededFuture;

import java.util.Map;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Autowired;

import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.resource.MappingRules;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.MappingRuleService;
import org.folio.services.util.QueryPathUtil;
import org.folio.spring.SpringContextUtil;

public class MappingRulesProviderImpl implements MappingRules {
  private String tenantId;
  @Autowired
  private MappingRuleService mappingRuleService;

  public MappingRulesProviderImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }


  @Override
  public void getMappingRulesByRecordType(String recordType, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    succeededFuture()
      .compose(ar -> mappingRuleService.get(QueryPathUtil.toRecordType(recordType).orElseThrow(() ->
        new BadRequestException("Only marc-bib or marc-holdings supported")), tenantId))
      .map(optionalRules -> optionalRules.orElseThrow(() ->
        new NotFoundException(String.format("Can not find mapping rules with type '%s' for tenant '%s'", recordType, tenantId))))
      .map(rules -> GetMappingRulesByRecordTypeResponse.respond200WithApplicationJson(rules.encode()))
      .map(Response.class::cast)
      .otherwise(ExceptionHelper::mapExceptionToResponse)
      .onComplete(asyncResultHandler);
  }


  @Override
  public void putMappingRulesByRecordType(String recordType, String entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    succeededFuture()
      .compose(ar -> mappingRuleService.update(entity, QueryPathUtil.toRecordType(recordType).orElseThrow(() ->
        new BadRequestException("Only marc-bib or marc-holdings supported")), tenantId))
      .map(rules -> PutMappingRulesByRecordTypeResponse.respond200WithApplicationJson(rules.encode()))
      .map(Response.class::cast)
      .otherwise(ExceptionHelper::mapExceptionToResponse)
      .onComplete(asyncResultHandler);
  }


  @Override
  public void putMappingRulesRestoreByRecordType(String recordType, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    succeededFuture()
      .compose(ar -> mappingRuleService.restore(QueryPathUtil.toRecordType(recordType).orElseThrow(() ->
        new BadRequestException("Only marc-bib or marc-holdings supported")), tenantId))
      .map(rules -> PutMappingRulesRestoreByRecordTypeResponse.respond200WithApplicationJson(rules.encode()))
      .map(Response.class::cast)
      .otherwise(ExceptionHelper::mapExceptionToResponse)
      .onComplete(asyncResultHandler);
  }
}
