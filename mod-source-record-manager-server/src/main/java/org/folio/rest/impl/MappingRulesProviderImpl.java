package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.resource.MappingRules;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.MappingRuleService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import java.util.Map;

import static io.vertx.core.Future.succeededFuture;
import static java.lang.String.format;

public class MappingRulesProviderImpl implements MappingRules {
  private String tenantId;
  @Autowired
  private MappingRuleService mappingRuleService;

  public MappingRulesProviderImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void getMappingRules(Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    succeededFuture()
      .compose(ar -> mappingRuleService.get(tenantId))
      .map(optionalRules -> optionalRules.orElseThrow(() ->
        new NotFoundException(format("Can not find mapping rules for tenant '%s'", tenantId))))
      .map(rules -> GetMappingRulesResponse.respond200WithApplicationJson(rules.encode()))
      .map(Response.class::cast)
      .otherwise(ExceptionHelper::mapExceptionToResponse)
      .setHandler(asyncResultHandler);
  }

  @Override
  public void putMappingRules(String entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    succeededFuture()
      .compose(ar -> mappingRuleService.update(entity, tenantId))
      .map(rules -> PutMappingRulesResponse.respond200WithApplicationJson(rules.encode()))
      .map(Response.class::cast)
      .otherwise(ExceptionHelper::mapExceptionToResponse)
      .setHandler(asyncResultHandler);
  }

  @Override
  public void putMappingRulesRestore(Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    succeededFuture()
      .compose(ar -> mappingRuleService.restore(tenantId))
      .map(rules -> PutMappingRulesRestoreResponse.respond200WithApplicationJson(rules.encode()))
      .map(Response.class::cast)
      .otherwise(ExceptionHelper::mapExceptionToResponse)
      .setHandler(asyncResultHandler);
  }
}
