package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.folio.Record;
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
  public void getMappingRulesByRecordType(String recordType, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    Record.RecordType ruleType = setRuleType(recordType);
    if (ruleType == null) {
      GetMappingRulesByRecordTypeResponse.respond404WithTextPlain("Specified invalid record type");
      return;
    }
    succeededFuture()
      .compose(ar -> mappingRuleService.get(tenantId, ruleType))
      .map(optionalRules -> optionalRules.orElseThrow(() ->
        new NotFoundException(format("Can not find mapping rules with type '%s' for tenant '%s'", ruleType, tenantId))))
      .map(rules -> GetMappingRulesByRecordTypeResponse.respond200WithApplicationJson(rules.encode()))
      .map(Response.class::cast)
      .otherwise(ExceptionHelper::mapExceptionToResponse)
      .onComplete(asyncResultHandler);
  }

  private Record.RecordType setRuleType(String recordType){
    if (recordType.equals("marc-bib")) return Record.RecordType.MARC_BIB;
    if (recordType.equals("marc-holdings")) return Record.RecordType.MARC_HOLDING;
    return null;
  }

  @Override
  public void putMappingRules(String entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    succeededFuture()
      .compose(ar -> mappingRuleService.update(entity, tenantId))
      .map(rules -> PutMappingRulesResponse.respond200WithApplicationJson(rules.encode()))
      .map(Response.class::cast)
      .otherwise(ExceptionHelper::mapExceptionToResponse)
      .onComplete(asyncResultHandler);
  }

  @Override
  public void putMappingRulesRestore(Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    succeededFuture()
      .compose(ar -> mappingRuleService.restore(tenantId))
      .map(rules -> PutMappingRulesRestoreResponse.respond200WithApplicationJson(rules.encode()))
      .map(Response.class::cast)
      .otherwise(ExceptionHelper::mapExceptionToResponse)
      .onComplete(asyncResultHandler);
  }
}
