package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import java.util.Optional;

public interface MappingRuleService {

  Future<Optional<JsonObject>> get(String tenantId);

  Future<Void> saveDefaultRules(String tenantId);

  Future<JsonObject> update(String rules, String tenantId);

  Future<JsonObject> restore(String tenantId);
}
