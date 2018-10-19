package org.folio.services.provider;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * Metadata Service interface, contains logic of accessing jobs and logs metadata
 */
@ProxyGen
public interface MetadataService {

  static MetadataService create(Vertx vertx) {
    return new MetadataServiceImpl(vertx);
  }

  static MetadataService createProxy(Vertx vertx, String address) {
    return new MetadataServiceVertxEBProxy(vertx, address);
  }

  /**
   * Returns logs of executed jobs
   *
   * @param tenantId            tenant id
   * @param query               query string to filter logs based on matching criteria in fields
   * @param offset              starting index in a list of results
   * @param limit               maximum number of results to return
   * @param landingPage         flag that indicates whether requested collection of entities have to be prepared for rendering on the landing page
   * @param asyncResultHandler  result handler parametrized with JsonObject containing {@link org.folio.rest.jaxrs.model.LogCollection}
   */
  void getLogs(String tenantId, String query, int offset, int limit, boolean landingPage, Handler<AsyncResult<JsonObject>> asyncResultHandler);

  /**
   * Returns jobExecutions
   *
   * @param tenantId            tenant id
   * @param query               query string to filter jobExecutions based on matching criteria in fields
   * @param offset              starting index in a list of results
   * @param limit               maximum number of results to return
   * @param asyncResultHandler  result handler parametrized with JsonObject containing {@link org.folio.rest.jaxrs.model.JobExecutionCollection}
   */
  void getJobExecutions(String tenantId, String query, int offset, int limit, Handler<AsyncResult<JsonObject>> asyncResultHandler);
}
