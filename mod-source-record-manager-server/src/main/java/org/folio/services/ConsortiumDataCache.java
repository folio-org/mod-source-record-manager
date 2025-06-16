package org.folio.services;

import static org.folio.services.util.ConsortiumUtil.DEFAULT_EXPIRATION_TIME_SECONDS;
import static org.folio.services.util.ConsortiumUtil.EXPIRATION_TIME_PARAM;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.services.entity.ConsortiumConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ConsortiumDataCache {

  private static final Logger LOG = LogManager.getLogger(ConsortiumDataCache.class);

  private static final String USER_TENANTS_PATH = "/user-tenants";
  private static final String LIMIT_PARAM = "limit=%d";
  private static final String USER_TENANTS_FIELD = "userTenants";
  private static final String CENTRAL_TENANT_ID_FIELD = "centralTenantId";
  private static final String CONSORTIUM_ID_FIELD = "consortiumId";

  private final Vertx vertx;
  private final AsyncCache<String, Optional<ConsortiumConfiguration>> cache;

  @Autowired
  public ConsortiumDataCache(Vertx vertx) {
    this.vertx = vertx;
    int expirationTime = Integer.parseInt(System.getProperty(EXPIRATION_TIME_PARAM, DEFAULT_EXPIRATION_TIME_SECONDS));
    this.cache = Caffeine.newBuilder()
      .expireAfterWrite(expirationTime, TimeUnit.SECONDS)
      .executor(task -> vertx.runOnContext(v -> task.run()))
      .buildAsync();
  }

  /**
   * Returns consortium data by specified connection params.
   *
   * @return future of Optional with consortium data for the specified connection params,
   *   if the specified in connection params tenant is not included to any consortium,
   *   then returns future with empty Optional
   */
  public Future<Optional<ConsortiumConfiguration>> getConsortiumData(OkapiConnectionParams params) {
    var tenantId = params.getTenantId();
    try {
      // Ensure we're running in Vert.x context
      if (Vertx.currentContext() == null) {
        return vertx.executeBlocking(promise ->
          cache.get(tenantId, (key, executor) -> loadData(params))
            .thenAccept(promise::complete)
            .exceptionally(e -> {
              promise.fail(e);
              return null;
            })
        );

      }
      return Future.fromCompletionStage(cache.get(tenantId, (key, executor) -> loadData(params)));
    } catch (Exception e) {
      LOG.warn("getConsortiumData:: Error loading consortium data, tenantId: '{}'", tenantId, e);
      return Future.failedFuture(e);
    }
  }

  private CompletableFuture<Optional<ConsortiumConfiguration>> loadData(OkapiConnectionParams params) {
    Promise<Optional<ConsortiumConfiguration>> promise = Promise.promise();
    RestUtil.doRequestWithSystemUser(params, USER_TENANTS_PATH + "?" + LIMIT_PARAM.formatted(1), HttpMethod.GET, null)
      .onComplete(response -> {
        try {
          if (RestUtil.validateAsyncResult(response, promise)) {
            JsonArray userTenants = response.result().getJson().getJsonArray(USER_TENANTS_FIELD);
            if (userTenants.isEmpty()) {
              promise.complete(Optional.empty());
              return;
            }

            LOG.info("loadConsortiumData:: Consortium data was loaded, tenantId: '{}'", params.getTenantId());
            JsonObject userTenant = userTenants.getJsonObject(0);
            promise.complete(Optional.of(buildConsortiumConfig(userTenant)));
          } else {
            String msg = String.format("Error loading consortium data, tenantId: '%s'", params.getTenantId());
            LOG.warn("loadConsortiumData:: {}", msg);
            promise.fail((msg));
          }
        } catch (Exception e) {
          LOG.warn("Error loading consortium data, tenantId: {}", params.getTenantId(), e);
          promise.fail(e);
        }
      });
    return promise.future().toCompletionStage().toCompletableFuture();
  }

  private ConsortiumConfiguration buildConsortiumConfig(JsonObject userTenant) {
    return new ConsortiumConfiguration(
      userTenant.getString(CENTRAL_TENANT_ID_FIELD),
      userTenant.getString(CONSORTIUM_ID_FIELD)
    );
  }
}
