package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.FileExtensionCollection;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.FileExtensionService;
import org.folio.services.FileExtensionServiceImpl;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class ModTenantAPI extends TenantAPI {
  private static final Logger LOGGER = LoggerFactory.getLogger(ModTenantAPI.class);

  private static final String TEST_MODE = "test.mode";

  private static final String SETUP_TEST_DATA_SQL = "templates/db_scripts/setup_test_data.sql";

  public static final String TENANT_PLACEHOLDER = "${myuniversity}";

  public static final String MODULE_PLACEHOLDER = "${mymodule}";

  @Validate
  @Override
  public void postTenant(TenantAttributes entity, Map<String, String> headers, Handler<AsyncResult<Response>> handlers, Context context) {
    super.postTenant(entity, headers, ar -> {
      if (ar.failed()) {
        handlers.handle(ar);
      } else {
        setupTestData(headers, context)
          .compose(v -> setupDefaultFileExtensions(headers, context))
          .setHandler(event -> handlers.handle(ar));
      }
    }, context);
  }

  private Future<List<String>> setupTestData(Map<String, String> headers, Context context) {
    try {
//      LOGGER.info("***System.getenv():");
//      System.getenv().entrySet().forEach(e -> LOGGER.info("       -> " + e.getKey() + ": " + e.getValue()));
//
//      LOGGER.info("***System.getProperties():");
//      System.getProperties().entrySet().forEach(e -> LOGGER.info("       -> " + e.getKey() + ": " + e.getValue()));

      if (!Boolean.TRUE.equals(Boolean.valueOf(System.getenv(TEST_MODE)))) {
        LOGGER.info("Test data was not initialized.");
        return Future.succeededFuture();
      }

      InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(SETUP_TEST_DATA_SQL);

      if (inputStream == null) {
        LOGGER.info("Test data was not initialized: no resources found: {}", SETUP_TEST_DATA_SQL);
        return Future.succeededFuture();
      }

      String sqlScript = IOUtils.toString(inputStream, StandardCharsets.UTF_8.name());
      if (StringUtils.isBlank(sqlScript)) {
        return Future.succeededFuture();
      }

      String tenantId = TenantTool.calculateTenantId((String) headers.get("x-okapi-tenant"));
      String moduleName = PostgresClient.getModuleName();

      sqlScript = sqlScript.replace(TENANT_PLACEHOLDER, tenantId).replace(MODULE_PLACEHOLDER, moduleName);

      Future<List<String>> future = Future.future();
      PostgresClient.getInstance(context.owner()).runSQLFile(sqlScript, false, future);

      LOGGER.info("Test data will be initialized. Check the server log for details.");

      return future;
    } catch (IOException e) {
      return Future.failedFuture(e);
    }
  }

  private Future<Boolean> setupDefaultFileExtensions(Map<String, String> headers, Context context) {
    try {
      FileExtensionService service = new FileExtensionServiceImpl(context.owner(), TenantTool.calculateTenantId(headers.get("x-okapi-tenant")));
      return
        service.getFileExtensions(null, 0, 1)
          .compose(r -> createDefExtensionsIfNeed(r, service));
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  private Future<Boolean> createDefExtensionsIfNeed(FileExtensionCollection collection, FileExtensionService service) {
    Future<Boolean> future = Future.future();
    if (collection.getTotalRecords() == 0) {
      return service.copyExtensionsFromDefault()
        .map(r -> r.getUpdated() > 0);
    } else {
      future.complete(true);
    }
    return future;
  }

}
