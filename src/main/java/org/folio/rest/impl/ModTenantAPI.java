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
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.TenantTool;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class ModTenantAPI extends TenantAPI {
  private static final Logger LOGGER = LoggerFactory.getLogger(ModTenantAPI.class);

  private static final String TEST_MODE = "test.mode";

  private static final String SETUP_TEST_DATA_SQL = "templates/db_scripts/setup_test_data.sql";

  private static final String TENANT_PLACEHOLDER = "${myuniversity}";

  private static final String MODULE_PLACEHOLDER = "${mymodule}";

  @Validate
  @Override
  public void postTenant(TenantAttributes entity, Map<String, String> headers, Handler<AsyncResult<Response>> handlers, Context context) {
    super.postTenant(entity, headers, ar -> {
      if (ar.failed()) {
        handlers.handle(ar);
      } else {
        setupTestData(headers, context).setHandler(event -> handlers.handle(ar));
      }
    }, context);
  }

  private Future<List<String>> setupTestData(Map<String, String> headers, Context context) {
    try {

      if (!Boolean.TRUE.equals(Boolean.valueOf(System.getProperty(TEST_MODE, "false")))) {
        LOGGER.info("Test data was not initialized.");
        return Future.succeededFuture();
      }

      String sqlScript = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream(SETUP_TEST_DATA_SQL), StandardCharsets.UTF_8.name());
      if (StringUtils.isBlank(sqlScript)) {
        return Future.succeededFuture();
      }

      String tenantId = TenantTool.calculateTenantId((String) headers.get("x-okapi-tenant"));
      String moduleName = PostgresClient.getModuleName();

      sqlScript = sqlScript.replace(TENANT_PLACEHOLDER, tenantId).replace(MODULE_PLACEHOLDER, moduleName);

      Future<List<String>> future = Future.future();
      PostgresClient.getInstance(context.owner()).runSQLFile(sqlScript, false, future::handle);

      LOGGER.info("Test data was initialized.");

      return future;
    } catch (IOException e) {
      return Future.failedFuture(e);
    }
  }

}
