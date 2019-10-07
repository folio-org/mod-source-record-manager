package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.UpdateResult;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.MappingRuleService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class ModTenantAPI extends TenantAPI {
  private static final Logger LOGGER = LoggerFactory.getLogger(ModTenantAPI.class);

  private static final String TEST_MODE = "test.mode";

  public static final String GRANT_SEQUENCES_PERMISSION_PATTERN = "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA %s TO %s;";

  private static final String SETUP_TEST_DATA_SQL = "templates/db_scripts/setup_test_data.sql";

  public static final String TENANT_PLACEHOLDER = "${myuniversity}";

  public static final String MODULE_PLACEHOLDER = "${mymodule}";


  @Autowired
  private MappingRuleService mappingRuleService;

  public ModTenantAPI() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Validate
  @Override
  public void postTenant(TenantAttributes entity, Map<String, String> headers, Handler<AsyncResult<Response>> handler, Context context) {
    super.postTenant(entity, headers, postTenantAr -> {
      if (postTenantAr.failed()) {
        handler.handle(postTenantAr);
      } else {
        OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(headers, context.owner());
        String tenantId = TenantTool.calculateTenantId(okapiConnectionParams.getTenantId());
        setSequencesPermissionForDbUser(context, tenantId)
          .compose(permissionAr -> setupTestData(context, tenantId))
          .compose(ar -> mappingRuleService.saveDefaultRules(tenantId))
          .setHandler(event -> handler.handle(postTenantAr));
      }
    }, context);
  }

  private Future<UpdateResult> setSequencesPermissionForDbUser(Context context, String tenantId) {
    Future<UpdateResult> future = Future.future();
    String dbSchemaName = new StringBuilder()
      .append(tenantId)
      .append('_')
      .append(PostgresClient.getModuleName()).toString();
    String preparedSql = String.format(GRANT_SEQUENCES_PERMISSION_PATTERN, dbSchemaName, dbSchemaName);
    PostgresClient.getInstance(context.owner()).execute(preparedSql, future);
    return future;
  }

  private Future<List<String>> setupTestData(Context context, String tenantId) {
    try {
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

}
