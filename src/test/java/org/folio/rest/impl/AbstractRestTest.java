package org.folio.rest.impl;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.services.JobExecutionServiceImpl;
import org.folio.util.OkapiConnectionParams;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.folio.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.util.RestUtil.OKAPI_URL_HEADER;

/**
 * Abstract test for the REST API testing needs.
 */
public abstract class AbstractRestTest {

  private static final String JOB_EXECUTIONS_TABLE_NAME = "job_executions";
  private static final String TOKEN = "token";
  private static final String HTTP_PORT = "http.port";
  private static int port;
  private static String useExternalDatabase;
  private static String postedSnapshotResponseBody = UUID.randomUUID().toString();
  protected static Vertx vertx;
  protected static final String TENANT_ID = "diku";
  protected static RequestSpecification spec;
  protected OkapiConnectionParams params;

  @Rule
  public WireMockRule snapshotMockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new ConsoleNotifier(true)));

  @BeforeClass
  public static void setUpClass(final TestContext context) throws Exception {
    Async async = context.async();
    vertx = Vertx.vertx();
    port = NetworkUtils.nextFreePort();
    PostgresClient.stopEmbeddedPostgres();
    PostgresClient.closeAllClients();
    useExternalDatabase = System.getProperty(
      "org.folio.metadata.provider.test.database",
      "embedded");

    switch (useExternalDatabase) {
      case "environment":
        System.out.println("Using environment settings");
        break;
      case "external":
        String postgresConfigPath = System.getProperty(
          "org.folio.metadata.provider.test.config",
          "/postgres-conf-local.json");
        PostgresClient.setConfigFilePath(postgresConfigPath);
        break;
      case "embedded":
        PostgresClient.setIsEmbedded(true);
        PostgresClient.getInstance(vertx).startEmbeddedPostgres();
        break;
      default:
        String message = "No understood database choice made." +
          "Please set org.folio.metadata.provider.test.database" +
          "to 'external', 'environment' or 'embedded'";
        throw new Exception(message);
    }

    TenantClient tenantClient = new TenantClient("localhost", port, TENANT_ID, TOKEN);

    final DeploymentOptions options = new DeploymentOptions().setConfig(new JsonObject().put(HTTP_PORT, port));
    vertx.deployVerticle(RestVerticle.class.getName(), options, res -> {
      try {
        TenantAttributes tenantAttributes = new TenantAttributes().withModuleFrom("0.0.1").withModuleTo("0.0.2");
        tenantClient.postTenant(tenantAttributes, res2 -> {
          async.complete();
        });
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }

  @AfterClass
  public static void tearDownClass(final TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      if (useExternalDatabase.equals("embedded")) {
        PostgresClient.stopEmbeddedPostgres();
      }
      async.complete();
    }));
  }

  @Before
  public void setUp(TestContext context) {
    clearTable(context);
    spec = new RequestSpecBuilder()
      .setContentType(ContentType.JSON)
      .addHeader(OKAPI_URL_HEADER, "http://localhost:" + snapshotMockServer.port())
      .addHeader(OKAPI_TENANT_HEADER, TENANT_ID)
      .addHeader(RestVerticle.OKAPI_USERID_HEADER, UUID.randomUUID().toString())
      .addHeader("Accept", "text/plain, application/json")
      .setBaseUri("http://localhost:" + port)
      .build();
    Map<String, String> okapiHeaders = new HashMap<>();
    okapiHeaders.put(OKAPI_URL_HEADER, "http://localhost:" + snapshotMockServer.port());
    okapiHeaders.put(OKAPI_TENANT_HEADER, TENANT_ID);
    okapiHeaders.put(RestVerticle.OKAPI_HEADER_TOKEN, TOKEN);
    okapiHeaders.put(RestVerticle.OKAPI_USERID_HEADER, UUID.randomUUID().toString());
    params = new OkapiConnectionParams(okapiHeaders, vertx.getOrCreateContext().owner());
    WireMock.stubFor(WireMock.post(JobExecutionServiceImpl.SNAPSHOT_SERVICE_URL)
      .willReturn(WireMock.created().withBody(postedSnapshotResponseBody)));
  }

  private void clearTable(TestContext context) {
    PostgresClient.getInstance(vertx, TENANT_ID).delete(JOB_EXECUTIONS_TABLE_NAME, new Criterion(), event -> {
      if (event.failed()) {
        context.fail(event.cause());
      }
    });
  }

}
