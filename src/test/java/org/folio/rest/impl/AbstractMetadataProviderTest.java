package org.folio.rest.impl;

import io.restassured.RestAssured;
import io.restassured.http.Header;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import javax.ws.rs.core.MediaType;

import static org.folio.rest.RestVerticle.OKAPI_HEADER_TENANT;

/**
 * Common abstract class for data necessary for testing the MetadataProvider
 */
public abstract class AbstractMetadataProviderTest {

  protected static final String HTTP_PORT = "http.port";
  protected static final String TENANT_ID = "diku";
  protected static final String TOKEN = "token";
  protected static final Header TENANT_HEADER = new Header(RestVerticle.OKAPI_HEADER_TENANT, TENANT_ID);
  protected static final String ACCEPT_VALUES = "application/json, text/plain";
  protected static final String CONTENT_LENGTH = "Content-Length";
  protected static final int CONTENT_LENGTH_DEFAULT = 1000;
  protected static final String HOST = "http://localhost:";
  protected static final String METADATA_PROVIDER_PATH = "/metadata-provider";
  protected static Vertx vertx;
  protected static int port;
  protected static String useExternalDatabase;
  protected static String baseServicePath;

  @BeforeClass
  public static void setUpClass(final TestContext context) throws Exception {
    Async async = context.async();
    vertx = Vertx.vertx();
    port = NetworkUtils.nextFreePort();

    useExternalDatabase = System.getProperty(
      "org.folio.password.validator.test.database",
      "embedded");

    switch (useExternalDatabase) {
      case "environment":
        System.out.println("Using environment settings");
        break;
      case "external":
        String postgresConfigPath = System.getProperty(
          "org.folio.password.validator.test.config",
          "/postgres-conf-local.json");
        PostgresClient.setConfigFilePath(postgresConfigPath);
        break;
      case "embedded":
        PostgresClient.setIsEmbedded(true);
        PostgresClient.getInstance(vertx).startEmbeddedPostgres();
        break;
      default:
        String message = "No understood database choice made." +
          "Please set org.folio.password.validator.test.config" +
          "to 'external', 'environment' or 'embedded'";
        throw new Exception(message);
    }

    TenantClient tenantClient = new TenantClient("localhost", port, TENANT_ID, TOKEN);

    final DeploymentOptions options = new DeploymentOptions().setConfig(new JsonObject().put(HTTP_PORT, port));
    vertx.deployVerticle(RestVerticle.class.getName(), options, res -> {
      try {
        tenantClient.postTenant(null, res2 -> {
          async.complete();
        });
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    baseServicePath = HOST + port + METADATA_PROVIDER_PATH;
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
  abstract public void setUp(TestContext context) throws Exception;

  protected RequestSpecification getDefaultGiven() {
    return RestAssured.given()
      .header(OKAPI_HEADER_TENANT, TENANT_ID)
      .header(HttpHeaders.ACCEPT.toString(), ACCEPT_VALUES)
      .header(HttpHeaders.CONTENT_TYPE.toString(), MediaType.APPLICATION_JSON)
      .header(CONTENT_LENGTH, CONTENT_LENGTH_DEFAULT);
  }
}
