package org.folio.rest.impl;

import io.restassured.RestAssured;
import io.restassured.http.Header;
import io.restassured.response.ResponseBodyExtractionOptions;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.UUID;

import static org.folio.rest.RestVerticle.OKAPI_HEADER_TENANT;

@RunWith(VertxUnitRunner.class)
public class ChangeManagerAPITest {

  private static final String HTTP_PORT = "http.port";
  private static final String TENANT_ID = "diku";
  private static final Header TENANT_HEADER = new Header(RestVerticle.OKAPI_HEADER_TENANT, TENANT_ID);
  private static final String TOKEN = "token";
  private static final String HOST = "http://localhost:";
  private static final String CHANGE_MANAGER_PATH = "/change-manager";
  private static final String ACCEPT_VALUES = "application/json, text/plain";

  private static Vertx vertx;
  private static int port;
  private static String useExternalDatabase;

  // TODO reuse verticle deployment
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
  }

  @Test
  public void testInitJobExecutions(TestContext context) {
    //TODO Replace testing stub

    String servicePath = "/jobExecutions";
    String testUrl = CHANGE_MANAGER_PATH + servicePath;
    String givenUploadDefinitionUuid = UUID.randomUUID().toString();
    int filesToUploadNumber = 10;
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.setUploadDefinitionId(givenUploadDefinitionUuid);
    requestDto.setFilesNumber(filesToUploadNumber);

    ResponseBodyExtractionOptions body = getDefaultGiven()
      .port(port)
      .header(TENANT_HEADER)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().post(testUrl)
      .then().statusCode(200)
      .extract().body();

    String actualUploadDefinitionId = body.jsonPath().getObject("uploadDefinitionId", String.class);
    List<JobExecution> actualJobExecutions = body.jsonPath().getObject("jobExecutions", List.class);

    Assert.assertEquals(givenUploadDefinitionUuid, actualUploadDefinitionId);
    Assert.assertEquals(filesToUploadNumber, actualJobExecutions.size());
  }


  protected RequestSpecification getDefaultGiven() {
    return RestAssured.given()
      .header(OKAPI_HEADER_TENANT, TENANT_ID)
      .header(HttpHeaders.ACCEPT.toString(), ACCEPT_VALUES)
      .header(HttpHeaders.CONTENT_TYPE.toString(), MediaType.APPLICATION_JSON);
  }
}
