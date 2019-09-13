package org.folio.rest.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseTransformer;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.Response;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.folio.TestUtil;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.InstancesBatchResponse;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;

/**
 * Abstract test for the REST API testing needs.
 */
public abstract class AbstractRestTest {

  private static final String JOB_EXECUTIONS_TABLE_NAME = "job_executions";
  private static final String CHUNKS_TABLE_NAME = "job_execution_source_chunks";
  private static final String TOKEN = "token";
  private static final String HTTP_PORT = "http.port";
  private static int port;
  private static String useExternalDatabase;
  private static String postedSnapshotResponseBody = UUID.randomUUID().toString();
  private static Vertx vertx;
  private static final String TENANT_ID = "diku";
  protected static RequestSpecification spec;

  protected static final String JOB_EXECUTION_PATH = "/change-manager/jobExecutions/";
  private static final String GET_USER_URL = "/users?query=id==";
  private static final String IDENTIFIER_TYPES_URL = "/identifier-types";
  private static final String INSTANCE_TYPES_URL = "/instance-types";
  private static final String CLASSIFICATION_TYPES_URL = "/classification-types";
  private static final String INSTANE_FORMATS_URL = "/instance-formats";
  protected static final String FILES_PATH = "src/test/resources/org/folio/rest/files.sample";
  protected static final String RECORD_PATH = "src/test/resources/org/folio/rest/record.json";
  protected static final String SNAPSHOT_SERVICE_URL = "/source-storage/snapshots";
  protected static final String RECORDS_SERVICE_URL = "/source-storage/batch/records";
  protected static final String RECORD_SERVICE_URL = "/source-storage/records";
  protected static final String INVENTORY_URL = "/inventory/instances/batch";
  protected static final String PARSED_RECORDS_COLLECTION_URL = "/source-storage/batch/parsed-records";
  protected static final String okapiUserIdHeader = UUID.randomUUID().toString();

  private JsonObject userResponse = new JsonObject()
    .put("users",
      new JsonArray().add(new JsonObject()
        .put("username", "diku_admin")
        .put("personal", new JsonObject().put("firstName", "DIKU").put("lastName", "ADMINISTRATOR"))))
    .put("totalRecords", 1);

  @Rule
  public WireMockRule snapshotMockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true))
      .extensions(new RequestToResponseTransformer(), new InstancesBatchResponseTransformer())
  );


  @BeforeClass
  public static void setUpClass(final TestContext context) throws Exception {
    Async async = context.async();
    vertx = Vertx.vertx();
    port = NetworkUtils.nextFreePort();
    String okapiUrl = "http://localhost:" + port;
    PostgresClient.stopEmbeddedPostgres();
    PostgresClient.closeAllClients();
    useExternalDatabase = System.getProperty(
      "org.folio.source.record.manager.test.database",
      "embedded");

    switch (useExternalDatabase) {
      case "environment":
        System.out.println("Using environment settings");
        break;
      case "external":
        String postgresConfigPath = System.getProperty(
          "org.folio.source.record.manager.test.config",
          "/postgres-conf-local.json");
        PostgresClient.setConfigFilePath(postgresConfigPath);
        break;
      case "embedded":
        PostgresClient.setIsEmbedded(true);
        PostgresClient.getInstance(vertx).startEmbeddedPostgres();
        break;
      default:
        String message = "No understood database choice made." +
          "Please set org.folio.source.record.manager.test.database" +
          "to 'external', 'environment' or 'embedded'";
        throw new Exception(message);
    }

    TenantClient tenantClient = new TenantClient(okapiUrl, TENANT_ID, TOKEN);

    final DeploymentOptions options = new DeploymentOptions().setConfig(new JsonObject().put(HTTP_PORT, port));
    vertx.deployVerticle(RestVerticle.class.getName(), options, res -> {
      try {
        TenantAttributes tenantAttributes = null;
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
  public void setUp(TestContext context) throws IOException {
    clearTable(context);
    spec = new RequestSpecBuilder()
      .setContentType(ContentType.JSON)
      .addHeader(OKAPI_URL_HEADER, "http://localhost:" + snapshotMockServer.port())
      .addHeader(OKAPI_TENANT_HEADER, TENANT_ID)
      .addHeader(RestVerticle.OKAPI_USERID_HEADER, okapiUserIdHeader)
      .addHeader("Accept", "text/plain, application/json")
      .setBaseUri("http://localhost:" + port)
      .build();
    Map<String, String> okapiHeaders = new HashMap<>();
    okapiHeaders.put(OKAPI_URL_HEADER, "http://localhost:" + snapshotMockServer.port());
    okapiHeaders.put(OKAPI_TENANT_HEADER, TENANT_ID);
    okapiHeaders.put(RestVerticle.OKAPI_HEADER_TOKEN, TOKEN);
    okapiHeaders.put(RestVerticle.OKAPI_USERID_HEADER, okapiUserIdHeader);

    String record = TestUtil.readFileFromPath(RECORD_PATH);

    WireMock.stubFor(WireMock.post(SNAPSHOT_SERVICE_URL)
      .willReturn(WireMock.created().withBody(postedSnapshotResponseBody)));
    WireMock.stubFor(WireMock.post(RECORDS_SERVICE_URL)
      .willReturn(WireMock.created()));
    WireMock.stubFor(WireMock.put(new UrlPathPattern(new RegexPattern(RECORD_SERVICE_URL + "/.*"), true))
      .willReturn(WireMock.ok()));
    WireMock.stubFor(WireMock.put(PARSED_RECORDS_COLLECTION_URL)
      .willReturn(WireMock.ok()));
    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern(RECORD_SERVICE_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(record)));
    WireMock.stubFor(WireMock.post(INVENTORY_URL)
      .willReturn(WireMock.created().withHeader("location", UUID.randomUUID().toString())));
    WireMock.stubFor(WireMock.put(new UrlPathPattern(new RegexPattern(SNAPSHOT_SERVICE_URL + "/.*"), true))
      .willReturn(WireMock.ok()));
    WireMock.stubFor(WireMock.get(GET_USER_URL + okapiUserIdHeader)
      .willReturn(WireMock.okJson(userResponse.toString())));
    WireMock.stubFor(WireMock.get(IDENTIFIER_TYPES_URL)
      .willReturn(WireMock.okJson(new JsonObject().toString())));
    WireMock.stubFor(WireMock.get(INSTANCE_TYPES_URL)
      .willReturn(WireMock.okJson(new JsonObject().toString())));
    WireMock.stubFor(WireMock.get(CLASSIFICATION_TYPES_URL)
      .willReturn(WireMock.okJson(new JsonObject().toString())));
    WireMock.stubFor(WireMock.get(INSTANE_FORMATS_URL)
      .willReturn(WireMock.okJson(new JsonObject().toString())));
    WireMock.stubFor(WireMock.delete(new UrlPathPattern(new RegexPattern("/source-storage/snapshots/.{36}/records"), true))
      .willReturn(WireMock.noContent()));
  }

  private void clearTable(TestContext context) {
    Async async = context.async();
    PostgresClient pgClient = PostgresClient.getInstance(vertx, TENANT_ID);
    pgClient.delete(CHUNKS_TABLE_NAME, new Criterion(), event1 -> {
      pgClient.delete(JOB_EXECUTIONS_TABLE_NAME, new Criterion(), event2 -> {
        if (event2.failed()) {
          context.fail(event2.cause());
        }
        async.complete();
      });
    });
  }

  protected InitJobExecutionsRsDto constructAndPostInitJobExecutionRqDto(int filesNumber) {
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    String jsonFiles = null;
    List<File> filesList = null;
    try {
      jsonFiles = TestUtil.readFileFromPath(FILES_PATH);
      filesList = new ObjectMapper().readValue(jsonFiles, new TypeReference<List<File>>() {
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    List<File> limitedFilesList = filesList.stream().limit(filesNumber).collect(Collectors.toList());
    requestDto.getFiles().addAll(limitedFilesList);
    requestDto.setUserId(okapiUserIdHeader);
    requestDto.setSourceType(InitJobExecutionsRqDto.SourceType.FILES);
    return RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().post(JOB_EXECUTION_PATH).body().as(InitJobExecutionsRsDto.class);
  }

  protected io.restassured.response.Response updateJobExecutionStatus(JobExecution jobExecution, StatusDto statusDto) {
    return RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(statusDto).toString())
      .when()
      .put(JOB_EXECUTION_PATH + jobExecution.getId() + "/status");
  }

  /**
   * Maps a request body to a response body.
   */
  public static class RequestToResponseTransformer extends ResponseTransformer {

    public static final String NAME = "request-to-response-transformer";

    @Override
    public Response transform(Request request, Response response, FileSource files, Parameters parameters) {
      return Response.Builder.like(response).but().body(request.getBody()).build();
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public boolean applyGlobally() {
      return false;
    }
  }

  /**
   * It takes a request, remove one instance from it and return it as a response.
   */
  public static class InstancesBatchResponseTransformer extends ResponseTransformer {

    public static final String NAME = "instances-batch-response-transformer";

    @Override
    public Response transform(Request request, Response response, FileSource files, Parameters parameters) {
      InstancesBatchResponse batchResponse = new JsonObject(request.getBodyAsString()).mapTo(InstancesBatchResponse.class);
      removeOneInstance(batchResponse);
      return Response.Builder.like(response).but().body(JsonObject.mapFrom(batchResponse).toString()).build();
    }

    private void removeOneInstance(InstancesBatchResponse batchResponse) {
      if (!batchResponse.getInstances().isEmpty()) {
        batchResponse.getInstances().remove(1);
      }
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public boolean applyGlobally() {
      return false;
    }
  }
}
