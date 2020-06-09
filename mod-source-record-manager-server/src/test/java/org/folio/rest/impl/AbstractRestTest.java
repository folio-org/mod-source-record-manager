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
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.folio.TestUtil;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.ActionProfile;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.InstancesBatchResponse;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfile;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.PomReader;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;

/**
 * Abstract test for the REST API testing needs.
 */
public abstract class AbstractRestTest {

  private static final String JOB_EXECUTIONS_TABLE_NAME = "job_executions";
  private static final String CHUNKS_TABLE_NAME = "job_execution_source_chunks";
  private static final String JOURNAL_RECORDS_TABLE = "journal_records";
  private static final String JOB_EXECUTION_PROGRESS_TABLE = "job_execution_progress";
  private static final String TOKEN = "token";
  private static final String HTTP_PORT = "http.port";
  private static int port;
  private static String useExternalDatabase;
  private static String postedSnapshotResponseBody = UUID.randomUUID().toString();
  private static Vertx vertx;
  protected static final String TENANT_ID = "diku";
  protected static RequestSpecification spec;

  protected static final String JOB_EXECUTION_PATH = "/change-manager/jobExecutions/";
  private static final String GET_USER_URL = "/users?query=id==";
  protected static final String IDENTIFIER_TYPES_URL = "/identifier-types?limit=500";
  protected static final String INSTANCE_TYPES_URL = "/instance-types?limit=500";
  protected static final String CLASSIFICATION_TYPES_URL = "/classification-types?limit=500";
  protected static final String INSTANCE_FORMATS_URL = "/instance-formats?limit=500";
  protected static final String CONTRIBUTOR_TYPES_URL = "/contributor-types?limit=500";
  protected static final String CONTRIBUTOR_NAME_TYPES_URL = "/contributor-name-types?limit=500";
  protected static final String ELECTRONIC_ACCESS_URL = "/electronic-access-relationships?limit=500";
  protected static final String INSTANCE_NOTE_TYPES_URL = "/instance-note-types?limit=500";
  protected static final String INSTANCE_ALTERNATIVE_TITLE_TYPES_URL = "/alternative-title-types?limit=500";
  protected static final String MODE_OF_ISSUANCE_TYPES_URL = "/modes-of-issuance?limit=500";

  protected static final String FILES_PATH = "src/test/resources/org/folio/rest/files.sample";
  protected static final String RECORD_PATH = "src/test/resources/org/folio/rest/record.json";
  protected static final String SNAPSHOT_SERVICE_URL = "/source-storage/snapshots";
  protected static final String RECORDS_SERVICE_URL = "/source-storage/batch/records";
  protected static final String RECORD_SERVICE_URL = "/source-storage/records";
  protected static final String INVENTORY_URL = "/inventory/instances/batch";
  protected static final String PARSED_RECORDS_COLLECTION_URL = "/source-storage/batch/parsed-records";
  protected static final String PROFILE_SNAPSHOT_URL = "/data-import-profiles/jobProfileSnapshots";
  protected static final String PUBSUB_PUBLISH_URL = "/pubsub/publish";
  protected static final String okapiUserIdHeader = UUID.randomUUID().toString();

  private JsonObject userResponse = new JsonObject()
    .put("users",
      new JsonArray().add(new JsonObject()
        .put("username", "diku_admin")
        .put("personal", new JsonObject().put("firstName", "DIKU").put("lastName", "ADMINISTRATOR"))))
    .put("totalRecords", 1);

  protected JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);

  private ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC Bib")
    .withAction(ActionProfile.Action.CREATE)
    .withFolioRecord(ActionProfile.FolioRecord.INSTANCE);

  protected ProfileSnapshotWrapper profileSnapshotWrapperResponse = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(jobProfile)
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(actionProfile.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(actionProfile)));

  @Rule
  public WireMockRule snapshotMockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true))
      .extensions(new RequestToResponseTransformer(), new InstancesBatchResponseTransformer())
  );


  @BeforeClass
  public static void setUpClass(final TestContext context) throws Exception {
    vertx = Vertx.vertx();
    runDatabase();
    deployVerticle(context);
  }

  private static void runDatabase() throws Exception {
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
  }

  private static void deployVerticle(final TestContext context) {
    Async async = context.async();
    port = NetworkUtils.nextFreePort();
    String okapiUrl = "http://localhost:" + port;
    TenantClient tenantClient = new TenantClient(okapiUrl, TENANT_ID, TOKEN);
    final DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject()
        .put(HTTP_PORT, port));
    vertx.deployVerticle(RestVerticle.class.getName(), options, deployVerticleAr -> {
      try {
        TenantAttributes tenantAttributes = new TenantAttributes();
        tenantAttributes.setModuleTo(PomReader.INSTANCE.getModuleName());
        tenantClient.postTenant(tenantAttributes, postTenantAr -> {
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
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(RECORD_SERVICE_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(record)));
    WireMock.stubFor(WireMock.post(INVENTORY_URL)
      .willReturn(WireMock.created().withHeader("location", UUID.randomUUID().toString())));
    WireMock.stubFor(WireMock.put(new UrlPathPattern(new RegexPattern(SNAPSHOT_SERVICE_URL + "/.*"), true))
      .willReturn(WireMock.ok()));
    WireMock.stubFor(post(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.created().withBody(Json.encode(profileSnapshotWrapperResponse))));
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(profileSnapshotWrapperResponse))));
    WireMock.stubFor(post(PUBSUB_PUBLISH_URL)
      .willReturn(WireMock.noContent()));

    WireMock.stubFor(get(GET_USER_URL + okapiUserIdHeader)
      .willReturn(okJson(userResponse.toString())));
    WireMock.stubFor(get(IDENTIFIER_TYPES_URL).willReturn(okJson(new JsonObject().put("identifierTypes", new JsonArray()).toString())));
    WireMock.stubFor(get(INSTANCE_TYPES_URL).willReturn(okJson(new JsonObject().put("instanceTypes", new JsonArray()).toString())));
    WireMock.stubFor(get(CLASSIFICATION_TYPES_URL).willReturn(okJson(new JsonObject().put("classificationTypes", new JsonArray()).toString())));
    WireMock.stubFor(get(ELECTRONIC_ACCESS_URL).willReturn(okJson(new JsonObject().put("electronicAccessRelationships", new JsonArray()).toString())));
    WireMock.stubFor(get(INSTANCE_FORMATS_URL).willReturn(okJson(new JsonObject().put("instanceFormats", new JsonArray()).toString())));
    WireMock.stubFor(get(CONTRIBUTOR_NAME_TYPES_URL).willReturn(okJson(new JsonObject().put("contributorNameTypes", new JsonArray()).toString())));
    WireMock.stubFor(get(CONTRIBUTOR_TYPES_URL).willReturn(okJson(new JsonObject().put("contributorTypes", new JsonArray()).toString())));
    WireMock.stubFor(get(INSTANCE_NOTE_TYPES_URL).willReturn(okJson(new JsonObject().put("instanceNoteTypes", new JsonArray()).toString())));
    WireMock.stubFor(get(INSTANCE_ALTERNATIVE_TITLE_TYPES_URL).willReturn(okJson(new JsonObject().put("alternativeTitleTypes", new JsonArray()).toString())));
    WireMock.stubFor(get(MODE_OF_ISSUANCE_TYPES_URL).willReturn(okJson(new JsonObject().put("issuanceModes", new JsonArray()).toString())));
    WireMock.stubFor(WireMock.delete(new UrlPathPattern(new RegexPattern("/source-storage/snapshots/.{36}/records"), true))
      .willReturn(WireMock.noContent()));
  }

  private void clearTable(TestContext context) {
    Async async = context.async();
    PostgresClient pgClient = PostgresClient.getInstance(vertx, TENANT_ID);
    pgClient.delete(CHUNKS_TABLE_NAME, new Criterion(), event1 ->
      pgClient.delete(JOURNAL_RECORDS_TABLE, new Criterion(), event2 ->
        pgClient.delete(JOB_EXECUTION_PROGRESS_TABLE, new Criterion(), event3 ->
          pgClient.delete(JOB_EXECUTIONS_TABLE_NAME, new Criterion(), event4 -> {
            if (event3.failed()) {
              context.fail(event3.cause());
            }
            async.complete();
          }))));
  }

  protected InitJobExecutionsRsDto constructAndPostInitJobExecutionRqDto(int filesNumber) {
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    String jsonFiles;
    List<File> filesList;
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
