package org.folio.rest.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.common.FileSource;
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
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;

import org.folio.TestUtil;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.ActionProfile;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfile;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.model.TenantJob;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.util.pubsub.PubSubClientUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.useDefaults;
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
  private static final String postedSnapshotResponseBody = UUID.randomUUID().toString();
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
  protected static final String INSTANCE_STATUSES_URL = "/instance-statuses?limit=500";
  protected static final String NATURE_OF_CONTENT_TERMS_URL = "/nature-of-content-terms?limit=500";
  protected static final String INSTANCE_RELATIONSHIP_TYPES_URL = "/instance-relationship-types?limit=500";
  protected static final String HOLDINGS_TYPES_URL = "/holdings-types?limit=500";
  protected static final String HOLDINGS_NOTE_TYPES_URL = "/holdings-note-types?limit=500";
  protected static final String ILL_POLICIES_URL = "/ill-policies?limit=500";
  protected static final String CALL_NUMBER_TYPES_URL = "/call-number-types?limit=500";
  protected static final String STATISTICAL_CODES_URL = "/statistical-codes?limit=500";
  protected static final String STATISTICAL_CODE_TYPES_URL = "/statistical-code-types?limit=500";
  protected static final String LOCATIONS_URL = "/locations?limit=500";
  protected static final String MATERIAL_TYPES_URL = "/material-types?limit=500";
  protected static final String ITEM_DAMAGED_STATUSES_URL = "/item-damaged-statuses?limit=500";
  protected static final String LOAN_TYPES_URL = "/loan-types?limit=500";
  protected static final String ITEM_NOTE_TYPES_URL = "/item-note-types?limit=500";
  protected static final String FIELD_PROTECTION_SETTINGS_URL = "/field-protection-settings/marc?limit=500";


  protected static final String FILES_PATH = "src/test/resources/org/folio/rest/files.sample";
  protected static final String RECORD_PATH = "src/test/resources/org/folio/rest/record.json";
  protected static final String SNAPSHOT_SERVICE_URL = "/source-storage/snapshots";
  protected static final String RECORDS_SERVICE_URL = "/source-storage/batch/records";
  protected static final String RECORD_SERVICE_URL = "/source-storage/records";
  protected static final String PARSED_RECORDS_COLLECTION_URL = "/source-storage/batch/parsed-records";
  protected static final String PROFILE_SNAPSHOT_URL = "/data-import-profiles/jobProfileSnapshots";
  protected static final String PUBSUB_PUBLISH_URL = "/pubsub/publish";
  protected static final String okapiUserIdHeader = UUID.randomUUID().toString();
  private static final String KAFKA_HOST = "KAFKA_HOST";
  private static final String KAFKA_PORT = "KAFKA_PORT";
  private static final String OKAPI_URL_ENV = "OKAPI_URL";
  private static final int PORT = NetworkUtils.nextFreePort();
  protected static final String OKAPI_URL = "http://localhost:" + PORT;

  private final JsonObject userResponse = new JsonObject()
    .put("users",
      new JsonArray().add(new JsonObject()
        .put("username", "diku_admin")
        .put("personal", new JsonObject().put("firstName", "DIKU").put("lastName", "ADMINISTRATOR"))))
    .put("totalRecords", 1);

  protected JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);

  private final ActionProfile actionProfile = new ActionProfile()
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
      .notifier(new ConsoleNotifier(true))
      .extensions(new RequestToResponseTransformer())
  );

  @ClassRule
  public static EmbeddedKafkaCluster cluster = provisionWith(useDefaults());

  @BeforeClass
  public static void setUpClass(final TestContext context) throws Exception {
    vertx = Vertx.vertx();
    String[] hostAndPort = cluster.getBrokerList().split(":");

    System.setProperty(KAFKA_HOST, hostAndPort[0]);
    System.setProperty(KAFKA_PORT, hostAndPort[1]);
    System.setProperty(OKAPI_URL_ENV, OKAPI_URL);
    runDatabase();
    deployVerticle(context);
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
        tenantAttributes.setModuleTo(PubSubClientUtils.constructModuleName());
        tenantClient.postTenant(tenantAttributes, res2 -> {
          if (res2.result().statusCode() == 204) {
            return;
          }
          if (res2.result().statusCode() == 201) {
            tenantClient.getTenantByOperationId(res2.result().bodyAsJson(TenantJob.class).getId(), 60000, context.asyncAssertSuccess(res3 -> {
              context.assertTrue(res3.bodyAsJson(TenantJob.class).getComplete());
              String error = res3.bodyAsJson(TenantJob.class).getError();
              if (error != null) {
                context.assertTrue(error.contains("EventDescriptor was not registered for eventType"));
              }
            }));
          } else {
            context.assertEquals("Failed to make post tenant. Received status code 400", res2.result().bodyAsString());
          }
          async.complete();
        });
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
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
    WireMock.stubFor(get(INSTANCE_STATUSES_URL).willReturn(okJson(new JsonObject().put("instanceStatuses", new JsonArray()).toString())));
    WireMock.stubFor(get(NATURE_OF_CONTENT_TERMS_URL).willReturn(okJson(new JsonObject().put("natureOfContentTerms", new JsonArray()).toString())));
    WireMock.stubFor(get(INSTANCE_RELATIONSHIP_TYPES_URL).willReturn(okJson(new JsonObject().put("instanceRelationshipTypes", new JsonArray()).toString())));
    WireMock.stubFor(get(HOLDINGS_TYPES_URL).willReturn(okJson(new JsonObject().put("holdingsTypes", new JsonArray()).toString())));
    WireMock.stubFor(get(HOLDINGS_NOTE_TYPES_URL).willReturn(okJson(new JsonObject().put("holdingsNoteTypes", new JsonArray()).toString())));
    WireMock.stubFor(get(ILL_POLICIES_URL).willReturn(okJson(new JsonObject().put("illPolicies", new JsonArray()).toString())));
    WireMock.stubFor(get(CALL_NUMBER_TYPES_URL).willReturn(okJson(new JsonObject().put("callNumberTypes", new JsonArray()).toString())));
    WireMock.stubFor(get(STATISTICAL_CODES_URL).willReturn(okJson(new JsonObject().put("statisticalCodes", new JsonArray()).toString())));
    WireMock.stubFor(get(STATISTICAL_CODE_TYPES_URL).willReturn(okJson(new JsonObject().put("statisticalCodeTypes", new JsonArray()).toString())));
    WireMock.stubFor(get(LOCATIONS_URL).willReturn(okJson(new JsonObject().put("locations", new JsonArray()).toString())));
    WireMock.stubFor(get(MATERIAL_TYPES_URL).willReturn(okJson(new JsonObject().put("mtypes", new JsonArray()).toString())));
    WireMock.stubFor(get(ITEM_DAMAGED_STATUSES_URL).willReturn(okJson(new JsonObject().put("itemDamageStatuses", new JsonArray()).toString())));
    WireMock.stubFor(get(LOAN_TYPES_URL).willReturn(okJson(new JsonObject().put("loantypes", new JsonArray()).toString())));
    WireMock.stubFor(get(ITEM_NOTE_TYPES_URL).willReturn(okJson(new JsonObject().put("itemNoteTypes", new JsonArray()).toString())));
    WireMock.stubFor(get(FIELD_PROTECTION_SETTINGS_URL).willReturn(okJson(new JsonObject().put("marcFieldProtectionSettings", new JsonArray()).toString())));


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
      filesList = new ObjectMapper().readValue(jsonFiles, new TypeReference<>() {
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

}
