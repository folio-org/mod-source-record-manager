package org.folio.rest.impl;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static org.folio.KafkaUtil.getKafkaHostAndPort;
import static org.folio.KafkaUtil.startKafka;
import static org.folio.KafkaUtil.stopKafka;
import static org.folio.dao.IncomingRecordDaoImpl.INCOMING_RECORDS_TABLE;
import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MATCH_PROFILE;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.services.util.EventHandlingUtil.constructModuleName;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.admin.NotFoundException;
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
import io.vertx.core.impl.VertxImpl;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.folio.KafkaUtil;
import org.folio.MappingProfile;
import org.folio.MatchProfile;
import org.folio.TestUtil;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.*;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.Envs;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.util.SharedDataUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Abstract test for the REST API testing needs.
 */
public abstract class AbstractRestTest {

  public static final String POSTGRES_IMAGE = "postgres:16-alpine";
  private static PostgreSQLContainer<?> postgresSQLContainer;

  private static final String JOB_EXECUTIONS_TABLE_NAME = "job_execution";
  private static final String CHUNKS_TABLE_NAME = "job_execution_source_chunks";
  private static final String JOURNAL_RECORDS_TABLE = "journal_records";
  private static final String JOB_EXECUTION_PROGRESS_TABLE = "job_execution_progress";
  protected static final String TOKEN = "token";
  private static final String HTTP_PORT = "http.port";
  private static int port;
  private static String useExternalDatabase;
  private static final String postedSnapshotResponseBody = UUID.randomUUID().toString();
  protected static Vertx vertx;
  protected static final String TENANT_ID = "diku";
  protected static RequestSpecification spec;

  protected static final String JOB_EXECUTION_PATH = "/change-manager/jobExecutions/";
  protected static final String GET_USER_URL = "/users?query=id==";
  protected static final String IDENTIFIER_TYPES_URL = "/identifier-types?limit=1000";
  protected static final String INSTANCE_TYPES_URL = "/instance-types?limit=1000";
  protected static final String CLASSIFICATION_TYPES_URL = "/classification-types?limit=1000";
  protected static final String INSTANCE_FORMATS_URL = "/instance-formats?limit=1000";
  protected static final String CONTRIBUTOR_TYPES_URL = "/contributor-types?limit=1000";
  protected static final String CONTRIBUTOR_NAME_TYPES_URL = "/contributor-name-types?limit=1000";
  protected static final String ELECTRONIC_ACCESS_URL = "/electronic-access-relationships?limit=1000";
  protected static final String INSTANCE_NOTE_TYPES_URL = "/instance-note-types?limit=1000";
  protected static final String INSTANCE_ALTERNATIVE_TITLE_TYPES_URL = "/alternative-title-types?limit=1000";
  protected static final String MODE_OF_ISSUANCE_TYPES_URL = "/modes-of-issuance?limit=1000";
  protected static final String INSTANCE_STATUSES_URL = "/instance-statuses?limit=1000";
  protected static final String NATURE_OF_CONTENT_TERMS_URL = "/nature-of-content-terms?limit=1000";
  protected static final String INSTANCE_RELATIONSHIP_TYPES_URL = "/instance-relationship-types?limit=1000";
  protected static final String HOLDINGS_TYPES_URL = "/holdings-types?limit=1000";
  protected static final String HOLDINGS_NOTE_TYPES_URL = "/holdings-note-types?limit=1000";
  protected static final String ILL_POLICIES_URL = "/ill-policies?limit=1000";
  protected static final String CALL_NUMBER_TYPES_URL = "/call-number-types?limit=1000";
  protected static final String STATISTICAL_CODES_URL = "/statistical-codes?limit=1000";
  protected static final String STATISTICAL_CODE_TYPES_URL = "/statistical-code-types?limit=1000";
  protected static final String LOCATIONS_URL = "/locations?limit=1000";
  protected static final String MATERIAL_TYPES_URL = "/material-types?limit=1000";
  protected static final String ITEM_DAMAGED_STATUSES_URL = "/item-damaged-statuses?limit=1000";
  protected static final String LOAN_TYPES_URL = "/loan-types?limit=1000";
  protected static final String ITEM_NOTE_TYPES_URL = "/item-note-types?limit=1000";
  protected static final String AUTHORITY_NOTE_TYPES_URL = "/authority-note-types?limit=1000";
  protected static final String AUTHORITY_SOURCE_FILES_URL = "/authority-source-files?limit=1000";
  protected static final String FIELD_PROTECTION_SETTINGS_URL = "/field-protection-settings/marc?limit=1000";
  protected static final String SUBJECT_SOURCES_URL = "/subject-sources?limit=1000";
  protected static final String SUBJECT_TYPES_URL = "/subject-types?limit=1000";
  protected static final String INSTANCE_DATE_TYPES_URL = "/instance-date-types?limit=1000";


  protected static final String TENANT_CONFIGURATION_ZONE_SETTINGS_URL = "/configurations/entries?query=" + URLEncoder.encode("(module==ORG and configName==localeSettings)", StandardCharsets.UTF_8);


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
  private static final String KAFKA_ENV = "ENV";
  protected static final String KAFKA_ENV_VALUE = "test-env";
  public static final String OKAPI_URL_ENV = "OKAPI_URL";
  private static final int PORT = NetworkUtils.nextFreePort();
  protected static final String OKAPI_URL = "http://localhost:" + PORT;
  protected static final String JOB_EXECUTION_ID_HEADER = "jobExecutionId";

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

  private final ActionProfile actionMarcHoldingsCreateProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC-Holdings ")
    .withAction(ActionProfile.Action.CREATE)
    .withFolioRecord(ActionProfile.FolioRecord.HOLDINGS);

  private final MappingProfile marcHoldingsMappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC-Holdings ")
    .withIncomingRecordType(EntityType.MARC_HOLDINGS)
    .withExistingRecordType(EntityType.HOLDINGS);

  private final MappingProfile marcInstanceMappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC-Instance ")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(EntityType.INSTANCE);

  private final ActionProfile actionMarcAuthorityCreateProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC-Holdings ")
    .withAction(ActionProfile.Action.CREATE)
    .withFolioRecord(ActionProfile.FolioRecord.AUTHORITY);

  private final ActionProfile createInvoiceActionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create invoice")
    .withAction(ActionProfile.Action.CREATE)
    .withFolioRecord(ActionProfile.FolioRecord.INVOICE);

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

  protected JobProfile orderJobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create Order")
    .withDataType(JobProfile.DataType.MARC);

  private final ActionProfile orderActionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create Order")
    .withAction(ActionProfile.Action.CREATE)
    .withFolioRecord(ActionProfile.FolioRecord.ORDER);
  protected ProfileSnapshotWrapper orderProfileSnapshotWrapperResponse = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(orderJobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(orderJobProfile)
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(orderActionProfile.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(orderActionProfile)));

  protected ProfileSnapshotWrapper profileMarcHoldingsSnapshotWrapperResponse = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(jobProfile)
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(actionMarcHoldingsCreateProfile.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(actionMarcHoldingsCreateProfile)
        .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
          .withProfileId(marcHoldingsMappingProfile.getId())
          .withContentType(MAPPING_PROFILE)
          .withContent(marcHoldingsMappingProfile)))));

  protected ProfileSnapshotWrapper profileCreateMarcHoldingsSnapshotWrapperResponse = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(new JsonObject())
    .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
      .withProfileId(UUID.randomUUID().toString())
      .withContentType(ACTION_PROFILE)
      .withContent(actionMarcHoldingsCreateProfile)
      .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
        .withProfileId(UUID.randomUUID().toString())
        .withContentType(MAPPING_PROFILE)
        .withContent(marcHoldingsMappingProfile)))));

  protected ProfileSnapshotWrapper profileCreateMarcInstanceSnapshotWrapperResponse = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(new JsonObject())
    .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
      .withProfileId(UUID.randomUUID().toString())
      .withContentType(ACTION_PROFILE)
      .withReactTo(ReactToType.NON_MATCH)
      .withContent(actionProfile)
      .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
        .withProfileId(UUID.randomUUID().toString())
        .withContentType(MAPPING_PROFILE)
        .withContent(marcInstanceMappingProfile)
      ))));

  protected ProfileSnapshotWrapper profileMarcAuthoritySnapshotWrapperResponse = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(jobProfile)
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(actionMarcAuthorityCreateProfile.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(actionMarcAuthorityCreateProfile)));

  protected ProfileSnapshotWrapper createInvoiceProfileSnapshotWrapperResponse = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(jobProfile)
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(createInvoiceActionProfile.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(createInvoiceActionProfile)));

  protected JobProfile updateJobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Update MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);

  private final ActionProfile updateActionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Update MARC Bib")
    .withAction(ActionProfile.Action.UPDATE)
    .withFolioRecord(ActionProfile.FolioRecord.INSTANCE);

  protected ProfileSnapshotWrapper updateProfileSnapshotWrapperResponse = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(updateJobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(updateJobProfile)
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(updateActionProfile.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(updateActionProfile)));

  private MatchProfile matchProfileMarcAuthorityToMarcAuthority =
    new MatchProfile()
      .withIncomingRecordType(EntityType.MARC_AUTHORITY)
      .withExistingRecordType(EntityType.MARC_AUTHORITY);

  protected ProfileSnapshotWrapper matchProfileSnapshotWrapperResponse = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(updateJobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(updateJobProfile)
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withContentType(MATCH_PROFILE)
        .withContent(matchProfileMarcAuthorityToMarcAuthority)));

  @Rule
  public WireMockRule snapshotMockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new ConsoleNotifier(true))
      .extensions(new RequestToResponseTransformer())
  );
  protected static KafkaConfig kafkaConfig;

  @BeforeClass
  public static void setUpClass(final TestContext context) throws Exception {
    vertx = Vertx.vertx();
    startKafka();
    SharedDataUtil.setIsTesting(vertx);

    String[] hostAndPort = getKafkaHostAndPort();

    System.setProperty(KAFKA_HOST, hostAndPort[0]);
    System.setProperty(KAFKA_PORT, hostAndPort[1]);
    System.setProperty(KAFKA_ENV, KAFKA_ENV_VALUE);
    System.setProperty(OKAPI_URL_ENV, OKAPI_URL);
    runDatabase();
    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(hostAndPort[0])
      .kafkaPort(hostAndPort[1])
      .envId(KAFKA_ENV_VALUE)
      .build();
    deployVerticle(context);
  }

  @After
  public void clearKafkaTopics() {
    KafkaUtil.clearAllTopics();
  }

  @AfterClass
  public static void tearDownClass(final TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      if (useExternalDatabase.equals("embedded")) {
        PostgresClient.stopPostgresTester();
      }
      stopKafka();
      async.complete();
    }));
  }

  private static void runDatabase() throws Exception {
    PostgresClient.stopPostgresTester();
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
        postgresSQLContainer = new PostgreSQLContainer<>(POSTGRES_IMAGE);
        postgresSQLContainer.start();

        Envs.setEnv(
          postgresSQLContainer.getHost(),
          postgresSQLContainer.getFirstMappedPort(),
          postgresSQLContainer.getUsername(),
          postgresSQLContainer.getPassword(),
          postgresSQLContainer.getDatabaseName()
        );
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
        tenantAttributes.setModuleTo(constructModuleName());
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
    WireMock.stubFor(get(AUTHORITY_NOTE_TYPES_URL).willReturn(okJson(new JsonObject().put("authorityNoteTypes", new JsonArray()).toString())));
    WireMock.stubFor(get(AUTHORITY_SOURCE_FILES_URL).willReturn(okJson(new JsonObject().put("authoritySourceFiles", new JsonArray()).toString())));
    WireMock.stubFor(get(SUBJECT_SOURCES_URL).willReturn(okJson(new JsonObject().put("subjectSources", new JsonArray()).toString())));
    WireMock.stubFor(get(SUBJECT_TYPES_URL).willReturn(okJson(new JsonObject().put("subjectTypes", new JsonArray()).toString())));
    WireMock.stubFor(get(INSTANCE_DATE_TYPES_URL).willReturn(okJson(new JsonObject().put("instanceDateTypes", new JsonArray()).toString())));

    WireMock.stubFor(get(FIELD_PROTECTION_SETTINGS_URL).willReturn(okJson(new JsonObject().put("marcFieldProtectionSettings", new JsonArray()).toString())));
    WireMock.stubFor(get(TENANT_CONFIGURATION_ZONE_SETTINGS_URL).willReturn(okJson(new JsonObject().put("configs", new JsonArray()).toString())));


    WireMock.stubFor(WireMock.delete(new UrlPathPattern(new RegexPattern("/source-storage/snapshots/.{36}/records"), true))
      .willReturn(WireMock.noContent()));
  }

  private void clearTable(TestContext context) {
    Async async = context.async();
    PostgresClient pgClient = PostgresClient.getInstance(vertx, TENANT_ID);
    pgClient.delete(CHUNKS_TABLE_NAME, new Criterion(), event1 ->
      pgClient.delete(JOURNAL_RECORDS_TABLE, new Criterion(), event2 ->
        pgClient.delete(INCOMING_RECORDS_TABLE, new Criterion(), event3 ->
          pgClient.delete(JOB_EXECUTION_PROGRESS_TABLE, new Criterion(), event4 ->
            pgClient.delete(JOB_EXECUTIONS_TABLE_NAME, new Criterion(), event5 -> {
              if (event4.failed()) {
                context.fail(event4.cause());
              }
              async.complete();
            })))));
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

  /** Returns list of [parent, child1, ..., childN] */
  protected List<JobExecution> constructAndPostCompositeInitJobExecutionRqDto(String filename, int children) {
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.getFiles().add(new File().withName(filename));
    requestDto.setUserId(okapiUserIdHeader);
    requestDto.setSourceType(InitJobExecutionsRqDto.SourceType.COMPOSITE);

    JobExecution parent = RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().post(JOB_EXECUTION_PATH).body().as(InitJobExecutionsRsDto.class).getJobExecutions().get(0);

    List<JobExecution> result = new ArrayList<>();
    result.add(parent);

    for (int i = 1; i <= children; i++) {
      InitJobExecutionsRqDto childRequestDto = new InitJobExecutionsRqDto();
      childRequestDto.getFiles().add(new File().withName(filename + i));
      childRequestDto.setUserId(okapiUserIdHeader);
      childRequestDto.setSourceType(InitJobExecutionsRqDto.SourceType.COMPOSITE);
      childRequestDto.setParentJobId(parent.getId());
      childRequestDto.setJobPartNumber(i);
      childRequestDto.setTotalJobParts(children);

      result.add(
        RestAssured.given()
          .spec(spec)
          .body(JsonObject.mapFrom(childRequestDto).toString())
          .when().post(JOB_EXECUTION_PATH).body().as(InitJobExecutionsRsDto.class).getJobExecutions().get(0)
      );
    }

    return result;
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

  protected String formatToKafkaTopicName(String eventType) {
    return formatToKafkaTopicName(eventType, TENANT_ID);
  }

  protected String formatToKafkaTopicName(String eventType, String tenantId) {
    return KafkaTopicNameHelper.formatTopicName(KAFKA_ENV_VALUE, getDefaultNameSpace(), tenantId, eventType);
  }

  protected  <T> T getBeanFromSpringContext(Vertx vtx, Class<T> clazz) {

    String parentVerticleUUID = vertx.deploymentIDs().stream()
      .filter(v -> !((VertxImpl) vertx).getDeployment(v).isChild())
      .findFirst()
      .orElseThrow(() -> new NotFoundException("Couldn't find the parent verticle."));

    Optional<Object> context = Optional.of(((VertxImpl) vtx).getDeployment(parentVerticleUUID).getContexts().stream()
      .findFirst().map(v -> v.get("springContext")))
      .orElseThrow(() -> new NotFoundException("Couldn't find the spring context."));

    if (context.isPresent()) {
      return ((AnnotationConfigApplicationContext) context.get()).getBean(clazz);
    }
    throw new NotFoundException(String.format("Couldn't find bean %s", clazz.getName()));
  }

  protected <V> ConsumerRecord<String, String> buildConsumerRecord(String topic, Event event) {
    ConsumerRecord<java.lang.String, String> consumerRecord = new ConsumerRecord("folio", 0, 0, topic, Json.encode(event));
    consumerRecord.headers().add(new RecordHeader(OkapiConnectionParams.OKAPI_TENANT_HEADER, TENANT_ID.getBytes(StandardCharsets.UTF_8)));
    consumerRecord.headers().add(new RecordHeader(OKAPI_URL_HEADER, ("http://localhost:" + snapshotMockServer.port()).getBytes(StandardCharsets.UTF_8)));
    consumerRecord.headers().add(new RecordHeader(OKAPI_TOKEN_HEADER, (TOKEN).getBytes(StandardCharsets.UTF_8)));
    return consumerRecord;
  }

  protected <V> ConsumerRecord<String, byte[]> buildConsumerRecordAsByteArray(String topic, Event event) {
    ConsumerRecord<java.lang.String, byte[]> consumerRecord = new ConsumerRecord("folio", 0, 0, topic, Json.encode(event).getBytes(StandardCharsets.UTF_8));
    consumerRecord.headers().add(new RecordHeader(OkapiConnectionParams.OKAPI_TENANT_HEADER, TENANT_ID.getBytes(StandardCharsets.UTF_8)));
    consumerRecord.headers().add(new RecordHeader(OKAPI_URL_HEADER, ("http://localhost:" + snapshotMockServer.port()).getBytes(StandardCharsets.UTF_8)));
    consumerRecord.headers().add(new RecordHeader(OKAPI_TOKEN_HEADER, (TOKEN).getBytes(StandardCharsets.UTF_8)));
    return consumerRecord;
  }
}
