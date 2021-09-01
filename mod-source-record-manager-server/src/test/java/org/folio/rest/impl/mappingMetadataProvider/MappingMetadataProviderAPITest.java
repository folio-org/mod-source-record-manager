package org.folio.rest.impl.mappingMetadataProvider;

import io.restassured.RestAssured;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.TestUtil;
import org.folio.dao.MappingParamsSnapshotDaoImpl;
import org.folio.dao.MappingRulesSnapshotDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.impl.AbstractRestTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.io.IOException;
import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class MappingMetadataProviderAPITest extends AbstractRestTest {

  private static final String SERVICE_PATH = "/mapping-metadata/";
  private static final String MARC_BIB_RULES_PATH = "src/test/resources/org/folio/services/marc_bib_rules.json";
  private static final String MARC_PARAMS_PATH = "src/test/resources/org/folio/services/marc_mapping_params.json";

  @Spy
  PostgresClientFactory postgresClientFactory = new PostgresClientFactory(Vertx.vertx());

  @InjectMocks
  MappingRulesSnapshotDaoImpl mappingRulesSnapshotDao;
  @InjectMocks
  MappingParamsSnapshotDaoImpl mappingParamsSnapshotDao;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void shouldReturnNotFoundIfNoMetadataExistForJobExecutionId() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SERVICE_PATH + UUID.randomUUID().toString())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnMappingMetadataOnGet(TestContext context) {
    Async async = context.async();
    String jobExecutionId = UUID.randomUUID().toString();
    addTestData(jobExecutionId).onSuccess(ar -> {
      JsonObject mappingMetadata = new JsonObject(
        RestAssured.given()
          .spec(spec)
          .when()
          .get(SERVICE_PATH + jobExecutionId)
          .then()
          .statusCode(HttpStatus.SC_OK)
          .extract().body().asString());
      Assert.assertNotNull(mappingMetadata);
      Assert.assertFalse(mappingMetadata.isEmpty());
      Assert.assertEquals(jobExecutionId, mappingMetadata.getString("jobExecutionId"));
      async.complete();
    });
  }

  private Future<String> saveMappingRules(String jobExecutionId) {
    JsonObject rules;
    try {
      rules = new JsonObject(TestUtil.readFileFromPath(MARC_BIB_RULES_PATH));
      return mappingRulesSnapshotDao.save(rules, jobExecutionId, TENANT_ID);
    } catch (IOException e) {
      return Future.failedFuture(e);
    }
  }

  private Future<String> saveMappingParams(String jobExecutionId) {
    JsonObject params;
    try {
      params = new JsonObject(TestUtil.readFileFromPath(MARC_PARAMS_PATH));
      return mappingParamsSnapshotDao.save(params.mapTo(MappingParameters.class), jobExecutionId, TENANT_ID);
    } catch (IOException e) {
      return Future.failedFuture(e);
    }
  }

  private Future<String> addTestData(String jobExecutionId) {
    return saveMappingRules(jobExecutionId)
      .compose(ar -> saveMappingParams(jobExecutionId));
  }
}
