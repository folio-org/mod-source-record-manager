package org.folio.rest.impl.mappingMetadataProvider;

import io.restassured.RestAssured;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.dao.MappingParamsSnapshotDaoImpl;
import org.folio.dao.MappingRulesSnapshotDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.impl.AbstractRestTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class MappingMetadataProviderAPITest extends AbstractRestTest {

  private static final String SERVICE_PATH = "/mapping-metadata/";
  @Spy
  Vertx vertx = Vertx.vertx();
  @Spy
  @InjectMocks
  PostgresClientFactory clientFactory;
  @Spy
  @InjectMocks
  private MappingRulesSnapshotDaoImpl mappingRulesSnapshotDao;
  @Spy
  @InjectMocks
  private MappingParamsSnapshotDaoImpl mappingParamsSnapshotDao;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }


  @Test
  public void shouldReturnNotFoundIfNoMetadataForJobExecutionExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SERVICE_PATH + UUID.randomUUID())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

}
