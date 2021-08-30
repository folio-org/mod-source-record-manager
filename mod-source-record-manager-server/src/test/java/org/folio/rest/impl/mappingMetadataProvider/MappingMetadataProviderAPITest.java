package org.folio.rest.impl.mappingMetadataProvider;

import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.impl.AbstractRestTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class MappingMetadataProviderAPITest extends AbstractRestTest {

  private static final String SERVICE_PATH = "/mapping-metadata/";

  @Test
  public void shouldReturnDefaultMarcBibRulesOnGet() {
    JsonObject mappingMetadata = new JsonObject(
      RestAssured.given()
        .spec(spec)
        .when()
        .get(SERVICE_PATH + UUID.randomUUID() + "?recordType=MARC_BIB")
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().body().asString());
    Assert.assertNotNull(mappingMetadata);
    Assert.assertFalse(mappingMetadata.isEmpty());
  }

}
