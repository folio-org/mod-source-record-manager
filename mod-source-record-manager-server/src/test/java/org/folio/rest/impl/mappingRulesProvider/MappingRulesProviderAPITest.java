package org.folio.rest.impl.mappingRulesProvider;

import io.restassured.RestAssured;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.impl.AbstractRestTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;

/**
 * REST tests for MappingRulesProvider
 */
@RunWith(VertxUnitRunner.class)
public class MappingRulesProviderAPITest extends AbstractRestTest {
  private static final String SERVICE_PATH = "/mapping-rules/{marc-bib}";

  @Ignore
  @Test
  public void shouldReturnDefaultRulesOnGet() {
    Map defaultRules =
      RestAssured.given()
        .spec(spec)
        .when()
        .get(SERVICE_PATH)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().body().as(Map.class);
    Assert.assertNotNull(defaultRules);
    Assert.assertFalse(defaultRules.isEmpty());
  }

  @Ignore
  @Test
  public void shouldUpdateDefaultRulesOnPut() {
    // given
    JsonObject expectedRules = new JsonObject()
      .put("999", new JsonArray()
        .add(new JsonObject()
          .put("target", "instanceTypeId")));

    // when
    RestAssured.given()
      .spec(spec)
      .body(expectedRules.encode())
      .when()
      .put(SERVICE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .log().everything();
    // then
    String actualRules =
      RestAssured.given()
        .spec(spec)
        .when()
        .get(SERVICE_PATH)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .log().everything()
        .extract().body().asString();
    Assert.assertEquals(expectedRules.toString(), actualRules);
  }

  @Ignore
  @Test
  public void shouldReturnBadRequestWhenSendingRulesInWrongFormatOnPut() {
    // given
    Map expectedDefaultRules =
      RestAssured.given()
        .spec(spec)
        .when()
        .get(SERVICE_PATH)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().body().as(Map.class);
    // when
    String rulesToUpdate = "WRONG-RULES-FORMAT";
    RestAssured.given()
      .spec(spec)
      .body(rulesToUpdate)
      .when()
      .put(SERVICE_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
    // then
    Map actualRules =
      RestAssured.given()
        .spec(spec)
        .when()
        .get(SERVICE_PATH)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().body().as(Map.class);
    Assert.assertEquals(expectedDefaultRules.toString(), actualRules.toString());
  }

  @Ignore
  @Test
  public void shouldRestoreDefaultRulesOnPut() {
    // given
    Map defaultRules =
      RestAssured.given()
        .spec(spec)
        .when()
        .get(SERVICE_PATH)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().body().as(Map.class);
    Assert.assertNotNull(defaultRules);
    // when
    JsonObject rulesToUpdate = new JsonObject()
      .put("999", new JsonArray()
        .add(new JsonObject()
          .put("target", "instanceTypeId")));
    Map updatedRules = RestAssured.given()
      .spec(spec)
      .when()
      .body(rulesToUpdate.encode())
      .put(SERVICE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().body().as(Map.class);
    // then
    Assert.assertNotEquals(defaultRules, updatedRules);
    Map restoredRules =
      RestAssured.given()
        .spec(spec)
        .when()
        .put(SERVICE_PATH + "/restore")
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().body().as(Map.class);
    Assert.assertEquals(defaultRules, restoredRules);
  }
}
