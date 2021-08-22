package org.folio.rest.impl.mappingRulesProvider;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.google.common.io.Resources;
import io.restassured.RestAssured;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.folio.rest.impl.AbstractRestTest;

/**
 * REST tests for MappingRulesProvider
 */
@RunWith(VertxUnitRunner.class)
public class MappingRulesProviderAPITest extends AbstractRestTest {
  private static final String SERVICE_PATH = "/mapping-rules";
  private static final String MARC_BIB = "/marc-bib";
  private static final String MARC_HOLDINGS= "/marc-holdings";
  private static final Charset DEFAULT_RULES_ENCODING = StandardCharsets.UTF_8;
  private static final String DEFAULT_BIB_RULES_PATH = "rules/marc_bib_rules.json";
  private static final String DEFAULT_HOLDING_RULES_PATH = "rules/marc_holding_rules.json";



  @Test
  public void shouldReturnDefaultMarcBibRulesOnGet() throws IOException {
    JsonObject expectedRules = new JsonObject(Resources.toString(Resources.getResource(DEFAULT_BIB_RULES_PATH), DEFAULT_RULES_ENCODING));
    JsonObject defaultBibRules = new JsonObject(
      RestAssured.given()
        .spec(spec)
        .when()
        .get(SERVICE_PATH + MARC_BIB)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().body().asString());
    Assert.assertNotNull(defaultBibRules);
    Assert.assertFalse(defaultBibRules.isEmpty());
    Assert.assertEquals(expectedRules, defaultBibRules);

  }

  @Test
  public void shouldReturnDefaultMarcHoldingsRulesOnGet() throws IOException {
    JsonObject expectedRules = new JsonObject(Resources.toString(Resources.getResource(DEFAULT_HOLDING_RULES_PATH), DEFAULT_RULES_ENCODING));
    JsonObject defaultHoldingsRules = new JsonObject(
      RestAssured.given()
        .spec(spec)
        .when()
        .get(SERVICE_PATH + MARC_HOLDINGS)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().body().asString());
    Assert.assertNotNull(defaultHoldingsRules);
    Assert.assertFalse(defaultHoldingsRules.isEmpty());
    Assert.assertEquals(expectedRules, defaultHoldingsRules);
  }

  @Test
  public void shouldReturnErrorOnGetByInvalidPath() {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(SERVICE_PATH + "/invalid")
        .then()
        .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

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
        .get(SERVICE_PATH + MARC_BIB)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .log().everything()
        .extract().body().asString();
    Assert.assertEquals(expectedRules.toString(), actualRules);
  }


  @Test
  public void shouldReturnBadRequestWhenSendingRulesInWrongFormatOnPut() {
    // given
    Map expectedDefaultRules =
      RestAssured.given()
        .spec(spec)
        .when()
        .get(SERVICE_PATH + MARC_BIB)
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
        .get(SERVICE_PATH + MARC_BIB)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().body().as(Map.class);
    Assert.assertEquals(expectedDefaultRules.toString(), actualRules.toString());
  }

  @Ignore("Waiting for changes by https://issues.folio.org/browse/MODSOURMAN-543")
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
        .put(SERVICE_PATH)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().body().as(Map.class);
    Assert.assertEquals(defaultRules, restoredRules);
  }
}
