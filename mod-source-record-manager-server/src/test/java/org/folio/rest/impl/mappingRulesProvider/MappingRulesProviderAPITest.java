package org.folio.rest.impl.mappingRulesProvider;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.folio.TestUtil;
import org.folio.rest.impl.AbstractRestTest;

/**
 * REST tests for MappingRulesProvider
 */
@RunWith(VertxUnitRunner.class)
public class MappingRulesProviderAPITest extends AbstractRestTest {
  private static final String SERVICE_PATH = "/mapping-rules";
  private static final String MARC_BIB = "/marc-bib";
  private static final String MARC_HOLDINGS = "/marc-holdings";
  private static final String RESTORE = "/restore";
  private static final String DEFAULT_MARC_BIB_RULES_PATH = "src/main/resources/rules/marc_bib_rules.json";
  private static final String DEFAULT_MARC_HOLDINGS_RULES_PATH = "src/main/resources/rules/marc_holdings_rules.json";

  private static final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void shouldReturnDefaultMarcBibRulesOnGet() throws IOException {
    JsonObject expectedRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MARC_BIB_RULES_PATH));
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
    JsonObject expectedRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MARC_HOLDINGS_RULES_PATH));
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
  public void shouldUpdateDefaultMarcBibRulesOnPut() throws IOException {
    // given
    String expectedRules = TestUtil.readFileFromPath(DEFAULT_MARC_BIB_RULES_PATH);
    // when
    RestAssured.given()
      .spec(spec)
      .body(expectedRules)
      .when()
      .put(SERVICE_PATH + MARC_BIB)
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

    Assert.assertEquals(mapper.readTree(expectedRules), mapper.readTree(actualRules));
  }

  @Test
  public void shouldUpdateDefaultMarcHoldingsRulesOnPut() throws IOException {
    // given
    String expectedRules = TestUtil.readFileFromPath(DEFAULT_MARC_HOLDINGS_RULES_PATH);
    // when
    RestAssured.given()
      .spec(spec)
      .body(expectedRules)
      .when()
      .put(SERVICE_PATH + MARC_HOLDINGS)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .log().everything();
    // then
    String actualRules =
      RestAssured.given()
        .spec(spec)
        .when()
        .get(SERVICE_PATH + MARC_HOLDINGS)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .log().everything()
        .extract().body().asString();

    Assert.assertEquals(mapper.readTree(expectedRules), mapper.readTree(actualRules));
  }

  @Test
  public void shouldReturnBadRequestWhenSendingRulesInWrongFormatOnPut() {
    // given
    String expectedDefaultRules =
      RestAssured.given()
        .spec(spec)
        .when()
        .get(SERVICE_PATH + MARC_BIB)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().body().asString();
    // when
    String rulesToUpdate = "WRONG-RULES-FORMAT";
    RestAssured.given()
      .spec(spec)
      .body(rulesToUpdate)
      .when()
      .put(SERVICE_PATH + MARC_BIB)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
    // then
    String actualRules =
      RestAssured.given()
        .spec(spec)
        .when()
        .get(SERVICE_PATH + MARC_BIB)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().body().asString();
    Assert.assertEquals(expectedDefaultRules, actualRules);
  }

  @Test
  public void shouldRestoreDefaultMarcBibRules() throws IOException {
    // given
    JsonObject defaultRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MARC_BIB_RULES_PATH));
    // when
    JsonObject rulesToUpdate = new JsonObject()
      .put("999", new JsonArray()
        .add(new JsonObject()
          .put("target", "instanceTypeId")));
    JsonObject updatedRules = new JsonObject(
      RestAssured.given()
        .spec(spec)
        .when()
        .body(rulesToUpdate.encode())
        .put(SERVICE_PATH + MARC_BIB)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().body().asString());
    Assert.assertNotEquals(defaultRules, updatedRules);
    // then
    JsonObject restoredRules = new JsonObject(
      RestAssured.given()
        .spec(spec)
        .when()
        .put(SERVICE_PATH + MARC_BIB + RESTORE)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().body().asString());
    Assert.assertEquals(defaultRules, restoredRules);
  }

  @Test
  public void shouldRestoreDefaultMarcHoldingsRules() throws IOException {
    // given
    JsonObject defaultRules = new JsonObject(TestUtil.readFileFromPath(DEFAULT_MARC_HOLDINGS_RULES_PATH));
    // when
    JsonObject rulesToUpdate = new JsonObject()
      .put("999", new JsonArray()
        .add(new JsonObject()
          .put("target", "instanceTypeId")));
    JsonObject updatedRules = new JsonObject(
      RestAssured.given()
        .spec(spec)
        .when()
        .body(rulesToUpdate.encode())
        .put(SERVICE_PATH + MARC_HOLDINGS)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().body().asString());
    Assert.assertNotEquals(defaultRules, updatedRules);
    // then
    JsonObject restoredRules = new JsonObject(
      RestAssured.given()
        .spec(spec)
        .when()
        .put(SERVICE_PATH + MARC_HOLDINGS + RESTORE)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().body().asString());
    Assert.assertEquals(defaultRules, restoredRules);
  }

  @Test
  public void shouldReturnErrorOnRestoreByInvalidPath() {
    RestAssured.given()
      .spec(spec)
      .when()
      .put(SERVICE_PATH + "/invalid" + RESTORE)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }
}
