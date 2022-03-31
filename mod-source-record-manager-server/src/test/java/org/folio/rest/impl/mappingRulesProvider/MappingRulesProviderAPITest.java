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
  private static final String MARC_AUTHORITY = "/marc-authority";
  private static final String RESTORE = "/restore";
  private static final String DEFAULT_MARC_BIB_RULES_PATH = "src/main/resources/rules/marc_bib_rules.json";
  private static final String DEFAULT_MARC_HOLDINGS_RULES_PATH = "src/main/resources/rules/marc_holdings_rules.json";
  private static final String DEFAULT_MARC_AUTHORITY_RULES_PATH = "src/main/resources/rules/marc_authority_rules.json";

  private static final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void shouldReturnDefaultMarcBibRulesOnGet() throws IOException {
    shouldReturnDefaultMarcRulesOnGet(DEFAULT_MARC_BIB_RULES_PATH, MARC_BIB);
  }

  @Test
  public void shouldReturnDefaultMarcHoldingsRulesOnGet() throws IOException {
    shouldReturnDefaultMarcRulesOnGet(DEFAULT_MARC_HOLDINGS_RULES_PATH, MARC_HOLDINGS);
  }

  @Test
  public void shouldReturnDefaultMarcAuthorityRulesOnGet() throws IOException {
    shouldReturnDefaultMarcRulesOnGet(DEFAULT_MARC_AUTHORITY_RULES_PATH, MARC_AUTHORITY);
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
    shouldUpdateDefaultMarcRulesOnPut(DEFAULT_MARC_BIB_RULES_PATH, MARC_BIB);
  }

  @Test
  public void shouldUpdateDefaultMarcHoldingsRulesOnPut() throws IOException {
    shouldUpdateDefaultMarcRulesOnPut(DEFAULT_MARC_HOLDINGS_RULES_PATH, MARC_HOLDINGS);
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
    shouldReturnBadRequestWhenTryToModifiedAuthorityRules("WRONG-RULES-FORMAT", SERVICE_PATH + MARC_BIB);
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
    shouldRestoreDefaultMarcRules(DEFAULT_MARC_BIB_RULES_PATH, MARC_BIB);
  }

  @Test
  public void shouldRestoreDefaultMarcHoldingsRules() throws IOException {
    shouldRestoreDefaultMarcRules(DEFAULT_MARC_HOLDINGS_RULES_PATH, MARC_HOLDINGS);
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

  @Test
  public void shouldReturnBadRequestWhenTryEditAuthorityRules() throws IOException {
    shouldReturnBadRequestWhenTryToModifiedAuthorityRules(TestUtil.readFileFromPath(DEFAULT_MARC_AUTHORITY_RULES_PATH),
      SERVICE_PATH + MARC_AUTHORITY);
  }

  private void shouldReturnBadRequestWhenTryToModifiedAuthorityRules(String defaultAuthorityRules, String url) {
    RestAssured.given()
      .spec(spec)
      .body(defaultAuthorityRules)
      .when()
      .put(url)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  private void shouldReturnDefaultMarcRulesOnGet(String defaultMarcBibRulesPath, String recordType) throws IOException {
    JsonObject expectedRules = new JsonObject(TestUtil.readFileFromPath(defaultMarcBibRulesPath));
    JsonObject defaultRules = new JsonObject(
      RestAssured.given()
        .spec(spec)
        .when()
        .get(SERVICE_PATH + recordType)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().body().asString());
    Assert.assertNotNull(defaultRules);
    Assert.assertFalse(defaultRules.isEmpty());
    Assert.assertEquals(expectedRules, defaultRules);
  }

  private void shouldUpdateDefaultMarcRulesOnPut(String defaultMarcBibRulesPath, String marcBib) throws IOException {
    // given
    String expectedRules = TestUtil.readFileFromPath(defaultMarcBibRulesPath);
    // when
    RestAssured.given()
      .spec(spec)
      .body(expectedRules)
      .when()
      .put(SERVICE_PATH + marcBib)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .log().everything();
    // then
    String actualRules =
      RestAssured.given()
        .spec(spec)
        .when()
        .get(SERVICE_PATH + marcBib)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .log().everything()
        .extract().body().asString();

    Assert.assertEquals(mapper.readTree(expectedRules), mapper.readTree(actualRules));
  }

  private void shouldRestoreDefaultMarcRules(String defaultMarcBibRulesPath, String recordType) throws IOException {
    // given
    JsonObject defaultRules = new JsonObject(TestUtil.readFileFromPath(defaultMarcBibRulesPath));
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
        .put(SERVICE_PATH + recordType)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().body().asString());
    Assert.assertNotEquals(defaultRules, updatedRules);
    // then
    JsonObject restoredRules = new JsonObject(
      RestAssured.given()
        .spec(spec)
        .when()
        .put(SERVICE_PATH + recordType + RESTORE)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().body().asString());
    Assert.assertEquals(defaultRules, restoredRules);
  }

}
