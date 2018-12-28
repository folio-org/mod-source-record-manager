package org.folio.rest.impl.changeManager;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.FileExtension;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class FileExtensionAPITest extends AbstractRestTest {

  private static final String FILE_EXTENSION_PATH = "/change-manager/fileExtension";

  private static JsonObject fileExtension_1 = new JsonObject()
    .put("extension", ".marc")
    .put("dataTypes", new JsonArray().add("MARC"))
    .put("importBlocked", false);
  private static JsonObject fileExtension_2 = new JsonObject()
    .put("extension", ".edi")
    .put("dataTypes", new JsonArray().add("EDIFACT"))
    .put("importBlocked", false);
  private static JsonObject fileExtension_3 = new JsonObject()
    .put("extension", ".pdf")
    .put("dataTypes", new JsonArray())
    .put("importBlocked", true);
  private static JsonObject fileExtension_4 = new JsonObject()
    .put("extension", ".marc")
    .put("dataTypes", new JsonArray())
    .put("importBlocked", true);

  @Test
  public void shouldReturnEmptyListOnGetIfNoFileExtensionsExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(FILE_EXTENSION_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(0))
      .body("fileExtensions", empty());
  }

  @Test
  public void shouldReturnAllFileExtensionsOnGetWhenNoQueryIsSpecified() {
    List<JsonObject> extensionsToPost = Arrays.asList(fileExtension_1, fileExtension_2, fileExtension_3);
    for (JsonObject extension : extensionsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(extension.toString())
        .when()
        .post(FILE_EXTENSION_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }

    RestAssured.given()
      .spec(spec)
      .when()
      .get(FILE_EXTENSION_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(extensionsToPost.size()));
  }

  @Test
  public void shouldReturnFileExtensionsWithBlockedImportSetToFalse() {
    List<JsonObject> extensionsToPost = Arrays.asList(fileExtension_1, fileExtension_2, fileExtension_3);
    for (JsonObject extension : extensionsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(extension.toString())
        .when()
        .post(FILE_EXTENSION_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }

    RestAssured.given()
      .spec(spec)
      .when()
      .get(FILE_EXTENSION_PATH + "?query=importBlocked=false")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(2))
      .body("fileExtensions*.importBlocked", everyItem(is(false)));
  }

  @Test
  public void shouldReturnLimitedCollectionOnGetWithLimit() {
    List<JsonObject> extensionsToPost = Arrays.asList(fileExtension_1, fileExtension_2, fileExtension_3);
    for (JsonObject extension : extensionsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(extension.toString())
        .when()
        .post(FILE_EXTENSION_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }

    RestAssured.given()
      .spec(spec)
      .when()
      .get(FILE_EXTENSION_PATH + "?limit=2")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("fileExtensions.size()", is(2))
      .body("totalRecords", is(extensionsToPost.size()));
  }

  @Test
  public void shouldReturnBadRequestOnPostWhenNoExtensionPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .post(FILE_EXTENSION_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldCreateFileExtensionOnPost() {
    RestAssured.given()
      .spec(spec)
      .body(fileExtension_2.toString())
      .when()
      .post(FILE_EXTENSION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("extension", is(fileExtension_2.getString("extension")))
      .body("dataTypes.size()", is(fileExtension_2.getJsonArray("dataTypes").size()))
      .body("importBlocked", is(fileExtension_2.getBoolean("importBlocked")));
  }

  @Test
  public void shouldReturnBadRequestOnPutWhenNoExtensionPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .put(FILE_EXTENSION_PATH + "/" + UUID.randomUUID().toString())
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldReturnNotFoundOnPutWhenFileExtensionDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .body(fileExtension_1.toString())
      .when()
      .put(FILE_EXTENSION_PATH + "/" + UUID.randomUUID().toString())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldUpdateExistingFileExtensionOnPut() {
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(fileExtension_1.toString())
      .when()
      .post(FILE_EXTENSION_PATH);
    Assert.assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    FileExtension fileExtension = createResponse.body().as(FileExtension.class);

    fileExtension.setImportBlocked(true);
    RestAssured.given()
      .spec(spec)
      .body(fileExtension)
      .when()
      .put(FILE_EXTENSION_PATH + "/" + fileExtension.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(fileExtension.getId()))
      .body("extension", is(fileExtension.getExtension()))
      .body("dataTypes.size()", is(fileExtension.getDataTypes().size()))
      .body("dataTypes.get(0)", is(fileExtension.getDataTypes().get(0).value()))
      .body("importBlocked", is(true));
  }

  @Test
  public void shouldReturnNotFoundOnGetByIdWhenFileExtensionDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(FILE_EXTENSION_PATH + "/" + UUID.randomUUID().toString())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnExistingFileExtensionOnGetById() {
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(fileExtension_3.toString())
      .when()
      .post(FILE_EXTENSION_PATH);
    Assert.assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    FileExtension fileExtension = createResponse.body().as(FileExtension.class);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(FILE_EXTENSION_PATH + "/" + fileExtension.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(fileExtension.getId()))
      .body("extension", is(fileExtension.getExtension()))
      .body("dataTypes", is(fileExtension.getDataTypes()))
      .body("importBlocked", is(fileExtension.getImportBlocked()));
  }

  @Test
  public void shouldReturnNotFoundOnDeleteWhenFileExtensionDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(FILE_EXTENSION_PATH + "/" + UUID.randomUUID().toString())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldDeleteExistingFileExtensionOnDelete() {
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(fileExtension_1.toString())
      .when()
      .post(FILE_EXTENSION_PATH);
    Assert.assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    FileExtension fileExtension = createResponse.body().as(FileExtension.class);

    RestAssured.given()
      .spec(spec)
      .when()
      .delete(FILE_EXTENSION_PATH + "/" + fileExtension.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
  }

  @Test
  public void shouldReturnErrorOnSavingDuplicateExtension() {
    RestAssured.given()
      .spec(spec)
      .body(fileExtension_1.toString())
      .when()
      .post(FILE_EXTENSION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("extension", is(fileExtension_1.getString("extension")))
      .body("dataTypes.size()", is(fileExtension_1.getJsonArray("dataTypes").size()))
      .body("importBlocked", is(fileExtension_1.getBoolean("importBlocked")));

    RestAssured.given()
      .spec(spec)
      .body(fileExtension_4.toString())
      .when()
      .post(FILE_EXTENSION_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

}
