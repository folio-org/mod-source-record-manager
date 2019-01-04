package org.folio.rest.impl.changeManager;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.DataType;
import org.folio.rest.jaxrs.model.FileExtension;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class FileExtensionAPITest extends AbstractRestTest {

  private static final String FILE_EXTENSION_PATH = "/metadata-provider/fileExtension";

  private static FileExtension fileExtension_1 = new FileExtension()
    .withExtension(".marc")
    .withDataTypes(Arrays.asList(DataType.MARC))
    .withImportBlocked(false);
  private static FileExtension fileExtension_2 = new FileExtension()
    .withExtension(".edi")
    .withDataTypes(Arrays.asList(DataType.EDIFACT))
    .withImportBlocked(false);
  private static FileExtension fileExtension_3 = new FileExtension()
    .withExtension(".pdf")
    .withDataTypes(new ArrayList<>())
    .withImportBlocked(true);
  private static FileExtension fileExtension_4 = new FileExtension()
    .withExtension(".marc")
    .withDataTypes(new ArrayList<>())
    .withImportBlocked(true);

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
    List<FileExtension> extensionsToPost = Arrays.asList(fileExtension_1, fileExtension_2, fileExtension_3);
    for (FileExtension extension : extensionsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(JsonObject.mapFrom(extension).toString())
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
    List<FileExtension> extensionsToPost = Arrays.asList(fileExtension_1, fileExtension_2, fileExtension_3);
    for (FileExtension extension : extensionsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(JsonObject.mapFrom(extension).toString())
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
    List<FileExtension> extensionsToPost = Arrays.asList(fileExtension_1, fileExtension_2, fileExtension_3);
    for (FileExtension extension : extensionsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(JsonObject.mapFrom(extension).toString())
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
      .body(JsonObject.mapFrom(fileExtension_2).toString())
      .when()
      .post(FILE_EXTENSION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("extension", is(fileExtension_2.getExtension()))
      .body("dataTypes.size()", is(fileExtension_2.getDataTypes().size()))
      .body("importBlocked", is(fileExtension_2.getImportBlocked()));
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
      .body(JsonObject.mapFrom(fileExtension_1).toString())
      .when()
      .put(FILE_EXTENSION_PATH + "/" + UUID.randomUUID().toString())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldUpdateExistingFileExtensionOnPut() {
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(fileExtension_1).toString())
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
      .body(JsonObject.mapFrom(fileExtension_3).toString())
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
      .body(JsonObject.mapFrom(fileExtension_1).toString())
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
      .body(JsonObject.mapFrom(fileExtension_1).toString())
      .when()
      .post(FILE_EXTENSION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("extension", is(fileExtension_1.getExtension()))
      .body("dataTypes.size()", is(fileExtension_1.getDataTypes().size()))
      .body("importBlocked", is(fileExtension_1.getImportBlocked()));

    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(fileExtension_4).toString())
      .when()
      .post(FILE_EXTENSION_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

}
