package org.folio.rest.impl.metadataProvider;

import io.restassured.RestAssured;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.impl.AbstractRestTest;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class DefaultFileExtensionAPITest extends AbstractRestTest {

  private static final String FILE_EXTENSION_PATH = "/metadata-provider/fileExtension";
  private static final String FILE_EXTENSION_DEFAULT = FILE_EXTENSION_PATH + "/default";

  @Test
  public void shouldRestoreToDefault() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(FILE_EXTENSION_DEFAULT)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(16));
  }

}
