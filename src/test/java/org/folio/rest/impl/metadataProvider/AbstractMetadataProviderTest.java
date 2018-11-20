package org.folio.rest.impl.metadataProvider;

import io.restassured.RestAssured;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.persist.PostgresClient;
import org.junit.AfterClass;
import org.junit.Before;

import javax.ws.rs.core.MediaType;

import static org.folio.rest.RestVerticle.OKAPI_HEADER_TENANT;

/**
 * Common abstract class for data necessary for testing the MetadataProvider
 */
public abstract class AbstractMetadataProviderTest extends AbstractRestTest {

  protected static final int CONTENT_LENGTH_DEFAULT = 1000;
  protected static final String METADATA_PROVIDER_PATH = "/metadata-provider";
  protected static final String ACCEPT_VALUES = "application/json, text/plain";
  protected static final String CONTENT_LENGTH = "Content-Length";

  @AfterClass
  public static void tearDownClass(final TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      if (useExternalDatabase.equals("embedded")) {
        PostgresClient.stopEmbeddedPostgres();
      }
      async.complete();
    }));
  }

  @Before
  abstract public void setUp(TestContext context) throws Exception;

  protected RequestSpecification getDefaultGiven() {
    return RestAssured.given()
      .header(OKAPI_HEADER_TENANT, TENANT_ID)
      .header(HttpHeaders.ACCEPT.toString(), ACCEPT_VALUES)
      .header(HttpHeaders.CONTENT_TYPE.toString(), MediaType.APPLICATION_JSON)
      .header(CONTENT_LENGTH, CONTENT_LENGTH_DEFAULT);
  }
}
