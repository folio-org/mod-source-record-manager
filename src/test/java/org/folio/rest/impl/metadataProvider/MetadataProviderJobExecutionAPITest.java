package org.folio.rest.impl.metadataProvider;

import io.restassured.RestAssured;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.impl.metadataProvider.AbstractMetadataProviderTest;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

/**
 * REST tests for MetadataProvider to manager JobExecution entities
 */
@RunWith(VertxUnitRunner.class)
public class MetadataProviderJobExecutionAPITest extends AbstractMetadataProviderTest {

  private static final String JOB_EXECUTIONS_TABLE_NAME = "job_executions";
  private String servicePath = "/jobExecutions";

  public void setUp(TestContext context) {
    clearJobExecutionsTable(context);
  }

  @Test
  public void shouldReturnEmptyListIfNoJobExecutionsExist(final TestContext context) {
    RestAssured.given()
      .port(port)
      .header(TENANT_HEADER)
      .when()
      .get(METADATA_PROVIDER_PATH + servicePath)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutionDtos", empty())
      .body("totalRecords", is(0));
  }

  private void clearJobExecutionsTable(TestContext context) {
    PostgresClient.getInstance(vertx, TENANT_ID).delete(JOB_EXECUTIONS_TABLE_NAME, new Criterion(), event -> {
      if (event.failed()) {
        context.fail(event.cause());
      }
    });
  }
}
