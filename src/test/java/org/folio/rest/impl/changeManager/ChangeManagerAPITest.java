package org.folio.rest.impl.changeManager;

import io.restassured.RestAssured;
import io.restassured.response.ResponseBodyExtractionOptions;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.ws.rs.core.MediaType;
import java.util.List;

import static org.folio.rest.RestVerticle.OKAPI_HEADER_TENANT;

/**
 * REST tests for ChangeManager to manager JobExecution entities initialization
 */

@RunWith(VertxUnitRunner.class)
public class ChangeManagerAPITest extends AbstractRestTest {


  private static final String CHANGE_MANAGER_PATH = "/change-manager";
  private static final String ACCEPT_VALUES = "application/json, text/plain";

  @Test
  public void testInitJobExecutions(TestContext context) {
    String servicePath = "/jobExecutions";
    String testUrl = CHANGE_MANAGER_PATH + servicePath;
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();

    requestDto.getFiles().add(new File().withName("importBib.bib"));
    requestDto.getFiles().add(new File().withName("importMarc.marc"));

    ResponseBodyExtractionOptions body = getDefaultGiven()
      .port(port)
      .header(TENANT_HEADER)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().post(testUrl)
      .then().statusCode(201)
      .extract().body();

    String actualParentJobExecutionId = body.jsonPath().getObject("parentJobExecutionId", String.class);
    List<JobExecution> actualJobExecutions = body.jsonPath().getObject("jobExecutions", List.class);

    Assert.assertNotNull(actualParentJobExecutionId);
    Assert.assertEquals(requestDto.getFiles().size(), actualJobExecutions.size());
  }

  protected RequestSpecification getDefaultGiven() {
    return RestAssured.given()
      .header(OKAPI_HEADER_TENANT, TENANT_ID)
      .header(HttpHeaders.ACCEPT.toString(), ACCEPT_VALUES)
      .header(HttpHeaders.CONTENT_TYPE.toString(), MediaType.APPLICATION_JSON);
  }
}
