package org.folio.rest.impl.metadataProvider;

import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

/**
 * REST tests for MetadataProvider to manager JobExecution entities
 */
@RunWith(VertxUnitRunner.class)
public class MetadataProviderJobExecutionAPITest extends AbstractRestTest {

  private static final String GET_JOB_EXECUTIONS_PATH = "/metadata-provider/jobExecutions";
  private static final String POST_JOB_EXECUTIONS_PATH = "/change-manager/jobExecutions";

  @Test
  public void shouldReturnEmptyListIfNoJobExecutionsExist(final TestContext context) {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_JOB_EXECUTIONS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutionDtos", empty())
      .body("totalRecords", is(0));
  }

  @Test
  public void shouldReturnAllJobExecutionsOnGetWhenNoQueryIsSpecified() {
    List<JobExecution> createdJobExecution = createJobExecutions();
    int givenJobExecutionsNumber = createdJobExecution.size();
    // We do not expect to get JobExecution with subordinationType=PARENT_MULTIPLE
    int expectedJobExecutionsNumber = givenJobExecutionsNumber - 1;
    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_JOB_EXECUTIONS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutionDtos.size()", is(expectedJobExecutionsNumber))
      .body("totalRecords", is(expectedJobExecutionsNumber));
  }

  @Test
  public void shouldReturnLimitedCollectionOnGetWithLimit() {
    List<JobExecution> createdJobExecution = createJobExecutions();
    int givenJobExecutionsNumber = createdJobExecution.size();
    // We do not expect to get JobExecution with subordinationType=PARENT_MULTIPLE
    int expectedJobExecutionsNumber = givenJobExecutionsNumber - 1;
    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_JOB_EXECUTIONS_PATH + "?limit=2")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutionDtos.size()", is(expectedJobExecutionsNumber))
      .body("totalRecords", is(expectedJobExecutionsNumber));
  }

  private List<JobExecution> createJobExecutions() {
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.getFiles().add(new File().withName("importBib.bib"));
    requestDto.getFiles().add(new File().withName("importMarc.mrc"));
    requestDto.setUserId(UUID.randomUUID().toString());
    InitJobExecutionsRsDto response = RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when()
      .post(POST_JOB_EXECUTIONS_PATH)
      .body().as(InitJobExecutionsRsDto.class);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(3));
    return createdJobExecutions;
  }

}
