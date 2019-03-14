package org.folio.rest.impl.metadataProvider;

import io.restassured.RestAssured;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.StatusDto;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * REST tests for MetadataProvider to manager JobExecution entities
 */
@RunWith(VertxUnitRunner.class)
public class MetadataProviderJobExecutionAPITest extends AbstractRestTest {

  private static final String GET_JOB_EXECUTIONS_PATH = "/metadata-provider/jobExecutions";

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
    List<JobExecution> createdJobExecution = constructAndPostInitJobExecutionRqDto(5).getJobExecutions();
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
    List<JobExecution> createdJobExecution = constructAndPostInitJobExecutionRqDto(5).getJobExecutions();
    int givenJobExecutionsNumber = createdJobExecution.size();
    // We do not expect to get JobExecution with subordinationType=PARENT_MULTIPLE
    int expectedJobExecutionsNumber = givenJobExecutionsNumber - 1;
    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_JOB_EXECUTIONS_PATH + "?limit=2")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutionDtos.size()", is(2))
      .body("totalRecords", is(expectedJobExecutionsNumber));
  }

  @Test
  public void shouldNotReturnDiscardedInCollection() {
    int numberOfFiles = 5;
    int expectedNotDiscardedNumber = 2;
    List<JobExecution> createdJobExecutions = constructAndPostInitJobExecutionRqDto(numberOfFiles).getJobExecutions();
    List<JobExecution> children = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.CHILD)).collect(Collectors.toList());
    StatusDto discardedStatus = new StatusDto().withStatus(StatusDto.Status.DISCARDED);

    for (int i = 0; i < children.size() - expectedNotDiscardedNumber; i++) {
      updateJobExecutionStatus(children.get(i), discardedStatus)
        .then()
        .statusCode(HttpStatus.SC_OK);
    }

    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_JOB_EXECUTIONS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutionDtos.size()", is(expectedNotDiscardedNumber))
      .body("jobExecutionDtos*.status", not(StatusDto.Status.DISCARDED.name()))
      .body("totalRecords", is(expectedNotDiscardedNumber));
  }

}
