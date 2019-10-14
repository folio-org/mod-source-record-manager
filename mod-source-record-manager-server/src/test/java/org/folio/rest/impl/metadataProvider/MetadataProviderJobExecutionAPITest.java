package org.folio.rest.impl.metadataProvider;

import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.jaxrs.model.StatusDto;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static org.folio.rest.jaxrs.model.JobExecution.SubordinationType.CHILD;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
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
      .body("jobExecutions", empty())
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
      .body("jobExecutions.size()", is(expectedJobExecutionsNumber))
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
      .body("jobExecutions.size()", is(2))
      .body("totalRecords", is(expectedJobExecutionsNumber));
  }

  @Test
  public void shouldNotReturnDiscardedInCollection() {
    int numberOfFiles = 5;
    int expectedNotDiscardedNumber = 2;
    List<JobExecution> createdJobExecutions = constructAndPostInitJobExecutionRqDto(numberOfFiles).getJobExecutions();
    List<JobExecution> children = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(CHILD)).collect(Collectors.toList());
    StatusDto discardedStatus = new StatusDto().withStatus(StatusDto.Status.DISCARDED);

    for (int i = 0; i < children.size() - expectedNotDiscardedNumber; i++) {
      updateJobExecutionStatus(children.get(i), discardedStatus)
        .then()
        .statusCode(HttpStatus.SC_OK);
    }

    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_JOB_EXECUTIONS_PATH + "?query=(status=\"\" NOT status=\"DISCARDED\")")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions.size()", is(expectedNotDiscardedNumber))
      .body("jobExecutions*.status", not(StatusDto.Status.DISCARDED.name()))
      .body("totalRecords", is(expectedNotDiscardedNumber));
  }

  @Test
  public void shouldReturnSortedJobExecutionsOnGetWhenSortByIsSpecified() {
    List<JobExecution> createdJobExecution = constructAndPostInitJobExecutionRqDto(5).getJobExecutions();

    for (int i = 0; i < createdJobExecution.size(); i++) {
      putJobExecution(createdJobExecution.get(i).withCompletedDate(new Date(1234567892000L + i)));
    }

    // We do not expect to get JobExecution with subordinationType=PARENT_MULTIPLE
    int expectedJobExecutionsNumber = createdJobExecution.size() - 1;
    JobExecutionCollection jobExecutionCollection = RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_JOB_EXECUTIONS_PATH + "?query=(uiStatus==\"INITIALIZATION\") sortBy completedDate/sort.descending")
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(JobExecutionCollection.class);

    List<JobExecution> jobExecutionDtoList = jobExecutionCollection.getJobExecutions();
    Assert.assertEquals(expectedJobExecutionsNumber, jobExecutionDtoList.size());
    Assert.assertTrue(jobExecutionDtoList.get(0).getCompletedDate().after(jobExecutionDtoList.get(1).getCompletedDate()));
    Assert.assertTrue(jobExecutionDtoList.get(1).getCompletedDate().after(jobExecutionDtoList.get(2).getCompletedDate()));
    Assert.assertTrue(jobExecutionDtoList.get(2).getCompletedDate().after(jobExecutionDtoList.get(3).getCompletedDate()));
  }

  @Test
  public void shouldReturnFilteredAndSortedJobExecutionsOnGetWhenConditionAndSortByIsSpecified() {
    List<JobExecution> createdJobExecution = constructAndPostInitJobExecutionRqDto(8).getJobExecutions();
    List<JobExecution> childJobsToUpdate = createdJobExecution.stream()
      .filter(jobExecution -> jobExecution.getSubordinationType().equals(CHILD))
      .collect(Collectors.toList());

    for (int i = 0; i < childJobsToUpdate.size(); i++) {
      if (i % 2 == 0) {
        childJobsToUpdate.get(i)
          .withStatus(JobExecution.Status.COMMITTED )
          .withUiStatus(JobExecution.UiStatus.RUNNING_COMPLETE);
      }
      createdJobExecution.get(i).setCompletedDate(new Date(1234567892000L + i));
      putJobExecution(createdJobExecution.get(i));
    }

    // We do not expect to get JobExecution with subordinationType=PARENT_MULTIPLE
    int expectedJobExecutionsNumber = childJobsToUpdate.size() / 2;
    JobExecutionCollection jobExecutionCollection = RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_JOB_EXECUTIONS_PATH + "?query=(uiStatus==\"RUNNING_COMPLETE\" AND status==\"COMMITTED\") sortBy completedDate/sort.descending")
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions*.status", everyItem(is(JobExecution.Status.COMMITTED.value())))
      .body("jobExecutions*.uiStatus", everyItem(is(JobExecution.UiStatus.RUNNING_COMPLETE.value())))
      .extract().response().body().as(JobExecutionCollection.class);

    List<JobExecution> jobExecutionDtoList = jobExecutionCollection.getJobExecutions();
    Assert.assertEquals(expectedJobExecutionsNumber, jobExecutionDtoList.size());
    Assert.assertTrue(jobExecutionDtoList.get(0).getCompletedDate().after(jobExecutionDtoList.get(1).getCompletedDate()));
    Assert.assertTrue(jobExecutionDtoList.get(1).getCompletedDate().after(jobExecutionDtoList.get(2).getCompletedDate()));
    Assert.assertTrue(jobExecutionDtoList.get(2).getCompletedDate().after(jobExecutionDtoList.get(3).getCompletedDate()));
  }

  private JobExecution putJobExecution(JobExecution jobExecution) {
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobExecution).encode())
      .when()
      .put(JOB_EXECUTION_PATH + jobExecution.getId())
      .then()
      .statusCode(HttpStatus.SC_OK);

    return jobExecution;
  }

}
