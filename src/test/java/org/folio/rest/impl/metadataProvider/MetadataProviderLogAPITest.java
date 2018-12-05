package org.folio.rest.impl.metadataProvider;

import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.LogCollectionDto;
import org.folio.rest.jaxrs.model.LogDto;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.folio.rest.jaxrs.model.JobExecution.Status.COMMITTED;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

/**
 * REST tests for MetadataProvider to manager Log entities
 */
@RunWith(VertxUnitRunner.class)
public class MetadataProviderLogAPITest extends AbstractRestTest {

  private static final String GET_LOGS_PATH = "/metadata-provider/logs";
  private static final String POST_JOB_EXECUTIONS_PATH = "/change-manager/jobExecutions";
  private static final String PUT_JOB_EXECUTIONS_PATH = "/change-manager/jobExecution/";
  private static final String profileName = "Parse Marc files profile";

  @Test
  public void shouldReturnEmptyListIfNoLogsExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("logDtos", empty())
      .body("totalRecords", is(0));
  }

  @Test
  public void shouldReturnEmptyListIfCreated1ParentMultiple2ChildJobExecutions() {
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.getFiles().add(new File().withName("importBib.bib"));
    requestDto.getFiles().add(new File().withName("importMarc.mrc"));

    InitJobExecutionsRsDto response = RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when()
      .post(POST_JOB_EXECUTIONS_PATH)
      .body().as(InitJobExecutionsRsDto.class);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(3));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("logDtos", empty())
      .body("totalRecords", is(0));
  }

  @Test
  public void shouldReturnEmptyListIfCreated1ParentSingleJobExecution() {
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.getFiles().add(new File().withName("importBib.bib"));

    InitJobExecutionsRsDto response = RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when()
      .post(POST_JOB_EXECUTIONS_PATH)
      .body().as(InitJobExecutionsRsDto.class);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("logDtos", empty())
      .body("totalRecords", is(0));
  }

  @Test
  public void shouldReturn1LogIfCreated1ParentMultiple1ChildNew1ChildCommittedJobExecutions() {
    int expectedLogNumber = 1;

    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.getFiles().add(new File().withName("importBib.bib"));
    requestDto.getFiles().add(new File().withName("importMarc.mrc"));

    InitJobExecutionsRsDto response = RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when()
      .post(POST_JOB_EXECUTIONS_PATH)
      .body().as(InitJobExecutionsRsDto.class);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(3));

    JobExecution expectedCommittedChild = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.CHILD)).findFirst().get();

    expectedCommittedChild.setStatus(COMMITTED);
    expectedCommittedChild.setJobProfileName(profileName);

    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(expectedCommittedChild).toString())
      .when()
      .put(PUT_JOB_EXECUTIONS_PATH + expectedCommittedChild.getId())
      .then()
      .statusCode(HttpStatus.SC_OK);

    LogCollectionDto logs = RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(LogCollectionDto.class);

    Assert.assertEquals(logs.getLogDtos().size(), expectedLogNumber);
    Assert.assertEquals(logs.getLogDtos().get(0), expectedCommittedChild.getId());
    Assert.assertEquals(logs.getTotalRecords().intValue(), expectedLogNumber);
  }

  @Test
  public void shouldReturn2LogsIfCreated1ParentMultiple2ChildCommittedJobExecutions() {
    int expectedLogNumber = 2;

    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.getFiles().add(new File().withName("importBib.bib"));
    requestDto.getFiles().add(new File().withName("importMarc.mrc"));

    InitJobExecutionsRsDto response = RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when()
      .post(POST_JOB_EXECUTIONS_PATH)
      .body().as(InitJobExecutionsRsDto.class);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(3));


    List<JobExecution> expectedCommittedChildren = new ArrayList<>();
    for (JobExecution createdJobExecution : createdJobExecutions) {
      if (JobExecution.SubordinationType.CHILD.equals(createdJobExecution.getSubordinationType())) {
        createdJobExecution.setStatus(COMMITTED);
        createdJobExecution.setJobProfileName(profileName);
        expectedCommittedChildren.add(createdJobExecution);
      }
    }

    for (JobExecution child : expectedCommittedChildren) {
      RestAssured.given()
        .spec(spec)
        .body(JsonObject.mapFrom(child).toString())
        .when()
        .put(PUT_JOB_EXECUTIONS_PATH + child.getId())
        .then()
        .statusCode(HttpStatus.SC_OK);
    }

    LogCollectionDto logs = RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(LogCollectionDto.class);

    Assert.assertEquals(logs.getLogDtos().size(), expectedLogNumber);
    Assert.assertEquals(logs.getTotalRecords().intValue(), expectedLogNumber);

    for (JobExecution childExpectedCommittedJoExec : expectedCommittedChildren) {
      LogDto log = logs.getLogDtos().stream()
        .filter(logDto -> childExpectedCommittedJoExec.getId().equals(logDto.getJobExecutionId()))
        .findAny()
        .orElse(null);
      Assert.assertNotNull(log);
    }
  }
}
