package org.folio.rest.impl.metadataProvider;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.TestUtil;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.impl.MetadataProviderImpl;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.LogCollectionDto;
import org.folio.rest.jaxrs.model.LogDto;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static org.folio.rest.jaxrs.model.JobExecution.Status.COMMITTED;
import static org.folio.rest.jaxrs.model.JobExecution.Status.ERROR;
import static org.folio.rest.jaxrs.model.JobExecution.SubordinationType.CHILD;
import static org.folio.rest.jaxrs.model.JobExecution.SubordinationType.PARENT_SINGLE;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;

/**
 * REST tests for MetadataProvider to manager Log entities
 */
@RunWith(VertxUnitRunner.class)
public class MetadataProviderLogAPITest extends AbstractRestTest {

  protected static final String FILES_PATH = "src/test/resources/org/folio/rest/files.sample";
  private static final String GET_LOGS_PATH_LANDING_PAGE_FALSE = "/metadata-provider/logs?landingPage=false";
  private static final String GET_LOGS_PATH_LANDING_PAGE_TRUE = "/metadata-provider/logs?landingPage=true";
  private static final String POST_JOB_EXECUTIONS_PATH = "/change-manager/jobExecutions";
  private static final String PUT_JOB_EXECUTIONS_PATH = "/change-manager/jobExecution/";
  private static final String profileName = "Parse Marc files profile";
  private int landingPageLogsLimit = MetadataProviderImpl.LANDING_PAGE_LOGS_LIMIT;

  @Test
  public void shouldReturnEmptyListOnGetIfNoLogsExist() {
    int expectedLogNumber = 0;
    int expectedTotalRecords = 0;

    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_FALSE)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("logDtos.size()", is(expectedLogNumber))
      .body("totalRecords", is(expectedTotalRecords));
  }

  @Test
  public void shouldReturnEmptyListOnGetIfCreated1ParentMultiple2Child() throws IOException {
    int actualFilesNumber = 2;
    int expectedLogNumber = 0;
    int expectedTotalRecords = 0;

    constructAndPostInitJobExecutionRqDto(actualFilesNumber);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_FALSE)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("logDtos.size()", is(expectedLogNumber))
      .body("totalRecords", is(expectedTotalRecords));
  }

  @Test
  public void shouldReturnEmptyListOnGetIfCreated1ParentSingle() throws IOException {
    int actualFilesNumber = 1;
    int expectedLogNumber = 0;
    int expectedTotalRecords = 0;

    constructAndPostInitJobExecutionRqDto(actualFilesNumber);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_FALSE)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("logDtos.size()", is(expectedLogNumber))
      .body("totalRecords", is(expectedTotalRecords));
  }

  @Test
  public void should1LogOnGetIfCreated1ParentSingleCommitted() throws IOException {
    int actualFilesNumber = 1;
    int expectedLogNumber = 1;
    int expectedTotalRecords = 1;

    List<JobExecution> createdJobExecutions = constructAndPostInitJobExecutionRqDto(actualFilesNumber)
      .as(InitJobExecutionsRsDto.class)
      .getJobExecutions();

    JobExecution expectedCommittedChild = createdJobExecutions.get(0);
    Assert.assertEquals(PARENT_SINGLE, expectedCommittedChild.getSubordinationType());
    expectedCommittedChild.setStatus(COMMITTED);
    expectedCommittedChild.setJobProfileName(profileName);

    putJobExecution(expectedCommittedChild)
      .then()
      .statusCode(HttpStatus.SC_OK);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_FALSE)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("logDtos.size()", is(expectedLogNumber))
      .body("totalRecords", is(expectedTotalRecords));
  }

  @Test
  public void shouldReturn1LogOnGetIfCreated1ParentMultiple1ChildNew1ChildCommitted() throws IOException {
    int actualFilesNumber = 2;
    int expectedLogNumber = 1;
    int expectedTotalRecords = 1;

    List<JobExecution> createdJobExecutions = constructAndPostInitJobExecutionRqDto(actualFilesNumber)
      .as(InitJobExecutionsRsDto.class)
      .getJobExecutions();

    JobExecution expectedCommittedChild = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(CHILD)).findFirst().get();

    expectedCommittedChild.setStatus(COMMITTED);
    expectedCommittedChild.setJobProfileName(profileName);

    putJobExecution(expectedCommittedChild)
      .then()
      .statusCode(HttpStatus.SC_OK);

    LogCollectionDto logs = RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_FALSE)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(LogCollectionDto.class);

    Assert.assertEquals(logs.getLogDtos().size(), expectedLogNumber);
    Assert.assertEquals(logs.getLogDtos().get(0).getJobExecutionId(), expectedCommittedChild.getId());
    Assert.assertEquals(logs.getTotalRecords().intValue(), expectedTotalRecords);
  }

  @Test
  public void shouldReturn2LogsOnGetIfCreated1ParentMultiple2ChildCommitted() throws IOException {
    int actualFilesNumber = 2;
    int expectedLogNumber = 2;
    int expectedTotalRecords = 2;

    List<JobExecution> createdJobExecutions = constructAndPostInitJobExecutionRqDto(actualFilesNumber)
      .as(InitJobExecutionsRsDto.class)
      .getJobExecutions();

    List<JobExecution> expectedCommittedChildren = new ArrayList<>();
    for (JobExecution createdJobExecution : createdJobExecutions) {
      if (CHILD.equals(createdJobExecution.getSubordinationType())) {
        createdJobExecution.setStatus(COMMITTED);
        createdJobExecution.setJobProfileName(profileName);
        expectedCommittedChildren.add(createdJobExecution);
      }
    }

    for (JobExecution child : expectedCommittedChildren) {
      putJobExecution(child)
        .then()
        .statusCode(HttpStatus.SC_OK);
    }

    LogCollectionDto logCollectionDto = RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_FALSE)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(LogCollectionDto.class);

    Assert.assertEquals(logCollectionDto.getLogDtos().size(), expectedLogNumber);
    Assert.assertEquals(logCollectionDto.getTotalRecords().intValue(), expectedTotalRecords);

    for (JobExecution childExpectedCommittedJoExec : expectedCommittedChildren) {
      LogDto log = logCollectionDto.getLogDtos().stream()
        .filter(logDto -> childExpectedCommittedJoExec.getId().equals(logDto.getJobExecutionId()))
        .findAny()
        .orElse(null);
      Assert.assertNotNull(log);
    }
  }

  @Test
  public void shouldReturnLimitedListOnGetIfCreated1ParentMultiple3ChildCommittedWithLimit() throws IOException {
    int actualLimit = 2;
    int expectedLogNumber = 2;
    // We do not expect PARENT entity in total records
    int expectedTotalRecords = 3;
    int actualFilesNumber = 3;

    List<JobExecution> createdJobExecutions = constructAndPostInitJobExecutionRqDto(actualFilesNumber)
      .as(InitJobExecutionsRsDto.class)
      .getJobExecutions();

    List<JobExecution> expectedCommittedChildren = new ArrayList<>();
    for (JobExecution createdJobExecution : createdJobExecutions) {
      if (CHILD.equals(createdJobExecution.getSubordinationType())) {
        createdJobExecution.setStatus(COMMITTED);
        createdJobExecution.setJobProfileName(profileName);
        expectedCommittedChildren.add(createdJobExecution);
      }
    }

    for (JobExecution child : expectedCommittedChildren) {
      putJobExecution(child)
        .then()
        .statusCode(HttpStatus.SC_OK);
    }

    LogCollectionDto logCollectionDto = RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_FALSE + "&limit=" + actualLimit)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(LogCollectionDto.class);

    Assert.assertEquals(logCollectionDto.getLogDtos().size(), expectedLogNumber);
    Assert.assertEquals(logCollectionDto.getTotalRecords().intValue(), expectedTotalRecords);
  }

  @Test
  public void shouldReturnSortedListOnGetIfCreated1ParentMultiple3ChildCommitted() throws IOException {
    int actualFilesNumber = 4;

    List<JobExecution> createdJobExecutions = constructAndPostInitJobExecutionRqDto(actualFilesNumber)
      .as(InitJobExecutionsRsDto.class)
      .getJobExecutions();

    List<JobExecution> expectedCommittedChildren = new ArrayList<>();
    for (int i = 0; i < createdJobExecutions.size(); i++) {
      JobExecution createdJobExecution = createdJobExecutions.get(i);
      if (CHILD.equals(createdJobExecution.getSubordinationType())) {
        createdJobExecution.setStatus(COMMITTED);
        createdJobExecution.setJobProfileName(profileName);
        createdJobExecution.setCompletedDate(new Date(1542714612000L + i));
        expectedCommittedChildren.add(createdJobExecution);
      }
    }

    for (JobExecution child : expectedCommittedChildren) {
      putJobExecution(child)
        .then()
        .statusCode(HttpStatus.SC_OK);
    }

    LogCollectionDto logCollectionDto = RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_FALSE)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(LogCollectionDto.class);

    List<LogDto> logsList = logCollectionDto.getLogDtos();
    Assert.assertEquals(logsList.size(), createdJobExecutions.size() - 1);
    Assert.assertTrue(logsList.get(0).getCompletedDate().after(logsList.get(1).getCompletedDate()));
    Assert.assertTrue(logsList.get(1).getCompletedDate().after(logsList.get(2).getCompletedDate()));
    Assert.assertTrue(logsList.get(2).getCompletedDate().after(logsList.get(3).getCompletedDate()));
  }

  @Test
  public void shouldReturnLimitedListOnGetIfLandingPageIsTrue() throws IOException {
    List<JobExecution> createdJobExecutions = constructAndPostInitJobExecutionRqDto(Integer.MAX_VALUE)
      .as(InitJobExecutionsRsDto.class)
      .getJobExecutions();

    Assert.assertTrue(landingPageLogsLimit < createdJobExecutions.size());

    List<JobExecution> expectedCommittedChildren = new ArrayList<>();
    for (int i = 0; i < createdJobExecutions.size(); i++) {
      JobExecution createdJobExecution = createdJobExecutions.get(i);
      if (CHILD.equals(createdJobExecution.getSubordinationType())) {
        createdJobExecution.setStatus(COMMITTED);
        createdJobExecution.setJobProfileName(profileName);
        expectedCommittedChildren.add(createdJobExecution);
      }
    }

    for (JobExecution child : expectedCommittedChildren) {
      putJobExecution(child)
        .then()
        .statusCode(HttpStatus.SC_OK);
    }

    LogCollectionDto logCollectionDto = RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_TRUE)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(LogCollectionDto.class);

    Assert.assertEquals(logCollectionDto.getLogDtos().size(), landingPageLogsLimit);
    Assert.assertEquals(logCollectionDto.getTotalRecords().intValue(), createdJobExecutions.size() - 1);
  }

  @Test
  public void shouldReturnCommittedAndErrorJobExecutionsAsLogs() throws IOException {
    int actualFilesNumber = 10;

    List<JobExecution> createdJobExecutions = constructAndPostInitJobExecutionRqDto(actualFilesNumber)
      .as(InitJobExecutionsRsDto.class)
      .getJobExecutions();

    List<JobExecution> expectedLogs = new ArrayList<>();
    for (int i = 0; i < createdJobExecutions.size(); i++) {
      JobExecution createdJobExecution = createdJobExecutions.get(i);
      if (CHILD.equals(createdJobExecution.getSubordinationType())) {
        if (i % 2 == 0) {
          createdJobExecution.setStatus(COMMITTED);
        } else {
          createdJobExecution.setStatus(ERROR);
        }
        createdJobExecution.setJobProfileName(profileName);
        createdJobExecution.setCompletedDate(new Date(1542714612000L + i));
        expectedLogs.add(createdJobExecution);
      }
    }

    for (JobExecution child : expectedLogs) {
      putJobExecution(child)
        .then()
        .statusCode(HttpStatus.SC_OK);
    }

    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_FALSE)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(expectedLogs.size()))
      .body("logDtos*.status", everyItem(anyOf(is(LogDto.Status.COMMITTED.name()), is(LogDto.Status.ERROR.name()))));
  }

  private Response constructAndPostInitJobExecutionRqDto(int filesNumber) throws IOException {
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    String jsonFiles = TestUtil.readFileFromPath(FILES_PATH);
    List<File> filesList = new ObjectMapper().readValue(jsonFiles, new TypeReference<List<File>>() {
    });
    List<File> limitedFilesList = filesList.stream().limit(filesNumber).collect(Collectors.toList());
    requestDto.getFiles().addAll(limitedFilesList);
    return RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().post(POST_JOB_EXECUTIONS_PATH);
  }

  private Response putJobExecution(JobExecution jobExecution) {
    return RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobExecution).toString())
      .when()
      .put(PUT_JOB_EXECUTIONS_PATH + jobExecution.getId());
  }
}
