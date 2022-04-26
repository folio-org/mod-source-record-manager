package org.folio.rest.impl.metadataProvider;

import io.restassured.RestAssured;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.dao.JournalRecordDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.JobExecutionDtoCollection;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.JournalRecordCollection;
import org.folio.rest.jaxrs.model.Progress;
import org.folio.rest.jaxrs.model.RunBy;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.JobExecutionsCache;
import org.folio.services.Status;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.folio.rest.jaxrs.model.JobExecution.SubordinationType.CHILD;
import static org.folio.rest.jaxrs.model.JobExecution.SubordinationType.PARENT_MULTIPLE;
import static org.folio.rest.jaxrs.model.JobProfileInfo.DataType.MARC;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.COMPLETED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.ERROR;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.CREATE;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.MODIFY;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.NON_MATCH;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.UPDATE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.EDIFACT;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.INVOICE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * REST tests for MetadataProvider to manager JobExecution entities
 */
@RunWith(VertxUnitRunner.class)
public class MetadataProviderJobExecutionAPITest extends AbstractRestTest {
  private static final String GET_JOB_EXECUTIONS_PATH = "/metadata-provider/jobExecutions";
  private static final String GET_JOB_EXECUTION_LOGS_PATH = "/metadata-provider/logs";
  private static final String GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH = "/metadata-provider/journalRecords";
  private static final String GET_JOB_EXECUTION_SUMMARY_PATH = "/metadata-provider/jobSummary";

  @Spy
  private PostgresClientFactory postgresClientFactory = new PostgresClientFactory(vertx);
  @Spy
  @InjectMocks
  private JournalRecordDaoImpl journalRecordDao;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void shouldReturnEmptyListIfNoJobExecutionsExist() {
    getBeanFromSpringContext(vertx, JobExecutionsCache.class).evictCache();
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
    getBeanFromSpringContext(vertx, JobExecutionsCache.class).evictCache();
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
      .queryParam("statusNot", Status.DISCARDED)
      .get(GET_JOB_EXECUTIONS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions.size()", is(expectedNotDiscardedNumber))
      .body("jobExecutions*.status", not(StatusDto.Status.DISCARDED.name()))
      .body("totalRecords", is(expectedNotDiscardedNumber));
  }

  @Test
  public void shouldNotReturnHiddenInCollection() {
    int numberOfFiles = 5;
    int expectedNotHiddenNumber = 2;
    List<JobExecution> createdJobExecutions = constructAndPostInitJobExecutionRqDto(numberOfFiles).getJobExecutions();
    List<JobExecution> children = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(CHILD)).collect(Collectors.toList());

    for (int i = 0; i < children.size() - expectedNotHiddenNumber; i++) {
      var jobExecution = children.get(i);
      jobExecution.setJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString())
        .withName("Marc jobs profile")
        .withHidden(true));

      RestAssured.given()
        .spec(spec)
        .body(JsonObject.mapFrom(jobExecution).toString())
        .when()
        .put(JOB_EXECUTION_PATH + jobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("id", is(jobExecution.getId()));
    }

    var jobExecutionDtoCollection = RestAssured.given()
      .spec(spec)
      .when()
      .queryParam("statusNot", Status.DISCARDED)
      .get(GET_JOB_EXECUTIONS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(JobExecutionDtoCollection.class);

    assertThat(jobExecutionDtoCollection.getTotalRecords(), is(expectedNotHiddenNumber));
  }

  @Test
  public void shouldReturnSortedJobExecutionsOnGetWhenSortByIsSpecified() {
    List<JobExecution> createdJobExecution = constructAndPostInitJobExecutionRqDto(5).getJobExecutions();

    for (int i = 0; i < createdJobExecution.size(); i++) {
      putJobExecution(createdJobExecution.get(i).withCompletedDate(new Date(1234567892000L + i)));
    }

    // We do not expect to get JobExecution with subordinationType=PARENT_MULTIPLE
    int expectedJobExecutionsNumber = createdJobExecution.size() - 1;
    JobExecutionDtoCollection jobExecutionCollection = RestAssured.given()
      .spec(spec)
      .when()
      .queryParam("uiStatusAny", "INITIALIZATION")
      .queryParam("sortBy", "completed_date,desc")
      .get(GET_JOB_EXECUTIONS_PATH)
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(JobExecutionDtoCollection.class);

    List<JobExecutionDto> jobExecutionDtoList = jobExecutionCollection.getJobExecutions();
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
          .withStatus(JobExecution.Status.COMMITTED)
          .withUiStatus(JobExecution.UiStatus.RUNNING_COMPLETE);
      }
      createdJobExecution.get(i).setCompletedDate(new Date(1234567892000L + i));
      putJobExecution(createdJobExecution.get(i));
    }

    // We do not expect to get JobExecution with subordinationType=PARENT_MULTIPLE
    int expectedJobExecutionsNumber = childJobsToUpdate.size() / 2;
    JobExecutionDtoCollection jobExecutionCollection = RestAssured.given()
      .spec(spec)
      .when()
      .queryParam("uiStatusAny", JobExecution.UiStatus.RUNNING_COMPLETE)
      .queryParam("statusAny", Status.COMMITTED, Status.ERROR)
      .queryParam("sortBy", "completed_date,desc")
      .get(GET_JOB_EXECUTIONS_PATH)
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions*.status", everyItem(is(JobExecution.Status.COMMITTED.value())))
      .body("jobExecutions*.uiStatus", everyItem(is(JobExecution.UiStatus.RUNNING_COMPLETE.value())))
      .extract().response().body().as(JobExecutionDtoCollection.class);

    List<JobExecutionDto> jobExecutionDtoList = jobExecutionCollection.getJobExecutions();
    Assert.assertEquals(expectedJobExecutionsNumber, jobExecutionDtoList.size());
    Assert.assertTrue(jobExecutionDtoList.get(0).getCompletedDate().after(jobExecutionDtoList.get(1).getCompletedDate()));
    Assert.assertTrue(jobExecutionDtoList.get(1).getCompletedDate().after(jobExecutionDtoList.get(2).getCompletedDate()));
    Assert.assertTrue(jobExecutionDtoList.get(2).getCompletedDate().after(jobExecutionDtoList.get(3).getCompletedDate()));
  }

  @Test
  public void shouldReturnSortedJobExecutionsByTotalProgressOnGet() {
    List<JobExecution> createdJobExecution = constructAndPostInitJobExecutionRqDto(4).getJobExecutions();
    List<JobExecution> childJobsToUpdate = createdJobExecution.stream()
      .filter(jobExecution -> jobExecution.getSubordinationType().equals(CHILD))
      .collect(Collectors.toList());

    for (int i = 0; i < childJobsToUpdate.size(); i++) {
      putJobExecution(createdJobExecution.get(i)
        .withProgress(new Progress().withTotal(i * 5)));
    }

    // We do not expect to get JobExecution with subordinationType=PARENT_MULTIPLE
    int expectedJobExecutionsNumber = childJobsToUpdate.size();
    JobExecutionDtoCollection jobExecutionCollection = RestAssured.given()
      .spec(spec)
      .when()
      .queryParam("sortBy", "progress_total,desc")
      .get(GET_JOB_EXECUTIONS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(JobExecutionDtoCollection.class);

    List<JobExecutionDto> jobExecutions = jobExecutionCollection.getJobExecutions();
    Assert.assertEquals(expectedJobExecutionsNumber, jobExecutions.size());
    assertThat(jobExecutions.get(0).getProgress().getTotal(), greaterThan(jobExecutions.get(1).getProgress().getTotal()));
    assertThat(jobExecutions.get(1).getProgress().getTotal(), greaterThan(jobExecutions.get(2).getProgress().getTotal()));
    assertThat(jobExecutions.get(2).getProgress().getTotal(), greaterThan(jobExecutions.get(3).getProgress().getTotal()));
  }

  @Test
  public void shouldReturnSortedCollectionByMultipleFieldsOnGet() {
    List<JobExecution> createdJobExecution = constructAndPostInitJobExecutionRqDto(4).getJobExecutions();
    List<JobExecution> childJobsToUpdate = createdJobExecution.stream()
      .filter(jobExecution -> jobExecution.getSubordinationType().equals(CHILD))
      .collect(Collectors.toList());

    for (int i = 0; i < childJobsToUpdate.size(); i++) {
      putJobExecution(createdJobExecution.get(i)
        .withRunBy(new RunBy()
          .withFirstName("John")
          .withLastName("Doe-" + i)));
    }

    // We do not expect to get JobExecution with subordinationType=PARENT_MULTIPLE
    int expectedJobExecutionsNumber = childJobsToUpdate.size();
    JobExecutionDtoCollection jobExecutionCollection = RestAssured.given()
      .spec(spec)
      .when()
      .queryParam("sortBy", "job_user_first_name,asc")
      .queryParam("sortBy", "job_user_last_name,desc")
      .get(GET_JOB_EXECUTIONS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(JobExecutionDtoCollection.class);

    List<JobExecutionDto> jobExecutions = jobExecutionCollection.getJobExecutions();
    Assert.assertEquals(expectedJobExecutionsNumber, jobExecutions.size());
    assertThat(jobExecutions.get(0).getRunBy().getLastName(), greaterThan(jobExecutions.get(1).getRunBy().getLastName()));
    assertThat(jobExecutions.get(1).getRunBy().getLastName(), greaterThan(jobExecutions.get(2).getRunBy().getLastName()));
    assertThat(jobExecutions.get(2).getRunBy().getLastName(), greaterThan(jobExecutions.get(3).getRunBy().getLastName()));
  }

  @Test
  public void shouldReturnBadRequestWhenInvalidSortableFieldIsSpecified() {
    RestAssured.given()
      .spec(spec)
      .when()
      .queryParam("sortBy", "obviousWrongField,asc")
      .get(GET_JOB_EXECUTIONS_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void shouldReturnBadRequestWhenSortOrderIsInvalid() {
    RestAssured.given()
      .spec(spec)
      .when()
      .queryParam("sortBy", "job_user_first_name,ascending")
      .get(GET_JOB_EXECUTIONS_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void shouldReturnFilteredCollectionByHrIdOrFileNameOnGet() {
    Integer expectedHrid = constructAndPostInitJobExecutionRqDto(5).getJobExecutions().stream()
      .filter(job -> !job.getSubordinationType().equals(PARENT_MULTIPLE))
      .findAny()
      .map(JobExecution::getHrId)
      .get();

    // We do not expect to get JobExecution with subordinationType=PARENT_MULTIPLE
    getBeanFromSpringContext(vertx, JobExecutionsCache.class).evictCache();
    RestAssured.given()
      .spec(spec)
      .when()
      .queryParam("hrid", expectedHrid)
      .queryParam("fileName", "*importBib5*")
      .get(GET_JOB_EXECUTIONS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions.size()", is(2))
      .body("totalRecords", is(2))
      .body("jobExecutions*.hrId", hasItem(is(expectedHrid)))
      .body("jobExecutions*.fileName", hasItem(is("importBib5.bib")));
  }

  @Test
  public void shouldReturnFilteredCollectionByHrIdOnGet() {
    Integer expectedHrid = constructAndPostInitJobExecutionRqDto(5).getJobExecutions().stream()
      .filter(job -> !job.getSubordinationType().equals(PARENT_MULTIPLE))
      .findAny()
      .map(JobExecution::getHrId)
      .get();

    // We do not expect to get JobExecution with subordinationType=PARENT_MULTIPLE
    getBeanFromSpringContext(vertx, JobExecutionsCache.class).evictCache();
    RestAssured.given()
      .spec(spec)
      .when()
      .queryParam("hrid", expectedHrid)
      .get(GET_JOB_EXECUTIONS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions.size()", is(1))
      .body("totalRecords", is(1))
      .body("jobExecutions[0].hrId", is(expectedHrid));
  }

  @Test
  public void shouldReturnFilteredCollectionByFileNameOnGet() {
    constructAndPostInitJobExecutionRqDto(5);
    // We do not expect to get JobExecution with subordinationType=PARENT_MULTIPLE
    getBeanFromSpringContext(vertx, JobExecutionsCache.class).evictCache();
    RestAssured.given()
      .spec(spec)
      .when()
      .queryParam("fileName", "*importBib3.bib")
      .get(GET_JOB_EXECUTIONS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions.size()", is(1))
      .body("totalRecords", is(1))
      .body("jobExecutions[0].fileName", is("importBib3.bib"));
  }

  @Test
  public void shouldNotReturnJobExecutionsWithoutSpecifiedProfileId() {
    String profileId = "d0ebb7b0-2f0f-11eb-adc1-0242ac120002";
    List<JobExecution> createdJobExecution = constructAndPostInitJobExecutionRqDto(4).getJobExecutions();
    List<JobExecution> childJobsToUpdate = createdJobExecution.stream()
      .filter(jobExecution -> jobExecution.getSubordinationType().equals(CHILD))
      .collect(Collectors.toList());

    for (int i = 0; i < childJobsToUpdate.size(); i++) {
      String id = (i % 2 == 0) ? profileId : UUID.randomUUID().toString();
      childJobsToUpdate.get(i).withJobProfileInfo(new JobProfileInfo()
        .withId(id)
        .withName("test")
        .withDataType(MARC));
      putJobExecution(createdJobExecution.get(i));
    }

    // We do not expect to get JobExecution with subordinationType=PARENT_MULTIPLE
    int expectedJobExecutionsNumber = childJobsToUpdate.size() / 2;
    RestAssured.given()
      .spec(spec)
      .when()
      .queryParam("profileIdNotAny", profileId)
      .get(GET_JOB_EXECUTIONS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions.size()", is(expectedJobExecutionsNumber))
      .body("totalRecords", is(expectedJobExecutionsNumber))
      .body("jobExecutions*.jobProfileInfo.id", everyItem(not(is(profileId))));
  }

  @Test
  public void shouldReturnFilteredCollectionByCompletedDateOnGet() {
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    List<JobExecution> childJobsToUpdate = constructAndPostInitJobExecutionRqDto(8).getJobExecutions().stream()
      .filter(jobExecution -> jobExecution.getSubordinationType().equals(CHILD))
      .collect(Collectors.toList());

    Date dateFrom = new Date();
    Date dateTo = Date.from(Instant.now().plus(1, ChronoUnit.DAYS));

    for (int i = 0; i < childJobsToUpdate.size(); i++) {
      if (i % 2 == 0) {
        childJobsToUpdate.get(i).setCompletedDate(new Date());
      } else {
        childJobsToUpdate.get(i).setCompletedDate(Date.from(dateFrom.toInstant().plus(2, ChronoUnit.DAYS)));
      }
      putJobExecution(childJobsToUpdate.get(i));
    }

    // We do not expect to get JobExecution with subordinationType=PARENT_MULTIPLE
    int expectedJobExecutionsNumber = childJobsToUpdate.size() / 2;
    List<Date> completedDates = RestAssured.given()
      .spec(spec)
      .when()
      .queryParam("completedAfter", dateTimeFormatter.format(ZonedDateTime.ofInstant(dateFrom.toInstant(), ZoneOffset.UTC)))
      .queryParam("completedBefore", dateTimeFormatter.format(ZonedDateTime.ofInstant(dateTo.toInstant(), ZoneOffset.UTC)))
      .get(GET_JOB_EXECUTIONS_PATH)
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions.size()", is(expectedJobExecutionsNumber))
      .body("totalRecords", is(expectedJobExecutionsNumber))
      .extract().as(JobExecutionDtoCollection.class)
      .getJobExecutions().stream()
      .map(JobExecutionDto::getCompletedDate)
      .collect(Collectors.toList());

    assertThat(completedDates, everyItem(greaterThanOrEqualTo(dateFrom)));
    assertThat(completedDates, everyItem(lessThanOrEqualTo((dateTo))));

  }

  @Test
  public void shouldReturnFilteredCollectionByProfileIdOnGet() {
    String profileId1 = "d0ebb7b0-2f0f-11eb-adc1-0242ac120002";
    String profileId2 = "91f9b8d6-d80e-4727-9783-73fb53e3c786";
    List<JobExecution> childJobsToUpdate = constructAndPostInitJobExecutionRqDto(4).getJobExecutions().stream()
      .filter(jobExecution -> jobExecution.getSubordinationType().equals(CHILD))
      .collect(Collectors.toList());

    int expectedJobExecutionsNumber = 2;
    for (int i = 0; i < expectedJobExecutionsNumber; i++) {
      String profileId = (i % 2 == 0) ? profileId1 : profileId2;
        childJobsToUpdate.get(i).withJobProfileInfo(new JobProfileInfo()
          .withId(profileId)
          .withName("test")
          .withDataType(MARC));
        putJobExecution(childJobsToUpdate.get(i));
    }

    // We do not expect to get JobExecution with subordinationType=PARENT_MULTIPLE
    RestAssured.given()
      .spec(spec)
      .when()
      .queryParam("profileIdAny", profileId1, profileId2)
      .get(GET_JOB_EXECUTIONS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions.size()", is(expectedJobExecutionsNumber))
      .body("totalRecords", is(expectedJobExecutionsNumber))
      .body("jobExecutions*.jobProfileInfo.id", hasItem(is(profileId1)))
      .body("jobExecutions*.jobProfileInfo.id", hasItem(is(profileId2)));
  }

  @Test
  public void shouldReturnFilteredCollectionByUserIdOnGet() {
    String userId = "d0ebb7b0-2f0f-11eb-adc1-0242ac120002";
    List<JobExecution> childJobsToUpdate = constructAndPostInitJobExecutionRqDto(4).getJobExecutions().stream()
      .filter(jobExecution -> jobExecution.getSubordinationType().equals(CHILD))
      .collect(Collectors.toList());

    for (int i = 0; i < childJobsToUpdate.size(); i++) {
      if (i % 2 == 0) {
        childJobsToUpdate.get(i).withUserId(userId);
        putJobExecution(childJobsToUpdate.get(i));
      }
    }

    // We do not expect to get JobExecution with subordinationType=PARENT_MULTIPLE
    int expectedJobExecutionsNumber = childJobsToUpdate.size() / 2;
    RestAssured.given()
      .spec(spec)
      .when()
      .queryParam("userId", userId)
      .get(GET_JOB_EXECUTIONS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions.size()", is(expectedJobExecutionsNumber))
      .body("totalRecords", is(expectedJobExecutionsNumber))
      .body("jobExecutions*.userId", everyItem(is(userId)));
  }

  @Test
  public void shouldReturnJobExecutionLogWithoutResultsWhenProcessingWasNotStarted() {
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_JOB_EXECUTION_LOGS_PATH + "/" + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutionResultLogs.size", is(0));
  }

  @Test
  public void shouldReturnNotFoundWhenSpecifiedJobExecutionDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_JOB_EXECUTION_LOGS_PATH + "/" + UUID.randomUUID())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnEmptyListWhenProcessingWasNotStarted() {
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    JournalRecordCollection journalRecords = RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(JournalRecordCollection.class);

    assertThat(journalRecords.getTotalRecords(), is(0));
    assertThat(journalRecords.getJournalRecords().size(), is(0));
  }

  @Test
  public void shouldReturnNotFoundWhenJobExecutionDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + UUID.randomUUID())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND)
      .body(Matchers.notNullValue(String.class));
  }

  @Test
  public void shouldReturnBadRequestWhenParameterSortByIsInvalid() {
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + jobExec.getId() + "?sortBy=foo&order=asc")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST)
      .body(Matchers.notNullValue(String.class));
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

  @Test
  public void shouldReturnJournalRecordsSortedBySourceRecordOrder(TestContext testContext) {
    Async async = testContext.async();
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String title = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecutions.get(0).getId(), sourceRecordId, null, null, title, 1, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecutions.get(0).getId(), sourceRecordId, null, null, title, 1, CREATE, INSTANCE, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecutions.get(0).getId(), sourceRecordId, null, null, title, 2, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecutions.get(0).getId(), sourceRecordId, null, null, title, 2, CREATE, INSTANCE, COMPLETED, null))
      .onFailure(testContext::fail);

    future.onComplete(ar -> testContext.verify(v -> {
      JournalRecordCollection journalRecords = RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + jobExec.getId() + "?sortBy=source_record_order&order=desc")
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().response().body().as(JournalRecordCollection.class);

      assertThat(journalRecords.getTotalRecords(), is(4));
      assertThat(journalRecords.getJournalRecords().size(), is(4));
      Assert.assertEquals(journalRecords.getJournalRecords().get(0).getSourceRecordOrder(), journalRecords.getJournalRecords().get(1).getSourceRecordOrder());
      assertThat(journalRecords.getJournalRecords().get(1).getSourceRecordOrder(), greaterThan(journalRecords.getJournalRecords().get(2).getSourceRecordOrder()));
      Assert.assertEquals(journalRecords.getJournalRecords().get(2).getSourceRecordOrder(), journalRecords.getJournalRecords().get(3).getSourceRecordOrder());

      async.complete();
    }));
  }

  @Test
  public void shouldReturnJournalRecordsWithTitleWhenSortedBySourceRecordOrder2(TestContext testContext) {
    Async async = testContext.async();
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);
    String expectedRecordTitle = "The Journal of ecclesiastical history.";
    String sourceRecordId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecutions.get(0).getId(), sourceRecordId, null, null, expectedRecordTitle, 1, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecutions.get(0).getId(), sourceRecordId, null, null, expectedRecordTitle, 1, CREATE, INSTANCE, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecutions.get(0).getId(), sourceRecordId, null, null, expectedRecordTitle, 2, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecutions.get(0).getId(), sourceRecordId, null, null, expectedRecordTitle, 2, CREATE, INSTANCE, COMPLETED, null))
      .onFailure(testContext::fail);

    future.onComplete(ar -> testContext.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + jobExec.getId() + "?sortBy=source_record_order&order=desc")
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("journalRecords.size()", is(4))
        .extract().response().body().as(JournalRecordCollection.class).getJournalRecords()
        .stream()
        .filter(journalRecord -> journalRecord.getEntityType().equals(EntityType.MARC_BIBLIOGRAPHIC))
        .forEach(journalRecord -> assertThat(journalRecord.getTitle(), is(expectedRecordTitle)));
      async.complete();
    }));
  }

// todo:

  @Test
  public void shouldReturnCreatedRecordInstanceHoldingItemSummary(TestContext context) {
    Async async = context.async();
    String jobExecutionId = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0).getId();
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(jobExecutionId, sourceRecordId, null, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(jobExecutionId, sourceRecordId, null, null, recordTitle, 0, CREATE, INSTANCE, COMPLETED, null))
      .compose(v -> createJournalRecord(jobExecutionId, sourceRecordId, null, null, recordTitle, 0, CREATE, HOLDINGS, COMPLETED, null))
      .compose(v -> createJournalRecord(jobExecutionId, sourceRecordId, null, null, recordTitle, 0, CREATE, ITEM, COMPLETED, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_SUMMARY_PATH + "/" + jobExecutionId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(jobExecutionId))
        .body("sourceRecordSummary.totalCreatedEntities", is(1))
        .body("sourceRecordSummary.totalUpdatedEntities", is(0))
        .body("sourceRecordSummary.totalDiscardedEntities", is(0))
        .body("sourceRecordSummary.totalErrors", is(0))

        .body("instanceSummary.totalCreatedEntities", is(1))
        .body("instanceSummary.totalUpdatedEntities", is(0))
        .body("instanceSummary.totalDiscardedEntities", is(0))
        .body("instanceSummary.totalErrors", is(0))

        .body("holdingSummary.totalCreatedEntities", is(1))
        .body("holdingSummary.totalUpdatedEntities", is(0))
        .body("holdingSummary.totalDiscardedEntities", is(0))
        .body("holdingSummary.totalErrors", is(0))

        .body("itemSummary.totalCreatedEntities", is(1))
        .body("itemSummary.totalUpdatedEntities", is(0))
        .body("itemSummary.totalDiscardedEntities", is(0))
        .body("itemSummary.totalErrors", is(0))
        .body("totalErrors", is(0));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnUpdatedSourceRecordSummaryWhenRecordWasUpdated(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, UPDATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_SUMMARY_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("sourceRecordSummary.totalCreatedEntities", is(1))
        .body("sourceRecordSummary.totalUpdatedEntities", is(1))
        .body("sourceRecordSummary.totalDiscardedEntities", is(0))
        .body("sourceRecordSummary.totalErrors", is(0))
        .body("totalErrors", is(0));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnUpdatedSourceRecordSummaryWhenRecordWasModified(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle,0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, MODIFY, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_SUMMARY_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("sourceRecordSummary.totalCreatedEntities", is(1))
        .body("sourceRecordSummary.totalUpdatedEntities", is(1))
        .body("sourceRecordSummary.totalDiscardedEntities", is(0))
        .body("sourceRecordSummary.totalErrors", is(0))
        .body("totalErrors", is(0));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnDiscardedInstanceWhenInstanceDidNotMatch(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null,  0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null,  0, NON_MATCH, INSTANCE, COMPLETED, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_SUMMARY_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("sourceRecordSummary.totalCreatedEntities", is(1))
        .body("sourceRecordSummary.totalUpdatedEntities", is(0))
        .body("sourceRecordSummary.totalDiscardedEntities", is(0))
        .body("sourceRecordSummary.totalErrors", is(0))

        .body("instanceSummary.totalCreatedEntities", is(0))
        .body("instanceSummary.totalUpdatedEntities", is(0))
        .body("instanceSummary.totalDiscardedEntities", is(1))
        .body("instanceSummary.totalErrors", is(0))
        .body("totalErrors", is(0));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnInstanceDiscardedWithErrorsWhenInstanceCreationFailed(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null,  0, CREATE, INSTANCE, ERROR, "error msg"))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_SUMMARY_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("sourceRecordSummary.totalCreatedEntities", is(1))
        .body("sourceRecordSummary.totalUpdatedEntities", is(0))
        .body("sourceRecordSummary.totalDiscardedEntities", is(0))
        .body("sourceRecordSummary.totalErrors", is(0))
        .body("instanceSummary.totalCreatedEntities", is(0))
        .body("instanceSummary.totalUpdatedEntities", is(0))
        .body("instanceSummary.totalDiscardedEntities", is(1))
        .body("instanceSummary.totalErrors", is(1))
        .body("totalErrors", is(1));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnInvoiceSummaryWhenInvoiceWasCreated(TestContext context) {
    Async async = context.async();
    String jobExecutionId = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0).getId();
    String sourceRecordId = UUID.randomUUID().toString();
    String invoiceTitle = "INVOICE";
    String invoiceLineDescription = "Some description";
    String invoiceVendorNo = "0704159";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(jobExecutionId, sourceRecordId, null, null, null,0, CREATE, EDIFACT, COMPLETED, null))
      .compose(v -> createJournalRecord(jobExecutionId, sourceRecordId, null, invoiceVendorNo, invoiceTitle, 0, CREATE, INVOICE, COMPLETED, null))
      .compose(v -> createJournalRecord(jobExecutionId, sourceRecordId, null, invoiceVendorNo + "-1", invoiceLineDescription, 1, CREATE, INVOICE, COMPLETED, null))
      .compose(v -> createJournalRecord(jobExecutionId, sourceRecordId, null, invoiceVendorNo + "-2", invoiceLineDescription, 2, CREATE, INVOICE, COMPLETED, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_SUMMARY_PATH + "/" + jobExecutionId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("invoiceSummary.totalCreatedEntities", is(1))
        .body("invoiceSummary.totalUpdatedEntities", is(0))
        .body("invoiceSummary.totalDiscardedEntities", is(0))
        .body("invoiceSummary.totalErrors", is(0))
        .body("totalErrors", is(0));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnInvoiceSummaryWithErrorsWhenInvoiceCreationFailed(TestContext context) {
    Async async = context.async();
    String jobExecutionId = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0).getId();
    String sourceRecordId = UUID.randomUUID().toString();
    String invoiceTitle = "INVOICE";
    String invoiceLineDescription = "Some description";
    String invoiceVendorNo = "0704159";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(jobExecutionId, sourceRecordId, null, null, null,0, CREATE, EDIFACT, COMPLETED, null))
      .compose(v -> createJournalRecord(jobExecutionId, sourceRecordId, null, invoiceVendorNo, invoiceTitle, 0, CREATE, INVOICE, ERROR, "error msg"))
      .compose(v -> createJournalRecord(jobExecutionId, sourceRecordId, null, invoiceVendorNo + "-1", invoiceLineDescription, 1, CREATE, INVOICE, ERROR, "error msg"))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_SUMMARY_PATH + "/" + jobExecutionId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("sourceRecordSummary.totalCreatedEntities", is(1))
        .body("sourceRecordSummary.totalErrors", is(0))
        .body("invoiceSummary.totalCreatedEntities", is(1))
        .body("invoiceSummary.totalUpdatedEntities", is(0))
        .body("invoiceSummary.totalDiscardedEntities", is(1))
        .body("invoiceSummary.totalErrors", is(1))
        .body("totalErrors", is(1));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnInvoiceSummaryWithErrorsWhenAnyInvoiceLineCreationFailed(TestContext context) {
    Async async = context.async();
    String jobExecutionId = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0).getId();
    String sourceRecordId = UUID.randomUUID().toString();
    String invoiceTitle = "INVOICE";
    String invoiceLineDescription = "Some description";
    String invoiceVendorNo = "0704159";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(jobExecutionId, sourceRecordId, null, null, null,0, CREATE, EDIFACT, COMPLETED, null))
      .compose(v -> createJournalRecord(jobExecutionId, sourceRecordId, null, invoiceVendorNo, invoiceTitle, 0, CREATE, INVOICE, COMPLETED, null))
      .compose(v -> createJournalRecord(jobExecutionId, sourceRecordId, null, invoiceVendorNo + "-1", invoiceLineDescription, 1, CREATE, INVOICE, COMPLETED, null))
      .compose(v -> createJournalRecord(jobExecutionId, sourceRecordId, null, invoiceVendorNo + "-2", invoiceLineDescription, 2, CREATE, INVOICE, ERROR, "error msg"))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_SUMMARY_PATH + "/" + jobExecutionId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("sourceRecordSummary.totalCreatedEntities", is(1))
        .body("sourceRecordSummary.totalErrors", is(0))
        .body("invoiceSummary.totalCreatedEntities", is(1))
        .body("invoiceSummary.totalUpdatedEntities", is(0))
        .body("invoiceSummary.totalDiscardedEntities", is(1))
        .body("invoiceSummary.totalErrors", is(1))
        .body("totalErrors", is(1));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnOneInvoiceErrorWhenAllInvoiceLinesCreationFailed(TestContext context) {
    Async async = context.async();
    String jobExecutionId = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0).getId();
    String sourceRecordId = UUID.randomUUID().toString();
    String invoiceTitle = "INVOICE";
    String invoiceLineDescription = "Some description";
    String invoiceVendorNo = "0704159";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(jobExecutionId, sourceRecordId, null, null, null,0, CREATE, EDIFACT, COMPLETED, null))
      .compose(v -> createJournalRecord(jobExecutionId, sourceRecordId, null, invoiceVendorNo, invoiceTitle, 0, CREATE, INVOICE, COMPLETED, null))
      .compose(v -> createJournalRecord(jobExecutionId, sourceRecordId, null, invoiceVendorNo + "-1", invoiceLineDescription, 1, CREATE, INVOICE, ERROR, "error msg"))
      .compose(v -> createJournalRecord(jobExecutionId, sourceRecordId, null, invoiceVendorNo + "-2", invoiceLineDescription, 2, CREATE, INVOICE, ERROR, "error msg"))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_SUMMARY_PATH + "/" + jobExecutionId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("sourceRecordSummary.totalCreatedEntities", is(1))
        .body("sourceRecordSummary.totalErrors", is(0))
        .body("invoiceSummary.totalCreatedEntities", is(1))
        .body("invoiceSummary.totalUpdatedEntities", is(0))
        .body("invoiceSummary.totalDiscardedEntities", is(1))
        .body("invoiceSummary.totalErrors", is(1))
        .body("totalErrors", is(1));

      async.complete();
    }));
  }

  @Test
  public void shouldNotFoundWhenJobExecutionDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_JOB_EXECUTION_SUMMARY_PATH + "/" + UUID.randomUUID())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  private Future<JournalRecord> createJournalRecord(String jobExecutionId, String sourceId, String entityId, String entityHrid, String title, int recordOrder, JournalRecord.ActionType actionType,
                                                    JournalRecord.EntityType entityType, JournalRecord.ActionStatus actionStatus, String errorMessage) {
    JournalRecord journalRecord = new JournalRecord()
      .withJobExecutionId(jobExecutionId)
      .withSourceId(sourceId)
      .withTitle(title)
      .withSourceRecordOrder(recordOrder)
      .withEntityType(entityType)
      .withActionType(actionType)
      .withActionStatus(actionStatus)
      .withError(errorMessage)
      .withActionDate(new Date())
      .withEntityId(entityId)
      .withEntityHrId(entityHrid);
    return journalRecordDao.save(journalRecord, TENANT_ID).map(journalRecord);
  }
}
