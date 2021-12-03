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
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.JobExecutionDtoCollection;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.Progress;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JournalRecordCollection;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.services.JobExecutionsCache;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.folio.rest.jaxrs.model.JobExecution.SubordinationType.CHILD;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.COMPLETED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.CREATE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;

/**
 * REST tests for MetadataProvider to manager JobExecution entities
 */
@RunWith(VertxUnitRunner.class)
public class MetadataProviderJobExecutionAPITest extends AbstractRestTest {
  private static final String GET_JOB_EXECUTIONS_PATH = "/metadata-provider/jobExecutions";
  private static final String GET_JOB_EXECUTION_LOGS_PATH = "/metadata-provider/logs";
  private static final String GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH = "/metadata-provider/journalRecords";

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
      .queryParam("statusNot", "DISCARDED")
      .get(GET_JOB_EXECUTIONS_PATH)
//      .get(GET_JOB_EXECUTIONS_PATH + "?query=(status=\"\" NOT status=\"DISCARDED\")")
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
    JobExecutionDtoCollection jobExecutionCollection = RestAssured.given()
      .spec(spec)
      .when()
      .queryParam("uiStatusAny", "INITIALIZATION")
      .get(GET_JOB_EXECUTIONS_PATH)
//      .get(GET_JOB_EXECUTIONS_PATH + "?query=(uiStatus==\"INITIALIZATION\") sortBy completedDate/sort.descending")
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(JobExecutionDtoCollection.class);

    List<JobExecutionDto> jobExecutionDtoList = jobExecutionCollection.getJobExecutions();
    Assert.assertEquals(expectedJobExecutionsNumber, jobExecutionDtoList.size());
//    Assert.assertTrue(jobExecutionDtoList.get(0).getCompletedDate().after(jobExecutionDtoList.get(1).getCompletedDate()));
//    Assert.assertTrue(jobExecutionDtoList.get(1).getCompletedDate().after(jobExecutionDtoList.get(2).getCompletedDate()));
//    Assert.assertTrue(jobExecutionDtoList.get(2).getCompletedDate().after(jobExecutionDtoList.get(3).getCompletedDate()));
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
      .queryParam("uiStatusAny", "RUNNING_COMPLETE")
      .queryParam("statusAny", "COMMITTED")
//      .get(GET_JOB_EXECUTIONS_PATH + "?query=(uiStatus==\"RUNNING_COMPLETE\" AND status==\"COMMITTED\") sortBy completedDate/sort.descending")
//      .get(GET_JOB_EXECUTIONS_PATH + "?uiStatusAny=RUNNING_COMPLETE&statusAny==COMMITTED")
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
      .get(GET_JOB_EXECUTIONS_PATH + "?query=cql.allRecords=1 sortBy progress.total/sort.descending/number")
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
      .get(GET_JOB_EXECUTION_LOGS_PATH + "/" + UUID.randomUUID().toString())
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
      .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + UUID.randomUUID().toString())
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
