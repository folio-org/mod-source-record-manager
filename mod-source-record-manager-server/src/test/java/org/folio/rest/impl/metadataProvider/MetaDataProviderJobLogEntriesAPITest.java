package org.folio.rest.impl.metadataProvider;

import io.restassured.RestAssured;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.dao.JournalRecordDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.ActionStatus;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobLogEntryDto;
import org.folio.rest.jaxrs.model.JobLogEntryDtoCollection;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.COMPLETED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.ERROR;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.CREATE;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.MODIFY;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.NON_MATCH;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.UPDATE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@RunWith(VertxUnitRunner.class)
public class MetaDataProviderJobLogEntriesAPITest extends AbstractRestTest {

  private static final String GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH = "/metadata-provider/jobLogEntries";
  @Spy
  Vertx vertx = Vertx.vertx();
  @Spy
  @InjectMocks
  PostgresClientFactory clientFactory;
  @Spy
  @InjectMocks
  private JournalRecordDaoImpl journalRecordDao;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void shouldReturnEmptyListOnGetIfHasNoLogRecordsBySpecifiedJobId() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + UUID.randomUUID().toString())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("entries", empty())
      .body("totalRecords", is(0));
  }

  @Test
  public void shouldReturnMarcBibUpdatedWhenMarcBibWasUpdated(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null,0, UPDATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries", hasSize(1))
        .body("totalRecords", is(1))
        .body("entries[0].jobExecutionId", is(createdJobExecution.getId()))
        .body("entries[0].sourceRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordActionStatus", is(ActionStatus.UPDATED.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnMarcBibUpdatedWhenMarcBibWasModified(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, recordTitle,0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null,0, MODIFY, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries", hasSize(1))
        .body("totalRecords", is(1))
        .body("entries[0].jobExecutionId", is(createdJobExecution.getId()))
        .body("entries[0].sourceRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordActionStatus", is(ActionStatus.UPDATED.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnInstanceDiscardedWhenInstanceWasNotMatched(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";


    Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, recordTitle,0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null,0, NON_MATCH, INSTANCE, COMPLETED, null))
      .onSuccess(v -> async.complete())
      .onFailure(context::fail);

    async.awaitSuccess();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("entries.size()", is(1))
      .body("totalRecords", is(1))
      .body("entries[0].jobExecutionId", is(createdJobExecution.getId()))
      .body("entries[0].sourceRecordId", is(sourceRecordId))
      .body("entries[0].sourceRecordTitle", is(recordTitle))
      .body("entries[0].sourceRecordActionStatus", is(ActionStatus.CREATED.value()))
      .body("entries[0].instanceActionStatus", is(ActionStatus.DISCARDED.value()))
      .body("entries[0].error", emptyOrNullString());
  }

  @Test
  public void shouldReturnInstanceDiscardedWhenInstanceCreationFailed(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, recordTitle,0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null,0, CREATE, INSTANCE, ERROR, "error msg"))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries.size()", is(1))
        .body("totalRecords", is(1))
        .body("entries[0].jobExecutionId", is(createdJobExecution.getId()))
        .body("entries[0].sourceRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].instanceActionStatus", is(ActionStatus.DISCARDED.value()))
        .body("entries[0].error", not(emptyOrNullString()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnHoldingsMultipleWhenMultipleHoldingsWereProcessed(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, recordTitle,0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null,0, CREATE, HOLDINGS, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null,0, UPDATE, HOLDINGS, COMPLETED, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries.size()", is(1))
        .body("totalRecords", is(1))
        .body("entries[0].sourceRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].holdingsActionStatus", is(ActionStatus.MULTIPLE.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnSortedEntriesWhenSortByParameterSpecified(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId1 = UUID.randomUUID().toString();
    String sourceRecordId2 = UUID.randomUUID().toString();
    String sourceRecordId3 = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId1, null,1, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId1, null,1, CREATE, INSTANCE, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId2, null,0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId2, null,0, CREATE, INSTANCE, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId3, null,3, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId3, null,3, CREATE, INSTANCE, COMPLETED, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      List<JobLogEntryDto> jobLogEntries = RestAssured.given()
        .spec(spec)
        .queryParam("sortBy", "source_record_order")
        .queryParam("order", "desc")
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries", hasSize(3))
        .body("totalRecords", is(3))
        .extract().body().as(JobLogEntryDtoCollection.class).getEntries();

      context.assertTrue(jobLogEntries.get(0).getSourceRecordOrder() > jobLogEntries.get(1).getSourceRecordOrder());
      context.assertTrue(jobLogEntries.get(1).getSourceRecordOrder() > jobLogEntries.get(2).getSourceRecordOrder());
      async.complete();
    }));
  }

  @Test
  public void shouldReturnBadRequestOnGetWhenInvalidSortingFieldIsSpecified() {
    RestAssured.given()
      .spec(spec)
      .queryParam("sortBy", "invalid_field")
      .queryParam("order", "asc")
      .when()
      .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + UUID.randomUUID().toString())
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void shouldReturnLimitedCollectionOnGetWithLimitAndOffset(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId1 = UUID.randomUUID().toString();
    String sourceRecordId2 = UUID.randomUUID().toString();
    String sourceRecordId3 = UUID.randomUUID().toString();
    String recordTitle1 = "title1";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId1, recordTitle1,1, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId1, null,1, CREATE, INSTANCE, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId2, "title0",0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId2, null,0, CREATE, INSTANCE, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId3, "title3",3, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId3, null,3, CREATE, INSTANCE, COMPLETED, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .queryParam("sortBy", "source_record_order")
        .queryParam("order", "desc")
        .queryParam("limit", "1")
        .queryParam("offset", "1")
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries.size()", is(1))
        .body("totalRecords", is(3))
        .body("entries[0].jobExecutionId", is(createdJobExecution.getId()))
        .body("entries[0].sourceRecordId", is(sourceRecordId1))
        .body("entries[0].sourceRecordTitle", is(recordTitle1))
        .body("entries[0].sourceRecordOrder", is(1));

      async.complete();
    }));
  }

  private Future<JournalRecord> createJournalRecord(String jobExecutionId, String sourceId, String title, int recordOrder, JournalRecord.ActionType actionType,
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
      .withActionDate(new Date());
    return journalRecordDao.save(journalRecord, TENANT_ID).map(journalRecord);
  }
}
