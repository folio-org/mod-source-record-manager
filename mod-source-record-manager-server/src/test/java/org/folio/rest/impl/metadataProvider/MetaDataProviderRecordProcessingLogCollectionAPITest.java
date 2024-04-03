package org.folio.rest.impl.metadataProvider;

import io.restassured.RestAssured;
import io.restassured.response.ValidatableResponse;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.dao.JournalRecordDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.ActionStatus;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.RecordProcessingLogDto;
import org.folio.rest.jaxrs.model.RecordProcessingLogDtoCollection;
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
import java.util.stream.IntStream;

import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.COMPLETED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.ERROR;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.CREATE;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.MATCH;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.MODIFY;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.NON_MATCH;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.UPDATE;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.PARSE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.AUTHORITY;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.EDIFACT;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.INVOICE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_HOLDINGS;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.PO_LINE;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

@RunWith(VertxUnitRunner.class)
public class MetaDataProviderRecordProcessingLogCollectionAPITest extends AbstractRestTest {

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
    String marcBibEntityId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, marcBibEntityId, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, marcBibEntityId, "in00000000001", null, 0, UPDATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
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
        .body("entries[0].sourceRecordId", is(marcBibEntityId))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordActionStatus", is(ActionStatus.CREATED.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnOneEntryIfWithAllMultipleHoldingsTwoErrorsDuringMultipleCreation(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = "i001";

    String[] holdingsId = generateRandomUUIDs(3);
    String[] holdingsHrid = {"h001", "h002", "h003"};

    String[] permanentLocation = {UUID.randomUUID().toString()};

    String errorMsg1 = "test error1";
    String errorMsg2 = "test error2";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceId, instanceHrid, null, 0, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, holdingsId[0], holdingsHrid[0], null, 0, CREATE, HOLDINGS, COMPLETED, null, null, instanceId, null, permanentLocation[0]))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, holdingsId[1], null, null, 0, CREATE, HOLDINGS, ERROR, errorMsg1, null, null, null, null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, holdingsId[2], null, null, 0, CREATE, HOLDINGS, ERROR, errorMsg2, null, null, null, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .log().all()
        .body("entries", hasSize(1))
        .body("totalRecords", is(1))
        .body("entries[0].jobExecutionId", is(createdJobExecution.getId()))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordActionStatus", is(ActionStatus.CREATED.value()))
        .body("entries[0].relatedHoldingsInfo[0].actionStatus", is(ActionStatus.CREATED.value()))
        .body("entries[0].relatedHoldingsInfo[0].id", is(holdingsId[0]))
        .body("entries[0].relatedHoldingsInfo[1].actionStatus", is(ActionStatus.DISCARDED.value()))
        .body("entries[0].relatedHoldingsInfo[1].error", oneOf(errorMsg1, errorMsg2))
        .body("entries[0].relatedHoldingsInfo[2].actionStatus", is(ActionStatus.DISCARDED.value()))
        .body("entries[0].relatedHoldingsInfo[2].error", oneOf(errorMsg1, errorMsg2));
      async.complete();
    }));
  }

  @Test
  public void shouldReturnMarcBibUpdatedWhenMarcBibWasModified(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";
    String marcBibEntityId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, marcBibEntityId, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, marcBibEntityId, null, null, 0, MODIFY, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
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
        .body("entries[0].sourceRecordId", is(marcBibEntityId))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordActionStatus", is(ActionStatus.CREATED.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnMarcBibCreatedWhenMarcBibWasCreatedInNonMatchSection(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";
    String marcBibEntityId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, NON_MATCH, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, marcBibEntityId, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
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
        .body("entries[0].sourceRecordId", is(marcBibEntityId))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordActionStatus", is(ActionStatus.CREATED.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnDiscardedForMarcBibAndInstanceIfMarcBibMatchedAndNoOtherAction(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, MATCH, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, MATCH, INSTANCE, COMPLETED, null, null))
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
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordActionStatus", is(ActionStatus.DISCARDED.value()))
        .body("entries[0].relatedInstanceInfo.actionStatus", is(ActionStatus.DISCARDED.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnDiscardedForMarcBibAndInstanceIfMarcBibNotMatched(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, NON_MATCH, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, NON_MATCH, INSTANCE, COMPLETED, null, null))
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
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordActionStatus", is(ActionStatus.DISCARDED.value()))
        .body("entries[0].relatedInstanceInfo.actionStatus", is(ActionStatus.DISCARDED.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnDiscardedForHoldingsIfHoldingsMatchedAndNoOtherAction(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, MATCH, HOLDINGS, COMPLETED, null, null))
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
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordActionStatus", is(emptyOrNullString()))
        .body("entries[0].relatedHoldingsInfo.size()", is(1))
        .body("entries[0].relatedHoldingsInfo[0].actionStatus", is(ActionStatus.DISCARDED.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnDiscardedForItemIfItemMatchedAndNoOtherAction(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, MATCH, ITEM, COMPLETED, null, null))
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
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordActionStatus", is(emptyOrNullString()))
        .body("entries[0].relatedItemInfo.size()", is(1))
        .body("entries[0].relatedItemInfo[0].actionStatus", is(ActionStatus.DISCARDED.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnDiscardedForAuthorityIfAuthorityMatchedAndNoOtherAction(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, MATCH, AUTHORITY, COMPLETED, null, null))
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
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordActionStatus", is(emptyOrNullString()))
        .body("entries[0].relatedAuthorityInfo.actionStatus", is(ActionStatus.DISCARDED.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnInstanceDiscardedWhenInstanceWasNotMatched(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";


    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, "in00000000001", null, 0, NON_MATCH, INSTANCE, COMPLETED, null, null))
      .onSuccess(v -> async.complete())
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
        .body("entries[0].sourceRecordActionStatus", is(ActionStatus.CREATED.value()))
        .body("entries[0].instanceActionStatus", is(ActionStatus.DISCARDED.value()))
        .body("entries[0].error", emptyOrNullString());

      async.complete();
    }));
  }

  @Test
  public void shouldReturnInstanceDiscardedWhenInstanceCreationFailed(TestContext context) {
    //test
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";
    String marcBibEntityId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, marcBibEntityId, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, CREATE, INSTANCE, ERROR, "error msg", null))
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
        .body("entries[0].sourceRecordId", is(marcBibEntityId))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].relatedInstanceInfo.actionStatus", is(ActionStatus.DISCARDED.value()))
        .body("entries[0].relatedInstanceInfo.error", not(emptyOrNullString()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnInstanceCreatedWhenMarcModify(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";
    String marcBibEntityId = UUID.randomUUID().toString();
    String instanceEntityId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, marcBibEntityId, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, marcBibEntityId, null, recordTitle, 0, MODIFY, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceEntityId, null, null, 0, UPDATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceEntityId, "in00000000001", null, 0, CREATE, INSTANCE, COMPLETED, null, null))
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
        .body("entries[0].sourceRecordId", is(marcBibEntityId))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].relatedInstanceInfo.actionStatus", is(ActionStatus.CREATED.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnPoLineCreatedWhenMarcCreate(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";
    String marcBibEntityId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, marcBibEntityId, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, "poLineEntityID", null, null, 0, CREATE, PO_LINE, COMPLETED, null, null))
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
        .body("entries[0].sourceRecordId", is(marcBibEntityId))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].relatedPoLineInfo.actionStatus", is(ActionStatus.CREATED.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnAuthorityDiscardedWhenErrorOnMatch(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";
    String authorityEntityId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, authorityEntityId, null, recordTitle, 0, MATCH, MARC_AUTHORITY, ERROR, "errorMsg", null))
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
        .body("entries[0].sourceRecordId", is(authorityEntityId))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordType", is(MARC_AUTHORITY.value()))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].relatedAuthorityInfo.error", is(notNullValue()))
        .body("entries[0].sourceRecordActionStatus", is(ActionStatus.DISCARDED.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnInstanceIdWhenHoldingsCreatedRecordProcessingLogDTOCollection(TestContext context) {
    Async async = context.async();
    JobExecution instanceCreationJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    JobExecution holdingsCreationJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);

    String instanceCreationSourceRecordId = UUID.randomUUID().toString();
    String holdingsCreationSourceRecordId = UUID.randomUUID().toString();

    String recordTitle = "test title";

    JournalRecord holdingsCreatedJournalRecord = new JournalRecord()
      .withJobExecutionId(holdingsCreationJobExecution.getId())
      .withSourceId(holdingsCreationSourceRecordId)
      .withTitle(null)
      .withSourceRecordOrder(0)
      .withEntityType(HOLDINGS)
      .withActionType(CREATE)
      .withActionStatus(COMPLETED)
      .withError(null)
      .withActionDate(new Date())
      .withEntityId("holdingsEntityID")
      .withEntityHrId("ho00000000001")
      .withInstanceId("instanceEntityID");

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(instanceCreationJobExecution.getId(), instanceCreationSourceRecordId, "instanceMarcEntityID", null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(instanceCreationJobExecution.getId(), instanceCreationSourceRecordId, "instanceEntityID", "in00000000001", null, 0, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> journalRecordDao.save(holdingsCreatedJournalRecord, TENANT_ID).map(holdingsCreatedJournalRecord))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + holdingsCreationJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries.size()", is(1))
        .body("totalRecords", is(1))
        .body("entries[0].jobExecutionId", is(holdingsCreationJobExecution.getId()))
        .body("entries[0].incomingRecordId", is(holdingsCreationSourceRecordId))
        .body("entries[0].sourceRecordOrder", is("0"))
        .body("entries[0].error", emptyOrNullString())
        .body("entries[0].relatedInstanceInfo.idList[0]", is("instanceEntityID"))
        .body("entries[0].relatedInstanceInfo.error", emptyOrNullString())
        .body("entries[0].relatedHoldingsInfo[0].id", is("holdingsEntityID"))
        .body("entries[0].relatedHoldingsInfo[0].hrid", is("ho00000000001"))
        .body("entries[0].relatedHoldingsInfo[0].error", emptyOrNullString());
      async.complete();
    }));
  }

  @Test
  public void shouldReturnPoLineWithOrderIdWhenMarcCreateRecordProcessingLogDTOCollection(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";
    String orderId = UUID.randomUUID().toString();
    String marcBibEntityId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, marcBibEntityId, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, "poLineEntityID", null, null, 0, CREATE, PO_LINE, COMPLETED, "Test error", orderId))
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
        .body("entries[0].sourceRecordId", is(marcBibEntityId))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordActionStatus", is(ActionStatus.CREATED.value()))
        .body("entries[0].relatedPoLineInfo", notNullValue())
        .body("entries[0].relatedPoLineInfo.orderId", is(orderId))
        .body("entries[0].relatedPoLineInfo.error", is("Test error"));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnOneInstanceIdWhenMarcBibUpdatedAndInstanceUpdatedRecordProcessingLogDTOCollection(TestContext context) {
    Async async = context.async();
    JobExecution marcBibAndInstanceUpdateJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);

    String marcBibAndInstanceUpdateSourceRecordId = UUID.randomUUID().toString();

    String recordTitle = "test title";
    String marcBibEntityId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(marcBibAndInstanceUpdateJobExecution.getId(), marcBibAndInstanceUpdateSourceRecordId, "instanceEntityID", "in00000000001", null, 0, UPDATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(marcBibAndInstanceUpdateJobExecution.getId(), marcBibAndInstanceUpdateSourceRecordId, "instanceEntityID", "in00000000001", null, 0, UPDATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(marcBibAndInstanceUpdateJobExecution.getId(), marcBibAndInstanceUpdateSourceRecordId, marcBibEntityId, null, recordTitle, 0, MODIFY, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + marcBibAndInstanceUpdateJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries.size()", is(1))
        .body("totalRecords", is(1))
        .body("entries[0].jobExecutionId", is(marcBibAndInstanceUpdateJobExecution.getId()))
        .body("entries[0].sourceRecordId", is(marcBibEntityId))
        .body("entries[0].incomingRecordId", is(marcBibAndInstanceUpdateSourceRecordId))
        .body("entries[0].error", emptyOrNullString())
        .body("entries[0].relatedInstanceInfo.idList.size()", is(1))
        .body("entries[0].relatedInstanceInfo.hridList.size()", is(1))
        .body("entries[0].relatedInstanceInfo.error", emptyOrNullString());

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
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, CREATE, HOLDINGS, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, UPDATE, HOLDINGS, COMPLETED, null, null))
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
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].relatedHoldingsInfo[0].actionStatus", is(ActionStatus.CREATED.value()));
      async.complete();
    }));
  }

  @Test
  public void shouldReturnHoldingsTitleWithHoldingsHrid(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, CREATE, MARC_HOLDINGS, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, "ho00000000001", null, 0, CREATE, HOLDINGS, COMPLETED, null, null))
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
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is("Holdings ho00000000001"))
        .body("entries[0].relatedHoldingsInfo[0].hrid", is("ho00000000001"))
        .body("entries[0].sourceRecordType", is(MARC_HOLDINGS.value()));

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
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId1, null, null, null, 1, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId1, null, "in00000000002", null, 1, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId2, null, null, null, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId2, null, "in00000000001", null, 0, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId3, null, null, null, 3, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId3, null, "in00000000003", null, 3, CREATE, INSTANCE, COMPLETED, null, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      List<RecordProcessingLogDto> recordProcessingLogDtos = RestAssured.given()
        .spec(spec)
        .queryParam("sortBy", "source_record_order")
        .queryParam("order", "desc")
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries", hasSize(3))
        .body("totalRecords", is(3))
        .extract().body().as(RecordProcessingLogDtoCollection.class).getEntries();

      context.assertTrue(Integer.parseInt(recordProcessingLogDtos.get(0).getSourceRecordOrder()) > Integer.parseInt(recordProcessingLogDtos.get(1).getSourceRecordOrder()));
      context.assertTrue(Integer.parseInt(recordProcessingLogDtos.get(1).getSourceRecordOrder()) > Integer.parseInt(recordProcessingLogDtos.get(2).getSourceRecordOrder()));
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
      .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + UUID.randomUUID())
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
    String marcBibEntityId1 = UUID.randomUUID().toString();
    String marcBibEntityId2 = UUID.randomUUID().toString();
    String marcBibEntityId3 = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId1, marcBibEntityId1, null, recordTitle1, 1, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId1, null, "in00000000001", null, 1, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId2, marcBibEntityId2, null, "title0", 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId2, null, null, null, 0, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId3, marcBibEntityId3, null, "title3", 3, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId3, null, null, null, 3, CREATE, INSTANCE, COMPLETED, null, null))
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
        .body("entries[0].sourceRecordId", is(marcBibEntityId1))
        .body("entries[0].incomingRecordId", is(sourceRecordId1))
        .body("entries[0].sourceRecordTitle", is(recordTitle1))
        .body("entries[0].sourceRecordOrder", is("1"))
        .body("entries[0].relatedHoldingsInfo.hrid", is(empty()))
        .body("entries[0].sourceRecordType", is(MARC_BIBLIOGRAPHIC.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnAuthorityCreated(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String entityId = UUID.randomUUID().toString();

    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, entityId, null, recordTitle, 0, CREATE, MARC_AUTHORITY, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, CREATE, AUTHORITY, COMPLETED, null, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      ValidatableResponse r = RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries.size()", is(1))
        .body("totalRecords", is(1))
        .body("entries[0].sourceRecordId", is(entityId))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordActionStatus", is(ActionStatus.CREATED.value()))
        .body("entries[0].relatedAuthorityInfo.actionStatus", is(ActionStatus.CREATED.value()));
      async.complete();
    }));
  }

  @Test
  public void shouldReturnMarcBibUpdatedByJobAndRecordIdsRecordProcessingLogDTOCollection(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";
    String marcBibEntityId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, marcBibEntityId, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, marcBibEntityId, null, null, 0, UPDATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
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
        .body("entries[0].sourceRecordId", is(marcBibEntityId))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordOrder", is("0"))
        .body("entries[0].sourceRecordActionStatus", is(ActionStatus.CREATED.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnEmptyMarcBibErrorAndInstanceDiscardedWhenInstanceCreationFailedRecordProcessingLogDTOCollection(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String entityId = UUID.randomUUID().toString();
    String entityHrid = "001";
    String marcBibEntityId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, marcBibEntityId, entityHrid, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, entityId, entityHrid, null, 0, CREATE, INSTANCE, ERROR, "error msg", null))
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
        .body("entries[0].sourceRecordId", is(marcBibEntityId))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordOrder", is("0"))
        .body("entries[0].error", emptyOrNullString())
        .body("entries[0].relatedInstanceInfo.idList[0]", is(entityId))
        .body("entries[0].relatedInstanceInfo.hridList[0]", is(entityHrid))
        .body("entries[0].relatedInstanceInfo.error", is("error msg"));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnNotEmptyMarcBibErrorWhenMarcBibFailedRecordProcessingLogDTOCollection(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";
    String marcBibEntityId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, marcBibEntityId, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, ERROR, "MarcBib error msg", null))
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
        .body("entries[0].sourceRecordId", is(marcBibEntityId))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordOrder", is("0"))
        .body("entries[0].error", is("MarcBib error msg"));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnMarcBibAndAllEntitiesWithoutErrorsRecordProcessingLogDTOCollection(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = "i001";

    String holdingsId = UUID.randomUUID().toString();
    String holdingsHrid = "h001";

    String itemId = UUID.randomUUID().toString();
    String itemHrid = "it001";

    String poLineId = UUID.randomUUID().toString();
    String poLineHrid = "po001";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceId, instanceHrid, null, 0, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, holdingsId, holdingsHrid, null, 0, CREATE, HOLDINGS, COMPLETED, null, null, instanceId, null, null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, itemId, itemHrid, null, 0, CREATE, ITEM, COMPLETED, null, null, instanceId, holdingsId, null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, poLineId, poLineHrid, null, 0, CREATE, PO_LINE, COMPLETED, null, null, instanceId, null, null))
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
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordOrder", is("0"))
        .body("entries[0].error", emptyOrNullString())
        .body("entries[0].relatedInstanceInfo.idList[0]", is(instanceId))
        .body("entries[0].relatedInstanceInfo.hridList[0]", is(instanceHrid))
        .body("entries[0].relatedInstanceInfo.error", emptyOrNullString())
        .body("entries[0].relatedHoldingsInfo[0].id", is(holdingsId))
        .body("entries[0].relatedHoldingsInfo[0].hrid", is(holdingsHrid))
        .body("entries[0].relatedHoldingsInfo[0].error", emptyOrNullString())
        .body("entries[0].relatedItemInfo[0].id", is(itemId))
        .body("entries[0].relatedItemInfo[0].hrid", is(itemHrid))
        .body("entries[0].relatedItemInfo[0].error", emptyOrNullString())
        .body("entries[0].relatedPoLineInfo.idList[0]", is(poLineId))
        .body("entries[0].relatedPoLineInfo.hridList[0]", is(poLineHrid))
        .body("entries[0].relatedPoLineInfo.error", emptyOrNullString())
        .body("entries[0].relatedInvoiceInfo.idList", empty())
        .body("entries[0].relatedInvoiceInfo.hridList", empty())
        .body("entries[0].relatedInvoiceInfo.error", emptyOrNullString());

      async.complete();
    }));
  }

  @Test
  public void shouldReturnDiscardedForHoldingsIfNoHoldingsCreatedRecordProcessingLogDTOCollection(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = "i001";

    String testError = "testError";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceId, instanceHrid, null, 0, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, HOLDINGS, ERROR, testError, null, null, null, null));

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
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordOrder", is("0"))
        .body("entries[0].error", emptyOrNullString())
        .body("entries[0].relatedInstanceInfo.idList[0]", is(instanceId))
        .body("entries[0].relatedInstanceInfo.hridList[0]", is(instanceHrid))
        .body("entries[0].relatedInstanceInfo.error", emptyOrNullString())
        .body("entries[0].relatedHoldingsInfo[0].id", emptyOrNullString())
        .body("entries[0].relatedHoldingsInfo[0].hrid", emptyOrNullString())
        .body("entries[0].relatedHoldingsInfo[0].error", is(testError));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnDiscardedForItemsIfNoItemsCreatedRecordProcessingLogDTOCollection(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = "i001";

    String holdingsId = UUID.randomUUID().toString();
    String holdingsHrid = "h001";

    String testError = "testError";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceId, instanceHrid, null, 0, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, holdingsId, holdingsHrid, recordTitle, 0, CREATE, HOLDINGS, COMPLETED, null, null, instanceId, null, null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, ITEM, ERROR, testError, null, null, null, null));

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
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordOrder", is("0"))
        .body("entries[0].error", emptyOrNullString())
        .body("entries[0].relatedInstanceInfo.idList[0]", is(instanceId))
        .body("entries[0].relatedInstanceInfo.hridList[0]", is(instanceHrid))
        .body("entries[0].relatedInstanceInfo.error", emptyOrNullString())
        .body("entries[0].relatedHoldingsInfo[0].id", is(holdingsId))
        .body("entries[0].relatedHoldingsInfo[0].hrid", is(holdingsHrid))
        .body("entries[0].relatedHoldingsInfo[0].error", emptyOrNullString())
        .body("entries[0].relatedItemInfo[0].id", emptyOrNullString())
        .body("entries[0].relatedItemInfo[0].hrid", emptyOrNullString())
        .body("entries[0].relatedItemInfo[0].error", is(testError));
      async.complete();
    }));
  }

  @Test
  public void shouldReturnDataForInvoiceLinesRecordProcessingLogDTOCollection(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String invoiceId = "aa67ab97-ec97-4145-beb8-0572dd60dd87";
    String invoiceHrid = "228D126";
    String invoiceVendorNumber = "0704159";
    String invoiceLineId1 = UUID.randomUUID().toString();
    String invoiceLineId2 = UUID.randomUUID().toString();
    String invoiceLineDescription = "Some description";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, CREATE, EDIFACT, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, invoiceId, invoiceHrid, "INVOICE", 0, CREATE, INVOICE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, invoiceLineId1, invoiceVendorNumber + "-1", invoiceLineDescription + "1", 1, CREATE, INVOICE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, invoiceLineId2, invoiceVendorNumber + "-2", invoiceLineDescription + "2", 2, CREATE, INVOICE, COMPLETED, null, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries.size()", is(2))
        .body("totalRecords", is(2))
        .body("entries[0].jobExecutionId", is(createdJobExecution.getId()))
        .body("entries[0].sourceRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(invoiceLineDescription + "1"))
        .body("entries[0].sourceRecordOrder", is("0704159-1"))
        .body("entries[0].error", emptyOrNullString())
        .body("entries[0].relatedInstanceInfo.idList.size", empty())
        .body("entries[0].relatedInstanceInfo.hridList.size", empty())
        .body("entries[0].relatedInstanceInfo.error", nullValue())
        .body("entries[0].relatedHoldingsInfo.size", empty())
        .body("entries[0].relatedItemInfo.size", empty())
        .body("entries[0].relatedPoLineInfo.idList.size", empty())
        .body("entries[0].relatedPoLineInfo.hridList.size", empty())
        .body("entries[0].relatedPoLineInfo.error", emptyOrNullString())
        .body("entries[0].relatedInvoiceInfo.idList[0]", is(invoiceId))
        .body("entries[0].relatedInvoiceInfo.hridList[0]", is(invoiceHrid))
        .body("entries[0].relatedInvoiceInfo.error", emptyOrNullString())
        .body("entries[0].relatedInvoiceLineInfo.id", is(invoiceLineId1))
        .body("entries[0].relatedInvoiceLineInfo.fullInvoiceLineNumber", is(invoiceVendorNumber + "-1"))
        .body("entries[0].relatedInvoiceLineInfo.error", emptyOrNullString())
        .body("entries[0].invoiceLineJournalRecordId", notNullValue())

        .body("entries[1].jobExecutionId", is(createdJobExecution.getId()))
        .body("entries[1].sourceRecordId", is(sourceRecordId))
        .body("entries[1].sourceRecordTitle", is(invoiceLineDescription + "2"))
        .body("entries[1].sourceRecordOrder", is("0704159-2"))
        .body("entries[1].error", emptyOrNullString())
        .body("entries[1].relatedInstanceInfo.idList.size", empty())
        .body("entries[1].relatedInstanceInfo.hridList.size", empty())
        .body("entries[1].relatedInstanceInfo.error", nullValue())
        .body("entries[1].relatedHoldingsInfo.size", empty())
        .body("entries[1].relatedItemInfo.size", empty())
        .body("entries[1].relatedPoLineInfo.idList.size", empty())
        .body("entries[1].relatedPoLineInfo.hridList.size", empty())
        .body("entries[1].relatedPoLineInfo.error", emptyOrNullString())
        .body("entries[1].relatedInvoiceInfo.idList[0]", is(invoiceId))
        .body("entries[1].relatedInvoiceInfo.hridList[0]", is(invoiceHrid))
        .body("entries[1].relatedInvoiceInfo.error", emptyOrNullString())
        .body("entries[1].relatedInvoiceLineInfo.id", is(invoiceLineId2))
        .body("entries[1].relatedInvoiceLineInfo.fullInvoiceLineNumber", is(invoiceVendorNumber + "-2"))
        .body("entries[1].relatedInvoiceLineInfo.error", emptyOrNullString())
        .body("entries[1].invoiceLineJournalRecordId", notNullValue());
      async.complete();
    }));
  }

  @Test
  public void shouldReturnInvoiceLineInfoWithErrorRecordProcessingLogDTOCollection(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String invoiceId = UUID.randomUUID().toString();
    String invoiceHrid = "228D126";
    String invoiceVendorNumber = "0704159";
    String invoiceLineId1 = UUID.randomUUID().toString();
    String invoiceLineDescription = "Some description";
    String errorMsg = "error-msg";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, CREATE, EDIFACT, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, invoiceId, invoiceHrid, "INVOICE", 0, CREATE, INVOICE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, invoiceLineId1, invoiceVendorNumber + "-1", invoiceLineDescription + "1", 1, CREATE, INVOICE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, invoiceVendorNumber + "-2", invoiceLineDescription + "2", 2, CREATE, INVOICE, ERROR, errorMsg, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries.size()", is(2))
        .body("totalRecords", is(2))
        .body("entries[0].jobExecutionId", is(createdJobExecution.getId()))
        .body("entries[0].sourceRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(invoiceLineDescription + "1"))
        .body("entries[0].sourceRecordOrder", is(invoiceVendorNumber + "-1"))
        .body("entries[0].error", emptyOrNullString())
        .body("entries[0].relatedInvoiceInfo.idList[0]", is(invoiceId))
        .body("entries[0].relatedInvoiceInfo.hridList[0]", is(invoiceHrid))
        .body("entries[0].relatedInvoiceInfo.error", emptyOrNullString())
        .body("entries[0].relatedInvoiceLineInfo.id", is(invoiceLineId1))
        .body("entries[0].relatedInvoiceLineInfo.fullInvoiceLineNumber", is(invoiceVendorNumber + "-1"))
        .body("entries[0].relatedInvoiceLineInfo.error", emptyOrNullString())
        .body("entries[0].invoiceLineJournalRecordId", notNullValue())

        .body("entries[1].jobExecutionId", is(createdJobExecution.getId()))
        .body("entries[1].sourceRecordId", is(sourceRecordId))
        .body("entries[1].sourceRecordTitle", is(invoiceLineDescription + "2"))
        .body("entries[1].sourceRecordOrder", is(invoiceVendorNumber + "-2"))
        .body("entries[1].error", emptyOrNullString())
        .body("entries[1].relatedInvoiceInfo.idList[0]", is(invoiceId))
        .body("entries[1].relatedInvoiceInfo.hridList[0]", is(invoiceHrid))
        .body("entries[1].relatedInvoiceInfo.error", emptyOrNullString())
        .body("entries[1].relatedInvoiceLineInfo.id", emptyOrNullString())
        .body("entries[1].relatedInvoiceLineInfo.fullInvoiceLineNumber", is(invoiceVendorNumber + "-2"))
        .body("entries[1].relatedInvoiceLineInfo.error", is(errorMsg))
        .body("entries[1].invoiceLineJournalRecordId", notNullValue());
      async.complete();
    }));
  }

  @Test
  public void shouldReturnNotEmptyListWithInvoicesLines(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();

    String invoiceLineDescription = "Some description";
    String invoiceLineId = "0704159";

    CompositeFuture future = GenericCompositeFuture.all(List.of(
        createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, "228D126", "INVOICE", 0, CREATE, INVOICE, COMPLETED, null, null).map(JournalRecord::getId),
        createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, invoiceLineId + "-1", invoiceLineDescription + "1", 1, CREATE, INVOICE, COMPLETED, null, null).map(JournalRecord::getId),
        createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, invoiceLineId + "-2", invoiceLineDescription + "2", 2, CREATE, INVOICE, COMPLETED, null, null).map(JournalRecord::getId),
        createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, invoiceLineId + "-3", invoiceLineDescription + "3", 3, CREATE, INVOICE, COMPLETED, null, null).map(JournalRecord::getId)))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries.size()", is(3))
        .body("totalRecords", is(3))
        .body("entries*.jobExecutionId", everyItem(is(createdJobExecution.getId())))
        .body("entries*.sourceRecordId", everyItem(is(sourceRecordId)))
        .body("entries[0].sourceRecordTitle", is(invoiceLineDescription + "1"))
        .body("entries[1].sourceRecordTitle", is(invoiceLineDescription + "2"))
        .body("entries[2].sourceRecordTitle", is(invoiceLineDescription + "3"))
        .body("entries[0].sourceRecordOrder", is(invoiceLineId + "-1"))
        .body("entries[1].sourceRecordOrder", is(invoiceLineId + "-2"))
        .body("entries[2].sourceRecordOrder", is(invoiceLineId + "-3"))
        // skip result at 0 index, since it is invoice related journal record id
        .body("entries[0].invoiceLineJournalRecordId", is(future.resultAt(1).toString()))
        .body("entries[1].invoiceLineJournalRecordId", is(future.resultAt(2).toString()))
        .body("entries[2].invoiceLineJournalRecordId", is(future.resultAt(3).toString()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnNotEmptyListWithInvoicesLinesThatContainsError(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();

    String invoiceLineDescription = "Some description";
    String invoiceLineId = "0704159";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, "228D126", "INVOICE", 0, CREATE, INVOICE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, invoiceLineId + "-1", invoiceLineDescription + "1", 1, CREATE, INVOICE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, invoiceLineId + "-2", invoiceLineDescription + "2", 2, CREATE, INVOICE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, invoiceLineId + "-3", invoiceLineDescription + "3", 3, CREATE, INVOICE, ERROR, "Exception", null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      List<RecordProcessingLogDto> recordProcessingLogDtos = RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries.size()", is(3))
        .body("totalRecords", is(3))
        .body("entries[0].jobExecutionId", is(createdJobExecution.getId()))
        .body("entries[0].sourceRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(invoiceLineDescription + "1"))
        .body("entries[0].sourceRecordOrder", is(invoiceLineId + "-1"))
        .body("entries[2].sourceRecordTitle", is(invoiceLineDescription + "3"))
        .body("entries[2].sourceRecordOrder", is(invoiceLineId + "-3"))
        .extract().body().as(RecordProcessingLogDtoCollection.class).getEntries();

      Assert.assertEquals("Exception", recordProcessingLogDtos.get(2).getRelatedInvoiceLineInfo().getError());
      Assert.assertEquals(ActionStatus.DISCARDED, recordProcessingLogDtos.get(2).getRelatedInvoiceLineInfo().getActionStatus());
      Assert.assertEquals(ActionStatus.DISCARDED, recordProcessingLogDtos.get(2).getRelatedInvoiceInfo().getActionStatus());

      async.complete();
    }));
  }

  @Test
  public void shouldNotReturnMarcBibRecordsWhenInstanceDiscarderRetrievingWithErrorsOnlyParam(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);

    String sourceRecordId1 = UUID.randomUUID().toString();
    String sourceRecordId2 = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId1, null, null, null, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId1, null, "in00000000001", null, 0, CREATE, INSTANCE, ERROR, "Error description 1", null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId2, null, null, null, 3, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId2, null, "in00000000003", null, 3, CREATE, INSTANCE, ERROR, "Error description 2", null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .param("errorsOnly", true)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries", hasSize(2))
        .body("totalRecords", is(2))
        .body("entries[0].relatedInstanceInfo.error", is("Error description 1"))
        .body("entries[1].relatedInstanceInfo.error", is("Error description 2"))
        .body("entries[0].sourceRecordOrder", is("0"))
        .body("entries[1].sourceRecordOrder", is("3"));

      RestAssured.given()
        .spec(spec)
        .param("errorsOnly", true)
        .param("entityType", "MARC")
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries", is(empty()))
        .body("totalRecords", is(0));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnOnlyInvoiceLinesWithErrorWhenRetrieveWithErrorsOnlyParam(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String invoiceLineDescription = "Some description";
    String invoiceLineId = "246816";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, "10001", "INVOICE", 0, CREATE, INVOICE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, invoiceLineId + "-1", invoiceLineDescription + "1", 1, CREATE, INVOICE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, invoiceLineId + "-2", invoiceLineDescription + "2", 2, CREATE, INVOICE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, invoiceLineId + "-3", invoiceLineDescription + "3", 3, CREATE, INVOICE, ERROR, "Exception", null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      List<RecordProcessingLogDto> jobLogEntries = RestAssured.given()
        .spec(spec)
        .when()
        .param("errorsOnly", true)
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries.size()", is(1))
        .body("totalRecords", is(1))
        .body("entries[0].jobExecutionId", is(createdJobExecution.getId()))
        .body("entries[0].sourceRecordId", is(sourceRecordId))
        .extract().body().as(RecordProcessingLogDtoCollection.class).getEntries();

      Assert.assertEquals("Exception", jobLogEntries.get(0).getRelatedInvoiceLineInfo().getError());
      Assert.assertEquals(ActionStatus.DISCARDED, jobLogEntries.get(0).getRelatedInvoiceInfo().getActionStatus());

      async.complete();
    }));
  }

  @Test
  public void shouldReturnOnlyOneSummaryEntityWhenRetrieveUsingEntityTypeParamWithValueHoldings(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);

    String sourceRecordId1 = UUID.randomUUID().toString();
    String sourceRecordId2 = UUID.randomUUID().toString();
    String sourceRecordId3 = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId1, null, null, null, 1, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId1, null, "in00000000002", null, 1, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId1, null, "ho00000000002", null, 1, CREATE, HOLDINGS, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId2, null, null, null, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId2, null, "in00000000001", null, 0, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId3, null, null, null, 3, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, "", null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId3, null, "in00000000003", null, 3, CREATE, INSTANCE, COMPLETED, null, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .param("entityType", INSTANCE.value())
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries", hasSize(3))
        .body("totalRecords", is(3));

      RestAssured.given()
        .spec(spec)
        .param("entityType", HOLDINGS.value())
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries", hasSize(1))
        .body("totalRecords", is(1));

      async.complete();
    }));
  }

  @Test
  public void shouldNotReturnWhenRetrieveFromJobWhichInitializedByInvoiceUsingEntityTypeParamWithValueMARC(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String invoiceLineDescription = "Some description";
    String invoiceLineId = "246816";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, "10001", "INVOICE", 0, CREATE, INVOICE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, invoiceLineId + "-1", invoiceLineDescription + "1", 1, CREATE, INVOICE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, invoiceLineId + "-2", invoiceLineDescription + "2", 2, CREATE, INVOICE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, invoiceLineId + "-3", invoiceLineDescription + "3", 3, CREATE, INVOICE, COMPLETED, null, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .param("entityType", "INVOICE")
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries.size()", is(3))
        .body("totalRecords", is(3));

      RestAssured.given()
        .spec(spec)
        .when()
        .param("entityType", "MARC")
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("entries.size()", is(0))
        .body("totalRecords", is(0));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnMarcBibAndAllEntitiesWithMultipleItemsAndHoldings(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = "i001";

    String[] holdingsId = generateRandomUUIDs(3);
    String[] holdingsHrid = {"h001", "h002", "h003"};

    String[] itemId = generateRandomUUIDs(4);
    String[] itemHrid = {"it001", "it002", "it003", "it004"};

    String[] permanentLocation = {UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString()};

    String errorMsg = "test error";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceId, instanceHrid, null, 0, CREATE, INSTANCE, COMPLETED, null, null))

      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, holdingsId[0], holdingsHrid[0], null, 0, CREATE, HOLDINGS, COMPLETED, null, null, instanceId, null, permanentLocation[0]))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, holdingsId[1], holdingsHrid[1], null, 0, CREATE, HOLDINGS, COMPLETED, null, null, instanceId, null, permanentLocation[1]))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, itemId[0], itemHrid[0], null, 0, CREATE, ITEM, COMPLETED, null, null, instanceId, holdingsId[0], null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, itemId[1], itemHrid[1], null, 0, CREATE, ITEM, COMPLETED, null, null, instanceId, holdingsId[1], null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, CREATE, ITEM, ERROR, errorMsg, null, null, null, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .log().all()

        .body("entries.size()", is(1))
        .body("totalRecords", is(1))
        .body("entries[0].jobExecutionId", is(createdJobExecution.getId()))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle));
      async.complete();
    }));
  }

  @Test
  public void shouldReturnMarcBibAndAllEntitiesWithItemsHoldingsWithoutDiscardedRecordProcessingLogDTOCollection(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = "i001";

    String holdingsId = UUID.randomUUID().toString();
    String holdingsHrid = "h001";

    String itemId = UUID.randomUUID().toString();
    String itemHrid = "it001";

    String permanentLocation = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceId, instanceHrid, recordTitle, 0, CREATE, INSTANCE, COMPLETED, null, null))

      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, NON_MATCH, HOLDINGS, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, NON_MATCH, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, NON_MATCH, ITEM, COMPLETED, null, null))

      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, holdingsId, holdingsHrid, recordTitle, 0, CREATE, HOLDINGS, COMPLETED, null, null, instanceId, null, permanentLocation))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, itemId, itemHrid, recordTitle, 0, CREATE, ITEM, COMPLETED, null, null, instanceId, holdingsId, null))

      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .log().all()
        .body("entries.size()", is(1))
        .body("totalRecords", is(1))
        .body("entries[0].jobExecutionId", is(createdJobExecution.getId()))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordOrder", is("0"))
        .body("entries[0].error", emptyOrNullString())
        .body("entries[0].relatedInstanceInfo.idList[0]", is(instanceId))
        .body("entries[0].relatedInstanceInfo.hridList[0]", is(instanceHrid))
        .body("entries[0].relatedInstanceInfo.error", emptyOrNullString())
        .body("entries[0].relatedHoldingsInfo.size()", is(1))
        .body("entries[0].relatedHoldingsInfo[0].id", is(holdingsId))
        .body("entries[0].relatedHoldingsInfo[0].hrid", is(holdingsHrid))
        .body("entries[0].relatedHoldingsInfo[0].permanentLocationId", is(permanentLocation))
        .body("entries[0].relatedHoldingsInfo[0].error", emptyOrNullString())
        .body("entries[0].relatedItemInfo.size()", is(1))
        .body("entries[0].relatedItemInfo[0].id", is(itemId))
        .body("entries[0].relatedItemInfo[0].hrid", is(itemHrid))
        .body("entries[0].relatedItemInfo[0].holdingsId", is(holdingsId))
        .body("entries[0].relatedItemInfo[0].error", emptyOrNullString())
        .body("entries[0].relatedInvoiceInfo.idList", empty())
        .body("entries[0].relatedInvoiceInfo.hridList", empty())
        .body("entries[0].relatedInvoiceInfo.error", emptyOrNullString());

      async.complete();
    }));
  }

  @Test
  public void shouldReturnMarcBibAndAllEntitiesWithDiscardedItemsHoldingsRecordProcessingLogDTOCollection(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, NON_MATCH, HOLDINGS, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, NON_MATCH, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, NON_MATCH, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, NON_MATCH, ITEM, COMPLETED, null, null))

      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .log().all()
        .body("entries.size()", is(1))
        .body("totalRecords", is(1))
        .body("entries[0].jobExecutionId", is(createdJobExecution.getId()))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordOrder", is("0"))
        .body("entries[0].error", emptyOrNullString())
        .body("entries[0].sourceRecordActionStatus", is(ActionStatus.DISCARDED.value()))
        .body("entries[0].relatedInstanceInfo.actionStatus", is(ActionStatus.DISCARDED.value()))
        .body("entries[0].relatedInstanceInfo.error", emptyOrNullString())
        .body("entries[0].relatedHoldingsInfo.size()", is(1))
        .body("entries[0].relatedHoldingsInfo[0].id", emptyOrNullString())
        .body("entries[0].relatedHoldingsInfo[0].hrid", emptyOrNullString())
        .body("entries[0].relatedHoldingsInfo[0].permanentLocationId", emptyOrNullString())
        .body("entries[0].relatedHoldingsInfo[0].error", emptyOrNullString())
        .body("entries[0].relatedHoldingsInfo[0].actionStatus", is(ActionStatus.DISCARDED.value()))
        .body("entries[0].relatedItemInfo.size()", is(1))
        .body("entries[0].relatedItemInfo[0].id", emptyOrNullString())
        .body("entries[0].relatedItemInfo[0].hrid", emptyOrNullString())
        .body("entries[0].relatedItemInfo[0].holdingsId", emptyOrNullString())
        .body("entries[0].relatedItemInfo[0].error", emptyOrNullString())
        .body("entries[0].relatedItemInfo[0].actionStatus", is(ActionStatus.DISCARDED.value()))
        .body("entries[0].relatedInvoiceInfo.idList", empty())
        .body("entries[0].relatedInvoiceInfo.hridList", empty())
        .body("entries[0].relatedInvoiceInfo.error", emptyOrNullString());
      async.complete();
    }));
  }

  @Test
  public void shouldNotReturnDiscardedForMarcBibAndInstanceIfHoldingsCreatedOnMatchByInstance(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";
    String instanceEntityId = UUID.randomUUID().toString();
    String holdingsEntityId = UUID.randomUUID().toString();
    String holdingsHrId = "holdingsHrid";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, MATCH, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceEntityId, null, recordTitle, 0, MATCH, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, holdingsEntityId, holdingsHrId, recordTitle, 0, CREATE, HOLDINGS, COMPLETED, null, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .log().all()
        .body("entries.size()", is(1))
        .body("totalRecords", is(1))
        .body("entries[0].jobExecutionId", is(createdJobExecution.getId()))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordOrder", is("0"))
        .body("entries[0].error", emptyOrNullString())
        .body("entries[0].sourceRecordActionStatus", emptyOrNullString())
        .body("entries[0].relatedInstanceInfo.actionStatus", emptyOrNullString())
        .body("entries[0].relatedInstanceInfo.error", emptyOrNullString())
        .body("entries[0].relatedHoldingsInfo.size()", is(1))
        .body("entries[0].relatedHoldingsInfo[0].id", is(holdingsEntityId))
        .body("entries[0].relatedHoldingsInfo[0].hrid", is(holdingsHrId))
        .body("entries[0].relatedHoldingsInfo[0].actionStatus", is(ActionStatus.CREATED.value()));
      async.complete();
    }));
  }

  @Test
  public void shouldNotReturnDiscardedForHoldingsIfItemCreatedOnMatchByHoldings(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";
    String holdingsEntityId = UUID.randomUUID().toString();

    String itemEntityId = UUID.randomUUID().toString();
    String itemHrId = "itemHrid";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, holdingsEntityId, null, recordTitle, 0, MATCH, HOLDINGS, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, itemEntityId, itemHrId, recordTitle, 0, CREATE, ITEM, COMPLETED, null, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .log().all()
        .body("entries.size()", is(1))
        .body("totalRecords", is(1))
        .body("entries[0].jobExecutionId", is(createdJobExecution.getId()))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordOrder", is("0"))
        .body("entries[0].error", emptyOrNullString())
        .body("entries[0].sourceRecordActionStatus", emptyOrNullString())
        .body("entries[0].relatedInstanceInfo.actionStatus", emptyOrNullString())
        .body("entries[0].relatedInstanceInfo.error", emptyOrNullString())
        .body("entries[0].relatedHoldingsInfo.size()", is(0))
        .body("entries[0].relatedItemInfo.size()", is(1))
        .body("entries[0].relatedItemInfo[0].id", is(itemEntityId))
        .body("entries[0].relatedItemInfo[0].hrid", is(itemHrId))
        .body("entries[0].relatedItemInfo[0].actionStatus", is(ActionStatus.CREATED.value()));
      async.complete();
    }));
  }

  @Test
  public void shouldReturnMarcBibAndAllEntitiesWithMultipleItemsUpdateRecordProcessingLogDTOCollection(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";
    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = "i001";
    String[] holdingsId = {"9f6b706f-eb88-4d36-92a7-50b03020e881", "e733fc11-c457-4ed7-9ef0-9ea669236a9a"};
    String[] itemId = generateRandomUUIDs(2);
    String[] itemHrid = {"it001", "it002"};

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceId, instanceHrid, null, 0, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, itemId[0], itemHrid[0], null, 0, UPDATE, ITEM, COMPLETED, null, null, instanceId, holdingsId[0], null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, itemId[1], itemHrid[1], null, 0, UPDATE, ITEM, COMPLETED, null, null, instanceId, holdingsId[1], null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .log().all()
        .body("entries.size()", is(1))
        .body("totalRecords", is(1))
        .body("entries[0].jobExecutionId", is(createdJobExecution.getId()))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordOrder", is("0"))
        .body("entries[0].error", emptyOrNullString())
        .body("entries[0].relatedInstanceInfo.idList[0]", is(instanceId))
        .body("entries[0].relatedInstanceInfo.hridList[0]", is(instanceHrid))
        .body("entries[0].relatedInstanceInfo.error", emptyOrNullString())
        .body("entries[0].relatedHoldingsInfo.size()", is(2))
        .body("entries[0].relatedHoldingsInfo[0].id", in(holdingsId))
        .body("entries[0].relatedHoldingsInfo[1].id", in(holdingsId))
        .body("entries[0].relatedItemInfo[0].id", in(itemId))
        .body("entries[0].relatedItemInfo[0].hrid", in(itemHrid))
        .body("entries[0].relatedItemInfo[0].error", emptyOrNullString())
        .body("entries[0].relatedItemInfo[1].id", in(itemId))
        .body("entries[0].relatedItemInfo[1].hrid", in(itemHrid))
        .body("entries[0].relatedItemInfo[1].error", emptyOrNullString())
        .body("entries[0].relatedInvoiceInfo.idList", empty())
        .body("entries[0].relatedInvoiceInfo.hridList", empty())
        .body("entries[0].relatedInvoiceInfo.error", emptyOrNullString());
      async.complete();
    }));
  }

  @Test
  public void shouldReturnCentralTenantIdForMarcRecordAndInstanceIfItIsSavedInJournalRecordRecordProcessingLogDTOCollection(TestContext context) {
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();
    String recordTitle = "test title";
    String expectedCentralTenantId = "mobius";
    String marcBibEntityId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, marcBibEntityId, null, recordTitle, 0, UPDATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null, expectedCentralTenantId))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceId, "in00000000001", null, 0, UPDATE, INSTANCE, COMPLETED, null, null, expectedCentralTenantId));

    future.onComplete(context.asyncAssertSuccess(v ->
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)

        .body("entries.size()", is(1))
        .body("totalRecords", is(1))
        .body("entries[0].jobExecutionId", is(createdJobExecution.getId()))
        .body("entries[0].sourceRecordId", is(marcBibEntityId))
        .body("entries[0].incomingRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordOrder", is("0"))
        .body("entries[0].sourceRecordTenantId", is(expectedCentralTenantId))
        .body("entries[0].relatedInstanceInfo.idList[0]", is(instanceId))
        .body("entries[0].relatedInstanceInfo.tenantId", is(expectedCentralTenantId))
        .body("entries[0].error", emptyOrNullString())));
  }

  @Test
  public void shouldReturnMarcHoldingsRecordsAndKeepTheOrderSequence(TestContext context) {
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String recordTitle = "No Content";
    String errMessage = "{\"error\":\"A new MARC-Holding was not created because the incoming record already contained a 999ff$s or 999ff$i field\"}";
    String recordOneSourceRecordId = UUID.randomUUID().toString();
    String recordTwoSourceRecordId = UUID.randomUUID().toString();
    String recordThreeSourceRecordId = UUID.randomUUID().toString();


    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v-> createJournalRecord(createdJobExecution.getId(), recordTwoSourceRecordId, null, null, null, 1, PARSE, null, ERROR, errMessage, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), recordTwoSourceRecordId, null, null, recordTitle, 1, CREATE, MARC_HOLDINGS, COMPLETED, null, null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), recordTwoSourceRecordId, UUID.randomUUID().toString(), "ho00000000002", recordTitle, 1, CREATE, HOLDINGS, COMPLETED, null, null, UUID.randomUUID().toString(), null, UUID.randomUUID().toString()))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), recordTwoSourceRecordId, UUID.randomUUID().toString(), null, null, 1, CREATE, MARC_HOLDINGS, COMPLETED, null, null))

      .compose(v-> createJournalRecord(createdJobExecution.getId(), recordOneSourceRecordId, null, null, null, 0, PARSE, null, ERROR, errMessage, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), recordOneSourceRecordId, null, null, null,0, CREATE, MARC_HOLDINGS, ERROR, errMessage, null))

      .compose(v-> createJournalRecord(createdJobExecution.getId(), recordThreeSourceRecordId, null, null, null, 2, PARSE, null, ERROR, errMessage, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), recordThreeSourceRecordId, null, null, null,2, CREATE, MARC_HOLDINGS, ERROR, errMessage, null));

    future.onComplete(context.asyncAssertSuccess(v ->
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .and().log().all()
        .body("entries.size()", is(3))
        .body("totalRecords", is(3))
        .body("entries[0].sourceRecordOrder", is("0"))
        .body("entries[1].sourceRecordOrder", is("1"))
        .body("entries[2].sourceRecordOrder", is("2"))));
  }

  private Future<JournalRecord> createJournalRecord(String jobExecutionId, String sourceId, String entityId,
                                                    String entityHrid, String title, int recordOrder,
                                                    JournalRecord.ActionType actionType, JournalRecord.EntityType entityType,
                                                    JournalRecord.ActionStatus actionStatus, String errorMessage, String orderId) {
    return createJournalRecord(jobExecutionId, sourceId, entityId, entityHrid, title, recordOrder, actionType,
      entityType, actionStatus, errorMessage, orderId, null);
  }

  private Future<JournalRecord> createJournalRecord(String jobExecutionId, String sourceId, String entityId,
                                                    String entityHrid, String title, int recordOrder,
                                                    JournalRecord.ActionType actionType, JournalRecord.EntityType entityType,
                                                    JournalRecord.ActionStatus actionStatus, String errorMessage,
                                                    String orderId, String tenantId) {
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
      .withEntityHrId(entityHrid)
      .withOrderId(orderId)
      .withTenantId(tenantId);
    return journalRecordDao.save(journalRecord, TENANT_ID).map(journalRecord);
  }

  private Future<JournalRecord> createJournalRecordAllFields(String jobExecutionId, String sourceId, String entityId, String entityHrid, String title, int recordOrder, JournalRecord.ActionType actionType,
                                                             JournalRecord.EntityType entityType, JournalRecord.ActionStatus actionStatus, String errorMessage, String orderId, String instanceId, String holdingsId, String permanentLocation) {
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
      .withEntityHrId(entityHrid)
      .withOrderId(orderId)
      .withInstanceId(instanceId)
      .withHoldingsId(holdingsId)
      .withPermanentLocationId(permanentLocation);
    return journalRecordDao.save(journalRecord, TENANT_ID).map(journalRecord);
  }

  private String[] generateRandomUUIDs(int n) {
    return IntStream.range(0, n).mapToObj(i -> UUID.randomUUID().toString()).toArray(String[]::new);
  }
}
