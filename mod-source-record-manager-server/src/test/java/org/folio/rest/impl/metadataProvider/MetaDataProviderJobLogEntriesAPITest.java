package org.folio.rest.impl.metadataProvider;

import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.COMPLETED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.ERROR;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.CREATE;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.MATCH;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.MODIFY;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.NON_MATCH;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.UPDATE;
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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import io.vertx.core.json.JsonArray;

import org.apache.http.HttpStatus;
import org.folio.dao.JournalRecordDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.ActionStatus;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobLogEntryDto;
import org.folio.rest.jaxrs.model.JobLogEntryDtoCollection;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import io.restassured.RestAssured;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.testcontainers.shaded.org.bouncycastle.est.CACertsResponse;

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
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, "in00000000001", null, 0, UPDATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
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
        .body("entries[0].sourceRecordActionStatus", is(ActionStatus.CREATED.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnOneEntryIfTwoErrorsDuringMultipleCreation(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = "i001";

    String[] holdingsId = generateRandomUUIDs(3);
    String[] holdingsHrid = {"h001","h002","h003"};

    String[] permanentLocation = {UUID.randomUUID().toString()};

    String errorMsg1 = "test error1";
    String errorMsg2 = "test error2";
    String errorArray = "[test error1, test error2]";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceId, instanceHrid, null,  0, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, holdingsId[0], holdingsHrid[0], null,  0, CREATE, HOLDINGS, COMPLETED, null, null,instanceId,null, permanentLocation[0]))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, holdingsId[1],null, null,  0, CREATE, HOLDINGS, ERROR, errorMsg1, null,null,null, null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, holdingsId[2], null, null,  0, CREATE, HOLDINGS, ERROR, errorMsg2, null,null,null, null))
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
        .body("entries[0].sourceRecordId", is(sourceRecordId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordActionStatus", is(ActionStatus.CREATED.value()))
        .body("entries[0].holdingsActionStatus", is(ActionStatus.DISCARDED.value()))
        .body("entries[0].error", is(errorArray));
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
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle,0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, MODIFY, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
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

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle,0, NON_MATCH, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
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
        .body("entries[0].sourceRecordActionStatus", is(ActionStatus.CREATED.value()));

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
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle,  0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, "in00000000001", null,  0, NON_MATCH, INSTANCE, COMPLETED, null, null))
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

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null,  0, CREATE, INSTANCE, ERROR, "error msg", null))
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
  public void shouldReturnInstanceCreatedWhenMarcModify(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, "marcEntityID", null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, "marcEntityID", null, recordTitle, 0, MODIFY, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null,  0, UPDATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, "instanceEntityID", "in00000000001", null,  0, CREATE, INSTANCE, COMPLETED, null, null))
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
        .body("entries[0].instanceActionStatus", is(ActionStatus.CREATED.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnPoLineCreatedWhenMarcCreate(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, "marcEntityID", null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, "poLineEntityID", null, null,  0, CREATE, PO_LINE, COMPLETED, null, null))
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
        .body("entries[0].poLineActionStatus", is(ActionStatus.CREATED.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnAuthorityDiscardedWhenErrorOnMatch(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, "authorityEntityID", null, recordTitle,  0, MATCH, MARC_AUTHORITY, ERROR, "errorMsg", null))
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
        .body("entries[0].sourceRecordType", is(MARC_AUTHORITY.value()))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].error", is(notNullValue()))
        .body("entries[0].sourceRecordActionStatus", is(ActionStatus.DISCARDED.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnInstanceIdWhenHoldingsCreated(TestContext context) {
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
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + holdingsCreationJobExecution.getId() + "/records/" + holdingsCreationSourceRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(holdingsCreationJobExecution.getId()))
        .body("sourceRecordId", is(holdingsCreationSourceRecordId))
        .body("sourceRecordOrder", is(0))
        .body("error", emptyOrNullString())
        .body("relatedInstanceInfo.idList[0]", is("instanceEntityID"))
        .body("relatedInstanceInfo.error", emptyOrNullString())
        .body("relatedHoldingsInfo[0].id", is("holdingsEntityID"))
        .body("relatedHoldingsInfo[0].hrid", is("ho00000000001"))
        .body("relatedHoldingsInfo[0].error", emptyOrNullString());

      async.complete();
    }));
  }

  @Test
  public void shouldReturnPoLineWithOrderIdWhenMarcCreate(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";
    String orderId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, "marcEntityID", null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, "poLineEntityID", null, null,  0, CREATE, PO_LINE, COMPLETED, "Test error", orderId))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId()+ "/records/" + sourceRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(createdJobExecution.getId()))
        .body("sourceRecordId", is(sourceRecordId))
        .body("sourceRecordTitle", is(recordTitle))
        .body("sourceRecordActionStatus", is(ActionStatus.CREATED.value()))
        .body("relatedPoLineInfo", notNullValue())
        .body("relatedPoLineInfo.orderId", is(orderId))
        .body("relatedPoLineInfo.error", is("Test error"));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnOneInstanceIdWhenMarcBibUpdatedAndInstanceUpdated(TestContext context) {
    Async async = context.async();
    JobExecution marcBibAndInstanceUpdateJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);

    String marcBibAndInstanceUpdateSourceRecordId = UUID.randomUUID().toString();

    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(marcBibAndInstanceUpdateJobExecution.getId(), marcBibAndInstanceUpdateSourceRecordId, "instanceEntityID", "in00000000001", null, 0, UPDATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(marcBibAndInstanceUpdateJobExecution.getId(), marcBibAndInstanceUpdateSourceRecordId, "instanceEntityID", "in00000000001", null, 0, UPDATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(marcBibAndInstanceUpdateJobExecution.getId(), marcBibAndInstanceUpdateSourceRecordId, "marcBibEntityID", null, recordTitle, 0, MODIFY, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + marcBibAndInstanceUpdateJobExecution.getId() + "/records/" + marcBibAndInstanceUpdateSourceRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(marcBibAndInstanceUpdateJobExecution.getId()))
        .body("sourceRecordId", is(marcBibAndInstanceUpdateSourceRecordId))
        .body("error", emptyOrNullString())
        .body("relatedInstanceInfo.idList.size()", is(1))
        .body("relatedInstanceInfo.hridList.size()", is(1))
        .body("relatedInstanceInfo.error", emptyOrNullString());

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
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null,  null, 0, CREATE, HOLDINGS, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null,null,  0, UPDATE, HOLDINGS, COMPLETED, null, null))
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
        .body("entries[0].holdingsActionStatus", is(ActionStatus.CREATED.value()));

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
        .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, "ho00000000001",  null, 0, CREATE, HOLDINGS, COMPLETED, null, null))
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
          .body("entries[0].sourceRecordTitle", is("Holdings ho00000000001"))
          .body("entries[0].holdingsRecordHridList[0]", is("ho00000000001"))
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

      context.assertTrue(Integer.parseInt(jobLogEntries.get(0).getSourceRecordOrder()) > Integer.parseInt(jobLogEntries.get(1).getSourceRecordOrder()));
      context.assertTrue(Integer.parseInt(jobLogEntries.get(1).getSourceRecordOrder()) > Integer.parseInt(jobLogEntries.get(2).getSourceRecordOrder()));
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

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId1, null, null, recordTitle1, 1, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId1, null, "in00000000001", null, 1, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId2, null, null, "title0", 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId2, null, null, null, 0, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId3, null, null, "title3", 3, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
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
        .body("entries[0].sourceRecordId", is(sourceRecordId1))
        .body("entries[0].sourceRecordTitle", is(recordTitle1))
        .body("entries[0].sourceRecordOrder", is("1"))
        .body("entries[0].holdingsRecordHridList", is(empty()))
        .body("entries[0].sourceRecordType", is(MARC_BIBLIOGRAPHIC.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnAuthorityCreated(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, MARC_AUTHORITY, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null,  null, 0, CREATE, AUTHORITY, COMPLETED, null, null))
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
        .body("entries[0].sourceRecordActionStatus", is(ActionStatus.CREATED.value()))
        .body("entries[0].authorityActionStatus", is(ActionStatus.CREATED.value()));
      async.complete();
    }));
  }

  @Test
  public void shouldReturnEmptyDtoIfHasNoLogRecordsBySpecifiedJobIdAndRecordId() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + UUID.randomUUID() + "/records/" + UUID.randomUUID())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnMarcBibUpdatedByJobAndRecordIds(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, UPDATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId() + "/records/" + sourceRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(createdJobExecution.getId()))
        .body("sourceRecordId", is(sourceRecordId))
        .body("sourceRecordTitle", is(recordTitle))
        .body("sourceRecordOrder", is(0))
        .body("sourceRecordActionStatus", is(ActionStatus.CREATED.value()));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnEmptyMarcBibErrorAndInstanceDiscardedWhenInstanceCreationFailed(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String entityId = UUID.randomUUID().toString();
    String entityHrid = "001";
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, entityId, entityHrid, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, entityId, entityHrid, null,  0, CREATE, INSTANCE, ERROR, "error msg", null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId() + "/records/" + sourceRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(createdJobExecution.getId()))
        .body("sourceRecordId", is(sourceRecordId))
        .body("sourceRecordTitle", is(recordTitle))
        .body("sourceRecordOrder", is(0))
        .body("error", emptyOrNullString())
        .body("relatedInstanceInfo.idList[0]", is(entityId))
        .body("relatedInstanceInfo.hridList[0]", is(entityHrid))
        .body("relatedInstanceInfo.error", is("error msg"));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnNotEmptyMarcBibErrorWhenMarcBibFailed(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, ERROR, "MarcBib error msg", null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId() + "/records/" + sourceRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(createdJobExecution.getId()))
        .body("sourceRecordId", is(sourceRecordId))
        .body("sourceRecordTitle", is(recordTitle))
        .body("sourceRecordOrder", is(0))
        .body("error", is("MarcBib error msg"));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnMarcBibAndAllEntitiesWithoutErrors(TestContext context) {
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
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceId, instanceHrid, null,  0, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, holdingsId, holdingsHrid, null,  0, CREATE, HOLDINGS, COMPLETED, null, null,instanceId,null,null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, itemId, itemHrid, null,  0, CREATE, ITEM, COMPLETED, null, null,instanceId,holdingsId,null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, poLineId, poLineHrid, null,  0, CREATE, PO_LINE, COMPLETED, null, null,instanceId,null,null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId() + "/records/" + sourceRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(createdJobExecution.getId()))
        .body("sourceRecordId", is(sourceRecordId))
        .body("sourceRecordTitle", is(recordTitle))
        .body("sourceRecordOrder", is(0))
        .body("error", emptyOrNullString())
        .body("relatedInstanceInfo.idList[0]", is(instanceId))
        .body("relatedInstanceInfo.hridList[0]", is(instanceHrid))
        .body("relatedInstanceInfo.error", emptyOrNullString())
        .body("relatedHoldingsInfo[0].id", is(holdingsId))
        .body("relatedHoldingsInfo[0].hrid", is(holdingsHrid))
        .body("relatedHoldingsInfo[0].error", emptyOrNullString())
        .body("relatedItemInfo[0].id", is(itemId))
        .body("relatedItemInfo[0].hrid", is(itemHrid))
        .body("relatedItemInfo[0].error", emptyOrNullString())
        .body("relatedPoLineInfo.idList[0]", is(poLineId))
        .body("relatedPoLineInfo.hridList[0]", is(poLineHrid))
        .body("relatedPoLineInfo.error", emptyOrNullString())
        .body("relatedInvoiceInfo.idList", empty())
        .body("relatedInvoiceInfo.hridList", empty())
        .body("relatedInvoiceInfo.error", emptyOrNullString());

      async.complete();
    }));
  }

  @Test
  public void shouldReturnDataForParticularInvoiceLine(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String invoiceId = UUID.randomUUID().toString();
    String invoiceHrid = "228D126";
    String invoiceVendorNumber = "0704159";
    String invoiceLineId1 = UUID.randomUUID().toString();
    String invoiceLineId2 = UUID.randomUUID().toString();
    String invoiceLineDescription = "Some description";

    Promise<String> journalRecordIdPromise = Promise.promise();
    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null,0, CREATE, EDIFACT, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, invoiceId, invoiceHrid, "INVOICE", 0, CREATE, INVOICE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, invoiceLineId1, invoiceVendorNumber + "-1", invoiceLineDescription + "1", 1, CREATE, INVOICE, COMPLETED, null, null))
      .onSuccess(journalRecord -> journalRecordIdPromise.complete(journalRecord.getId()))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, invoiceLineId2, invoiceVendorNumber + "-2", invoiceLineDescription + "2", 2, CREATE, INVOICE, COMPLETED, null, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      String invoiceLineJournalRecordId = journalRecordIdPromise.future().result();
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId() + "/records/" + invoiceLineJournalRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(createdJobExecution.getId()))
        .body("sourceRecordId", is(sourceRecordId))
        .body("sourceRecordOrder", is(0))
        .body("sourceRecordTitle", is(invoiceLineDescription + "1"))
        .body("error", emptyOrNullString())
        .body("relatedInstanceInfo.idList.size", empty())
        .body("relatedInstanceInfo.hridList.size", empty())
        .body("relatedInstanceInfo.error", nullValue())
        .body("relatedHoldingsInfo.size", empty())
        .body("relatedItemInfo.size", empty())
        .body("relatedPoLineInfo.idList.size", empty())
        .body("relatedPoLineInfo.hridList.size", empty())
        .body("relatedPoLineInfo.error", emptyOrNullString())
        .body("relatedInvoiceInfo.idList[0]", is(invoiceId))
        .body("relatedInvoiceInfo.hridList[0]", is(invoiceHrid))
        .body("relatedInvoiceInfo.error", emptyOrNullString())
        .body("relatedInvoiceLineInfo.id", is(invoiceLineId1))
        .body("relatedInvoiceLineInfo.fullInvoiceLineNumber", is(invoiceVendorNumber + "-1"))
        .body("relatedInvoiceLineInfo.error", emptyOrNullString());
      async.complete();
    }));
  }

  @Test
  public void shouldReturnInvoiceLineInfoWithError(TestContext context) {
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
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null,0, CREATE, EDIFACT, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, invoiceId, invoiceHrid, "INVOICE", 0, CREATE, INVOICE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, invoiceLineId1, invoiceVendorNumber + "-1", invoiceLineDescription + "1", 1, CREATE, INVOICE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, invoiceVendorNumber + "-2", invoiceLineDescription + "2", 2, CREATE, INVOICE, ERROR, errorMsg, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      String invoiceLineJournalRecordId = future.result().getId();
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId() + "/records/" + invoiceLineJournalRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(createdJobExecution.getId()))
        .body("sourceRecordId", is(sourceRecordId))
        .body("sourceRecordOrder", is(0))
        .body("sourceRecordTitle", is(invoiceLineDescription + "2"))
        .body("error", emptyOrNullString())
        .body("relatedInvoiceInfo.idList[0]", is(invoiceId))
        .body("relatedInvoiceInfo.hridList[0]", is(invoiceHrid))
        .body("relatedInvoiceInfo.error", emptyOrNullString())
        .body("relatedInvoiceLineInfo.id", nullValue())
        .body("relatedInvoiceLineInfo.fullInvoiceLineNumber", is(invoiceVendorNumber + "-2"))
        .body("relatedInvoiceLineInfo.error", is(errorMsg));
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
      List<JobLogEntryDto> jobLogEntries = RestAssured.given()
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
        .extract().body().as(JobLogEntryDtoCollection.class).getEntries();

      Assert.assertEquals("Exception", jobLogEntries.get(2).getError());
      Assert.assertEquals(ActionStatus.DISCARDED, jobLogEntries.get(2).getInvoiceActionStatus());

      async.complete();
    }));
  }

  @Test
  public void shouldNotReturnMarcBibRecordsWhenInstanceDiscarderRetrievingWithErrorsOnlyParam(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);

    String sourceRecordId1 = UUID.randomUUID().toString();
    String sourceRecordId2 = UUID.randomUUID().toString();
    String sourceRecordId3 = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId1, null, null, null, 1, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId1, null, "in00000000002", null, 1, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId2, null, null, null, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId2, null, "in00000000001", null, 0, CREATE, INSTANCE, ERROR, "Error description 1", null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId3, null, null, null, 3, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, "", null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId3, null, "in00000000003", null, 3, CREATE, INSTANCE, ERROR, "Error description 2", null))
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
        .body("entries[0].error", is("Error description 1"))
        .body("entries[1].error", is("Error description 2"))
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
      List<JobLogEntryDto> jobLogEntries = RestAssured.given()
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
        .extract().body().as(JobLogEntryDtoCollection.class).getEntries();

      Assert.assertEquals("Exception", jobLogEntries.get(0).getError());
      Assert.assertEquals(ActionStatus.DISCARDED, jobLogEntries.get(0).getInvoiceActionStatus());

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
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId1, null, "ho00000000002",  null, 1, CREATE, HOLDINGS, COMPLETED, null, null))
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
  public void shouldReturnMarcBibAndAllEntitiesWithMultipleItemsHoldings(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = "i001";

    String[] holdingsId = generateRandomUUIDs(3);
    String[] holdingsHrid = {"h001","h002","h003"};

    String[] itemId = generateRandomUUIDs(4);
    String[] itemHrid = {"it001","it002","it003","it004"};

    String[] permanentLocation = {UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString()};

    String errorMsg = "test error";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceId, instanceHrid, null,  0, CREATE, INSTANCE, COMPLETED, null, null))

      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, holdingsId[0], holdingsHrid[0], null,  0, CREATE, HOLDINGS, COMPLETED, null, null,instanceId,null, permanentLocation[0]))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, holdingsId[1], holdingsHrid[1], null,  0, CREATE, HOLDINGS, COMPLETED, null, null,instanceId,null, permanentLocation[1]))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, holdingsId[2], holdingsHrid[2], null,  0, CREATE, HOLDINGS, COMPLETED, null, null,instanceId,null, permanentLocation[2]))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, itemId[0], itemHrid[0], null,  0, CREATE, ITEM, COMPLETED, null, null,instanceId,holdingsId[0],null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, itemId[1], itemHrid[1], null,  0, CREATE, ITEM, COMPLETED, null, null,instanceId,holdingsId[1],null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, itemId[2], itemHrid[2], null,  0, CREATE, ITEM, COMPLETED, null, null,instanceId,holdingsId[1],null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, itemId[3], null, null,  0, CREATE, ITEM, ERROR, errorMsg, null,null, null,null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId() + "/records/" + sourceRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .log().all()
        .body("jobExecutionId", is(createdJobExecution.getId()))
        .body("sourceRecordId", is(sourceRecordId))
        .body("sourceRecordTitle", is(recordTitle))
        .body("sourceRecordOrder", is(0))
        .body("error", emptyOrNullString())
        .body("relatedInstanceInfo.idList[0]", is(instanceId))
        .body("relatedInstanceInfo.hridList[0]", is(instanceHrid))
        .body("relatedInstanceInfo.error", emptyOrNullString())
        .body("relatedHoldingsInfo.size()", is(3))
        .body("relatedHoldingsInfo[0].id", is(holdingsId[0]))
        .body("relatedHoldingsInfo[0].hrid", is(holdingsHrid[0]))
        .body("relatedHoldingsInfo[0].permanentLocationId", is(permanentLocation[0]))
        .body("relatedHoldingsInfo[0].error", emptyOrNullString())
        .body("relatedHoldingsInfo[1].id", is(holdingsId[1]))
        .body("relatedHoldingsInfo[1].hrid", is(holdingsHrid[1]))
        .body("relatedHoldingsInfo[1].permanentLocationId", is(permanentLocation[1]))
        .body("relatedHoldingsInfo[1].error", emptyOrNullString())
        .body("relatedHoldingsInfo[2].id", is(holdingsId[2]))
        .body("relatedHoldingsInfo[2].hrid", is(holdingsHrid[2]))
        .body("relatedHoldingsInfo[2].permanentLocationId", is(permanentLocation[2]))
        .body("relatedHoldingsInfo[2].error", emptyOrNullString())
        .body("relatedItemInfo[0].id", is(itemId[0]))
        .body("relatedItemInfo[0].hrid", is(itemHrid[0]))
        .body("relatedItemInfo[0].holdingsId", is(holdingsId[0]))
        .body("relatedItemInfo[0].error", emptyOrNullString())
        .body("relatedItemInfo[2].id", is(itemId[2]))
        .body("relatedItemInfo[2].hrid", is(itemHrid[2]))
        .body("relatedItemInfo[2].holdingsId", is(holdingsId[1]))
        .body("relatedItemInfo[2].error", emptyOrNullString())
        .body("relatedItemInfo[3].id", is(itemId[3]))
        .body("relatedItemInfo[3].hrid", emptyOrNullString())
        .body("relatedItemInfo[3].holdingsId", emptyOrNullString())
        .body("relatedItemInfo[3].error", is(errorMsg))
        .body("relatedInvoiceInfo.idList", empty())
        .body("relatedInvoiceInfo.hridList", empty())
        .body("relatedInvoiceInfo.error", emptyOrNullString());

      async.complete();
    }));
  }

  private Future<JournalRecord> createJournalRecord(String jobExecutionId, String sourceId, String entityId, String entityHrid, String title, int recordOrder, JournalRecord.ActionType actionType,
                                                    JournalRecord.EntityType entityType, JournalRecord.ActionStatus actionStatus, String errorMessage, String orderId) {
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
      .withOrderId(orderId);
    return journalRecordDao.save(journalRecord, TENANT_ID).map(journalRecord);
  }
  private Future<JournalRecord> createJournalRecordAllFields(String jobExecutionId, String sourceId, String entityId, String entityHrid, String title, int recordOrder, JournalRecord.ActionType actionType,
                                                    JournalRecord.EntityType entityType, JournalRecord.ActionStatus actionStatus, String errorMessage, String orderId,String instanceId,String holdingsId,String permanentLocation) {
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
    return IntStream.range(0,n).mapToObj(i->UUID.randomUUID().toString()).toArray(String[]::new);
  }
}
