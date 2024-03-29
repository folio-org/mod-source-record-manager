package org.folio.rest.impl.metadataProvider;

import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.COMPLETED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.ERROR;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.CREATE;
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
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.util.Date;
import java.util.UUID;
import java.util.stream.IntStream;

import org.apache.http.HttpStatus;
import org.folio.dao.JournalRecordDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.ActionStatus;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import io.restassured.RestAssured;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

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
  public void shouldReturnInstanceIdWhenHoldingsCreated(TestContext context) {
    Async async = context.async();
    JobExecution instanceCreationJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);

    String instanceCreationSourceRecordId = UUID.randomUUID().toString();

    String recordTitle = "test title";
    String instanceEntityId = UUID.randomUUID().toString();
    String holdingsEntityId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(instanceCreationJobExecution.getId(), instanceCreationSourceRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
      .compose(v -> createJournalRecord(instanceCreationJobExecution.getId(), instanceCreationSourceRecordId, UUID.randomUUID().toString(), null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(instanceCreationJobExecution.getId(), instanceCreationSourceRecordId, instanceEntityId, "in00000000001", recordTitle, 0, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(instanceCreationJobExecution.getId(), instanceCreationSourceRecordId, holdingsEntityId, "ho00000000001", recordTitle, 0, CREATE, HOLDINGS, COMPLETED, null, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + instanceCreationJobExecution.getId() + "/records/" + instanceCreationSourceRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(instanceCreationJobExecution.getId()))
        .body("incomingRecordId", is(instanceCreationSourceRecordId))
        .body("sourceRecordOrder", is("0"))
        .body("error", emptyOrNullString())
        .body("relatedInstanceInfo.idList[0]", is(instanceEntityId))
        .body("relatedInstanceInfo.error", emptyOrNullString())
        .body("relatedHoldingsInfo[0].id", is(holdingsEntityId))
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
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, UUID.randomUUID().toString(), null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, UUID.randomUUID().toString(), null, null,  0, CREATE, PO_LINE, COMPLETED, "Test error", orderId))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId()+ "/records/" + sourceRecordId)
        .then().log().all()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(createdJobExecution.getId()))
        .body("incomingRecordId", is(sourceRecordId))
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
      .compose(v -> createJournalRecord(marcBibAndInstanceUpdateJobExecution.getId(), marcBibAndInstanceUpdateSourceRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
      .compose(v -> createJournalRecord(marcBibAndInstanceUpdateJobExecution.getId(), marcBibAndInstanceUpdateSourceRecordId, UUID.randomUUID().toString(), "in00000000001", null, 0, UPDATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(marcBibAndInstanceUpdateJobExecution.getId(), marcBibAndInstanceUpdateSourceRecordId, UUID.randomUUID().toString(), "in00000000001", null, 0, UPDATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecord(marcBibAndInstanceUpdateJobExecution.getId(), marcBibAndInstanceUpdateSourceRecordId, UUID.randomUUID().toString(), null, recordTitle, 0, MODIFY, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + marcBibAndInstanceUpdateJobExecution.getId() + "/records/" + marcBibAndInstanceUpdateSourceRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(marcBibAndInstanceUpdateJobExecution.getId()))
        .body("incomingRecordId", is(marcBibAndInstanceUpdateSourceRecordId))
        .body("error", emptyOrNullString())
        .body("relatedInstanceInfo.idList.size()", is(1))
        .body("relatedInstanceInfo.hridList.size()", is(1))
        .body("relatedInstanceInfo.error", emptyOrNullString());

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
    String entityId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, entityId, null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, entityId, null, null, 0, UPDATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId() + "/records/" + sourceRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(createdJobExecution.getId()))
        .body("incomingRecordId", is(sourceRecordId))
        .body("sourceRecordId", is(entityId))
        .body("sourceRecordTitle", is(recordTitle))
        .body("sourceRecordOrder", is("0"))
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
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
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
        .body("incomingRecordId", is(sourceRecordId))
        .body("sourceRecordTitle", is(recordTitle))
        .body("sourceRecordOrder", is("0"))
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
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, UUID.randomUUID().toString(), null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, ERROR, "MarcBib error msg", null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId() + "/records/" + sourceRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(createdJobExecution.getId()))
        .body("incomingRecordId", is(sourceRecordId))
        .body("sourceRecordTitle", is(recordTitle))
        .body("sourceRecordOrder", is("0"))
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
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, UUID.randomUUID().toString(), null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
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
        .body("incomingRecordId", is(sourceRecordId))
        .body("sourceRecordTitle", is(recordTitle))
        .body("sourceRecordOrder", is("0"))
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
  public void shouldReturnDiscardedForHoldingsIfNoHoldingsCreated(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = "i001";

    String testError = "testError";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, UUID.randomUUID().toString(), null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceId, instanceHrid, null,  0, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle,  0, CREATE, HOLDINGS, ERROR, testError, null, null,null,null));

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId() + "/records/" + sourceRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(createdJobExecution.getId()))
        .body("incomingRecordId", is(sourceRecordId))
        .body("sourceRecordTitle", is(recordTitle))
        .body("sourceRecordOrder", is("0"))
        .body("error", emptyOrNullString())
        .body("relatedInstanceInfo.idList[0]", is(instanceId))
        .body("relatedInstanceInfo.hridList[0]", is(instanceHrid))
        .body("relatedInstanceInfo.error", emptyOrNullString())
        .body("relatedHoldingsInfo[0].id", emptyOrNullString())
        .body("relatedHoldingsInfo[0].hrid", emptyOrNullString())
        .body("relatedHoldingsInfo[0].error", is(testError));

      async.complete();
    }));
  }

  @Test
  public void shouldReturnDiscardedForItemsIfNoItemsCreated(TestContext context) {
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
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, UUID.randomUUID().toString(), null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceId, instanceHrid, null,  0, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, holdingsId, holdingsHrid, recordTitle,  0, CREATE, HOLDINGS, COMPLETED, null, null,instanceId,null,null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle,  0, CREATE, ITEM, ERROR, testError, null, null,null,null));

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId() + "/records/" + sourceRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(createdJobExecution.getId()))
        .body("incomingRecordId", is(sourceRecordId))
        .body("sourceRecordTitle", is(recordTitle))
        .body("sourceRecordOrder", is("0"))
        .body("error", emptyOrNullString())
        .body("relatedInstanceInfo.idList[0]", is(instanceId))
        .body("relatedInstanceInfo.hridList[0]", is(instanceHrid))
        .body("relatedInstanceInfo.error", emptyOrNullString())
        .body("relatedHoldingsInfo[0].id", is(holdingsId))
        .body("relatedHoldingsInfo[0].hrid", is(holdingsHrid))
        .body("relatedHoldingsInfo[0].error", emptyOrNullString())
        .body("relatedItemInfo[0].id", emptyOrNullString())
        .body("relatedItemInfo[0].hrid", emptyOrNullString())
        .body("relatedItemInfo[0].error", is(testError));
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
        .body("sourceRecordOrder", is("0"))
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
        .body("incomingRecordId", is(sourceRecordId))
        .body("sourceRecordOrder", is("0"))
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
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, UUID.randomUUID().toString(), null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
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
        .body("incomingRecordId", is(sourceRecordId))
        .body("sourceRecordTitle", is(recordTitle))
        .body("sourceRecordOrder", is("0"))
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

  @Test
  public void shouldReturnMarcBibAndAllEntitiesWithItemsHoldingsWithoutDiscarded(TestContext context) {
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
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, UUID.randomUUID().toString(), null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
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
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId() + "/records/" + sourceRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .log().all()
        .body("jobExecutionId", is(createdJobExecution.getId()))
        .body("incomingRecordId", is(sourceRecordId))
        .body("sourceRecordTitle", is(recordTitle))
        .body("sourceRecordOrder", is("0"))
        .body("error", emptyOrNullString())
        .body("relatedInstanceInfo.idList[0]", is(instanceId))
        .body("relatedInstanceInfo.hridList[0]", is(instanceHrid))
        .body("relatedInstanceInfo.error", emptyOrNullString())
        .body("relatedHoldingsInfo.size()", is(1))
        .body("relatedHoldingsInfo[0].id", is(holdingsId))
        .body("relatedHoldingsInfo[0].hrid", is(holdingsHrid))
        .body("relatedHoldingsInfo[0].permanentLocationId", is(permanentLocation))
        .body("relatedHoldingsInfo[0].error", emptyOrNullString())
        .body("relatedItemInfo.size()", is(1))
        .body("relatedItemInfo[0].id", is(itemId))
        .body("relatedItemInfo[0].hrid", is(itemHrid))
        .body("relatedItemInfo[0].holdingsId", is(holdingsId))
        .body("relatedItemInfo[0].error", emptyOrNullString())
        .body("relatedInvoiceInfo.idList", empty())
        .body("relatedInvoiceInfo.hridList", empty())
        .body("relatedInvoiceInfo.error", emptyOrNullString());

      async.complete();
    }));
  }

  @Test
  public void shouldReturnItemsHoldingsWithUpdatedAction(TestContext context) {
    var async = context.async();
    var createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    var sourceRecordId = UUID.randomUUID().toString();
    var recordTitle = "test title";

    var instanceId = UUID.randomUUID().toString();
    var holdingsId = UUID.randomUUID().toString();
    var itemId = UUID.randomUUID().toString();

    var instanceHrid = "i001";
    var holdingsHrid = "h001";
    var itemHrid = "it001";

    var actionStatus = "UPDATED";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, UUID.randomUUID().toString(), null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceId, instanceHrid, recordTitle, 0, CREATE, INSTANCE, COMPLETED, null, null))

      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, itemId, itemHrid, recordTitle, 0, UPDATE, ITEM, COMPLETED, null, null, instanceId, holdingsId, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, holdingsId, holdingsHrid, recordTitle, 0, UPDATE, HOLDINGS, COMPLETED, null, null))

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
        .body("incomingRecordId", is(sourceRecordId))
        .body("sourceRecordTitle", is(recordTitle))
        .body("sourceRecordOrder", is("0"))
        .body("error", emptyOrNullString())
        .body("relatedInstanceInfo.idList[0]", is(instanceId))
        .body("relatedInstanceInfo.hridList[0]", is(instanceHrid))
        .body("relatedInstanceInfo.error", emptyOrNullString())
        .body("relatedHoldingsInfo.size()", is(1))
        .body("relatedHoldingsInfo[0].id", is(holdingsId))
        .body("relatedHoldingsInfo[0].hrid", is(holdingsHrid))
        .body("relatedHoldingsInfo[0].actionStatus", is(actionStatus))
        .body("relatedHoldingsInfo[0].error", emptyOrNullString())
        .body("relatedItemInfo.size()", is(1))
        .body("relatedItemInfo[0].id", is(itemId))
        .body("relatedItemInfo[0].hrid", is(itemHrid))
        .body("relatedItemInfo[0].holdingsId", is(holdingsId))
        .body("relatedHoldingsInfo[0].actionStatus", is(actionStatus))
        .body("relatedItemInfo[0].error", emptyOrNullString());
      async.complete();
    }));
  }

  @Test
  public void shouldReturnMarcBibAndAllEntitiesWithDiscardedItemsHoldings(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = "i001";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, UUID.randomUUID().toString(), null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceId, instanceHrid, recordTitle, 0, CREATE, INSTANCE, COMPLETED, null, null))

      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, NON_MATCH, HOLDINGS, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, NON_MATCH, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, recordTitle, 0, NON_MATCH, ITEM, COMPLETED, null, null))

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
        .body("incomingRecordId", is(sourceRecordId))
        .body("sourceRecordTitle", is(recordTitle))
        .body("sourceRecordOrder", is("0"))
        .body("error", emptyOrNullString())
        .body("relatedInstanceInfo.idList[0]", is(instanceId))
        .body("relatedInstanceInfo.hridList[0]", is(instanceHrid))
        .body("relatedInstanceInfo.error", emptyOrNullString())
        .body("relatedHoldingsInfo.size()", is(1))
        .body("relatedHoldingsInfo[0].id", emptyOrNullString())
        .body("relatedHoldingsInfo[0].hrid", emptyOrNullString())
        .body("relatedHoldingsInfo[0].permanentLocationId", emptyOrNullString())
        .body("relatedHoldingsInfo[0].error", emptyOrNullString())
        .body("relatedItemInfo.size()", is(1))
        .body("relatedItemInfo[0].id", emptyOrNullString())
        .body("relatedItemInfo[0].hrid",emptyOrNullString())
        .body("relatedItemInfo[0].holdingsId",emptyOrNullString())
        .body("relatedItemInfo[0].error", emptyOrNullString())
        .body("relatedInvoiceInfo.idList", empty())
        .body("relatedInvoiceInfo.hridList", empty())
        .body("relatedInvoiceInfo.error", emptyOrNullString());

      async.complete();
    }));
  }

  @Test
  public void shouldReturnMarcBibAndAllEntitiesWithMultipleItemsUpdate(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";
    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = "i001";
    String[] holdingsId = generateRandomUUIDs(2);
    String[] itemId = generateRandomUUIDs(2);
    String[] itemHrid = {"it001", "it002"};

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, UUID.randomUUID().toString(), null, recordTitle, 0, CREATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceId, instanceHrid, null, 0, CREATE, INSTANCE, COMPLETED, null, null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, itemId[0], itemHrid[0], null, 0, UPDATE, ITEM, COMPLETED, null, null, instanceId, holdingsId[0], null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), sourceRecordId, itemId[1], itemHrid[1], null, 0, UPDATE, ITEM, COMPLETED, null, null, instanceId, holdingsId[1], null))
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
        .body("incomingRecordId", is(sourceRecordId))
        .body("sourceRecordTitle", is(recordTitle))
        .body("sourceRecordOrder", is("0"))
        .body("error", emptyOrNullString())
        .body("relatedInstanceInfo.idList[0]", is(instanceId))
        .body("relatedInstanceInfo.hridList[0]", is(instanceHrid))
        .body("relatedInstanceInfo.error", emptyOrNullString())
        .body("relatedHoldingsInfo.size()", is(2))
        .body("relatedHoldingsInfo[0].id", in(holdingsId))
        .body("relatedHoldingsInfo[1].id", in(holdingsId))
        .body("relatedItemInfo[0].id", in(itemId))
        .body("relatedItemInfo[0].hrid", in(itemHrid))
        .body("relatedItemInfo[0].error", emptyOrNullString())
        .body("relatedItemInfo[1].id", in(itemId))
        .body("relatedItemInfo[1].hrid", in(itemHrid))
        .body("relatedItemInfo[1].error", emptyOrNullString())
        .body("relatedInvoiceInfo.idList", empty())
        .body("relatedInvoiceInfo.hridList", empty())
        .body("relatedInvoiceInfo.error", emptyOrNullString());

      async.complete();
    }));
  }

  @Test
  public void shouldReturnCentralTenantIdForMarcRecordAndInstanceIfItIsSavedInJournalRecord(TestContext context) {
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String sourceRecordId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();
    String recordTitle = "test title";
    String expectedCentralTenantId = "mobius";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, UUID.randomUUID().toString(), null, recordTitle, 0, UPDATE, MARC_BIBLIOGRAPHIC, COMPLETED, null, null, expectedCentralTenantId))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), sourceRecordId, instanceId, "in00000000001", null, 0, UPDATE, INSTANCE, COMPLETED, null, null, expectedCentralTenantId));

    future.onComplete(context.asyncAssertSuccess(v ->
      RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId() + "/records/" + sourceRecordId)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutionId", is(createdJobExecution.getId()))
      .body("incomingRecordId", is(sourceRecordId))
      .body("sourceRecordTitle", is(recordTitle))
      .body("sourceRecordOrder", is("0"))
      .body("sourceRecordTenantId", is(expectedCentralTenantId))
      .body("relatedInstanceInfo.idList[0]", is(instanceId))
      .body("relatedInstanceInfo.tenantId", is(expectedCentralTenantId))
      .body("error", emptyOrNullString())));
  }

  @Test
  public void shouldReturnIncomingRecordDataIfMarcBibWasNotMatched(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String incomingRecordId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), incomingRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), incomingRecordId, null, null, recordTitle, 0, NON_MATCH, MARC_BIBLIOGRAPHIC, COMPLETED, null, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId() + "/records/" + incomingRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(createdJobExecution.getId()))
        .body("incomingRecordId", is(incomingRecordId))
        .body("sourceRecordOrder", is("0"))
        .body("error", emptyOrNullString());
      async.complete();
    }));
  }

  @Test
  public void shouldReturnAuthorityDataIfAuthorityWasCreated(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String incomingRecordId = UUID.randomUUID().toString();
    String marcAuthorityId = UUID.randomUUID().toString();
    String authorityId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), incomingRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), incomingRecordId, marcAuthorityId, null, recordTitle, 0, CREATE, MARC_AUTHORITY, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), incomingRecordId, authorityId, null, null, 0, CREATE, AUTHORITY, COMPLETED, null, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId() + "/records/" + incomingRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(createdJobExecution.getId()))
        .body("incomingRecordId", is(incomingRecordId))
        .body("sourceRecordId", is(marcAuthorityId))
        .body("sourceRecordTitle", is(recordTitle))
        .body("sourceRecordOrder", is("0"))
        .body("relatedAuthorityInfo.actionStatus", is(ActionStatus.CREATED.value()))
        .body("relatedAuthorityInfo.idList[0]", is(authorityId))
        .body("relatedAuthorityInfo.error", emptyOrNullString())
        .body("error", emptyOrNullString());
      async.complete();
    }));
  }

  @Test
  public void shouldReturnOneLogEntryAuthorityDataIfAuthorityWasCreated(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String incomingRecordId = UUID.randomUUID().toString();
    String marcAuthorityId = UUID.randomUUID().toString();
    String authorityId = UUID.randomUUID().toString();
    String recordTitle = "test title";

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), incomingRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), incomingRecordId, marcAuthorityId, null, recordTitle, 0, CREATE, MARC_AUTHORITY, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), incomingRecordId, authorityId, null, null, 0, CREATE, AUTHORITY, COMPLETED, null, null))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId() + "?limit=100&order=asc")
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("totalRecords", is(1))
        .body("entries[0].incomingRecordId", is(incomingRecordId))
        .body("entries[0].sourceRecordId", is(marcAuthorityId))
        .body("entries[0].sourceRecordTitle", is(recordTitle))
        .body("entries[0].sourceRecordOrder", is("0"));
      async.complete();
    }));
  }

  @Test
  public void shouldReturnHoldingsDataIfHoldingsPoweredByMarcHoldingsWasCreated(TestContext context) {
    Async async = context.async();
    JobExecution createdJobExecution = constructAndPostInitJobExecutionRqDto(1).getJobExecutions().get(0);
    String incomingRecordId = UUID.randomUUID().toString();
    String marcHoldingsId = UUID.randomUUID().toString();
    String holdingId = UUID.randomUUID().toString();
    String holdingHrid = "ho00000000001";
    String holdingPermanentLocationId = UUID.randomUUID().toString();

    Future<JournalRecord> future = Future.succeededFuture()
      .compose(v -> createJournalRecord(createdJobExecution.getId(), incomingRecordId, null, null, null, 0, PARSE, null, COMPLETED, null, null))
      .compose(v -> createJournalRecord(createdJobExecution.getId(), incomingRecordId, marcHoldingsId, null, null, 0, CREATE, MARC_HOLDINGS, COMPLETED, null, null))
      .compose(v -> createJournalRecordAllFields(createdJobExecution.getId(), incomingRecordId, holdingId, holdingHrid, null, 0, CREATE, HOLDINGS, COMPLETED, null, null, null, null, holdingPermanentLocationId))
      .onFailure(context::fail);

    future.onComplete(ar -> context.verify(v -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_JOB_EXECUTION_JOURNAL_RECORDS_PATH + "/" + createdJobExecution.getId() + "/records/" + incomingRecordId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("jobExecutionId", is(createdJobExecution.getId()))
        .body("incomingRecordId", is(incomingRecordId))
        .body("sourceRecordId", is(marcHoldingsId))
        .body("sourceRecordOrder", is("0"))
        .body("relatedHoldingsInfo.size()", is(1))
        .body("relatedHoldingsInfo[0].actionStatus", is(ActionStatus.CREATED.value()))
        .body("relatedHoldingsInfo[0].id", is(holdingId))
        .body("relatedHoldingsInfo[0].hrid", is(holdingHrid))
        .body("relatedHoldingsInfo[0].permanentLocationId", is(holdingPermanentLocationId))
        .body("relatedHoldingsInfo[0].error", emptyOrNullString())
        .body("error", emptyOrNullString());
      async.complete();
    }));
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
