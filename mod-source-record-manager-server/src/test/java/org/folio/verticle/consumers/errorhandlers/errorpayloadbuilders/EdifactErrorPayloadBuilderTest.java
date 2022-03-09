package org.folio.verticle.consumers.errorhandlers.errorpayloadbuilders;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.JobExecutionService;
import org.folio.verticle.consumers.errorhandlers.payloadbuilders.EdifactDiErrorPayloadBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;

import static org.folio.ActionProfile.FolioRecord.INVOICE;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.verticle.consumers.errorhandlers.RawMarcChunksErrorHandler.ERROR_KEY;
import static org.folio.verticle.consumers.errorhandlers.payloadbuilders.EdifactDiErrorPayloadBuilder.INVOICE_LINES_FIELD;
import static org.mockito.ArgumentMatchers.any;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


@RunWith(VertxUnitRunner.class)
public class EdifactErrorPayloadBuilderTest {
  private static final String JOB_EXECUTION_ID = UUID.randomUUID().toString();
  private static final String TENANT_ID = "diku";
  private static final String TOKEN = "token";
  public static final String LARGE_PAYLOAD_ERROR_MESSAGE = "Large payload!";
  public static final String INVOICE_FIELD = "INVOICE";
  public static final String INVOICE_LINES_FILED = "INVOICE_LINES";
  public static final String EDIFACT_INVOICE_FIELD = "EDIFACT_INVOICE";

  @Mock
  private JobExecutionService jobExecutionService;
  @Mock
  private Vertx vertx;

  @InjectMocks
  private EdifactDiErrorPayloadBuilder payloadBuilder = new EdifactDiErrorPayloadBuilder(jobExecutionService);

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void checkEligible() {
    boolean eligible = payloadBuilder.isEligible(Record.RecordType.EDIFACT);
    assertTrue(eligible);
  }

  @Test
  public void checkNotEligible() {
    boolean eligible = payloadBuilder.isEligible(Record.RecordType.MARC_AUTHORITY);
    assertFalse(eligible);
  }

  @Test
  public void shouldBuildEventPayload(TestContext context) {
    Async async = context.async();
    JobExecution jobExecution = getJobExecution();
    when(jobExecutionService.getJobExecutionById(JOB_EXECUTION_ID, TENANT_ID))
      .thenReturn(Future.succeededFuture(Optional.of(jobExecution)));

    Future<DataImportEventPayload> payloadFuture;
    try(var mockedStatic = Mockito.mockStatic(MappingManager.class)) {
      mockedStatic.when(() -> MappingManager.map(any(DataImportEventPayload.class), any(MappingContext.class)))
        .thenReturn(new DataImportEventPayload().withContext(getPayloadContext()));
      payloadFuture = payloadBuilder.buildEventPayload(new RecordTooLargeException(LARGE_PAYLOAD_ERROR_MESSAGE),
        getOkapiParams(), JOB_EXECUTION_ID, new Record().withRecordType(Record.RecordType.EDIFACT));
    }

    payloadFuture.onComplete(ar -> {
      DataImportEventPayload result = ar.result();
      assertEquals(DI_ERROR.value(), result.getEventType());
      assertTrue(result.getContext().containsKey(INVOICE_FIELD));
      assertTrue(result.getContext().containsKey(INVOICE_LINES_FILED));
      assertTrue(result.getContext().containsKey(EDIFACT_INVOICE_FIELD));

      Record recordFromContext = getRecordFromContext(result);
      assertNull(recordFromContext.getParsedRecord());
      assertNull(recordFromContext.getRawRecord());

      async.complete();
    });
  }

  @Test
  public void shouldBuildEventPayloadWhenJobExecutionNotFound(TestContext context) {
    Async async = context.async();
    when(jobExecutionService.getJobExecutionById(JOB_EXECUTION_ID, TENANT_ID))
      .thenReturn(Future.succeededFuture(Optional.empty()));

    Future<DataImportEventPayload> payloadFuture;
    try(var mockedStatic = Mockito.mockStatic(MappingManager.class)) {
      mockedStatic.when(() -> MappingManager.map(any(DataImportEventPayload.class), any(MappingContext.class)))
        .thenReturn(new DataImportEventPayload().withContext(getPayloadContext()));
      payloadFuture = payloadBuilder.buildEventPayload(new RecordTooLargeException(LARGE_PAYLOAD_ERROR_MESSAGE),
        getOkapiParams(), JOB_EXECUTION_ID, new Record().withRecordType(Record.RecordType.EDIFACT));
    }

    payloadFuture.onComplete(ar -> {
      DataImportEventPayload result = ar.result();
      assertEquals(DI_ERROR.value(), result.getEventType());
      assertEquals(LARGE_PAYLOAD_ERROR_MESSAGE, result.getContext().get(ERROR_KEY));
      assertTrue(result.getContext().containsKey(EDIFACT_INVOICE_FIELD));

      Record recordFromContext = getRecordFromContext(result);
      assertNull(recordFromContext.getParsedRecord());
      assertNull(recordFromContext.getRawRecord());

      assertFalse(result.getContext().containsKey(INVOICE_FIELD));
      assertFalse(result.getContext().containsKey(INVOICE_LINES_FILED));

      async.complete();
    });
  }

  private OkapiConnectionParams getOkapiParams() {
    HashMap<String, String> headers = new HashMap<>();
    headers.put(OKAPI_URL_HEADER, "http://localhost");
    headers.put(OKAPI_TENANT_HEADER, TENANT_ID);
    headers.put(OKAPI_TOKEN_HEADER, TOKEN);
    return new OkapiConnectionParams(headers, vertx);
  }

  private HashMap<String, String> getPayloadContext() {
    HashMap<String, String> payloadContext = Maps.newHashMap();
    payloadContext.put(INVOICE.value(), new JsonObject()
      .put(EdifactDiErrorPayloadBuilder.INVOICE_FIELD, new JsonObject()
        .put(INVOICE_LINES_FIELD, new JsonArray().add(new JsonObject()))).encode());
    payloadContext.put(ERROR_KEY, LARGE_PAYLOAD_ERROR_MESSAGE);
    return payloadContext;

  }

  private JobExecution getJobExecution() {
    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withContentType(ACTION_PROFILE)
      .withChildSnapshotWrappers(Lists.newArrayList(new ProfileSnapshotWrapper().withContentType(MAPPING_PROFILE)));

    return new JobExecution()
      .withId(JOB_EXECUTION_ID)
      .withJobProfileSnapshotWrapper(profileSnapshotWrapper);
  }

  private Record getRecordFromContext(DataImportEventPayload eventPayload) {
    String recordStr = eventPayload.getContext().get(EntityType.EDIFACT_INVOICE.value());
    return Json.decodeValue(recordStr, Record.class);
  }
}
