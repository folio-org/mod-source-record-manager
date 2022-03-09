package org.folio.verticle.consumers.errorhandlers;

import com.google.common.collect.Lists;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import org.folio.dataimport.util.OkapiConnectionParams;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import org.folio.services.ChangeEngineServiceImpl;
import org.folio.services.JobExecutionService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class ParsedRecordsDiErrorProviderTest {
  private static final String JOB_EXECUTION_ID = UUID.randomUUID().toString();
  private static final String TENANT_ID = "diku";
  private static final String TOKEN = "token";

  @Mock
  private JobExecutionService jobExecutionService;
  @Mock
  private ChangeEngineServiceImpl changeEngineService;

  @InjectMocks
  private ParsedRecordsDiErrorProvider diErrorProvider = new ParsedRecordsDiErrorProvider(jobExecutionService, changeEngineService);

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void shouldParseInitialRecords(TestContext context) {
    Async async = context.async();

    JobExecution jobExecution = new JobExecution().withId(JOB_EXECUTION_ID);
    when(jobExecutionService.getJobExecutionById(JOB_EXECUTION_ID, TENANT_ID))
      .thenReturn(Future.succeededFuture(Optional.of(jobExecution)));
    RawRecordsDto rawRecordsDto = new RawRecordsDto()
      .withInitialRecords(Lists.newArrayList(new InitialRecord()))
      .withRecordsMetadata(new RecordsMetadata().withContentType(RecordsMetadata.ContentType.MARC_JSON));
    when(changeEngineService.getParsedRecordsFromInitialRecords(eq(rawRecordsDto.getInitialRecords()),
      eq(rawRecordsDto.getRecordsMetadata().getContentType()), eq(jobExecution), anyString()))
        .thenReturn(Lists.newArrayList(new Record().withParsedRecord(new ParsedRecord())));
    Future<List<Record>> parserRecordsFuture = diErrorProvider.getParsedRecordsFromInitialRecords(getOkapiParams(), JOB_EXECUTION_ID, rawRecordsDto);
    parserRecordsFuture.onComplete(ar -> {
      List<Record> parsedRecords = ar.result();
      assertEquals(1, parsedRecords.size());
      async.complete();
    });
  }

  @Test
  public void shouldReturnEmptyListWhenJobExecutionNotFound(TestContext context) {
    Async async = context.async();

    when(jobExecutionService.getJobExecutionById(JOB_EXECUTION_ID, TENANT_ID))
      .thenReturn(Future.succeededFuture(Optional.empty()));
    Future<List<Record>> parserRecordsFuture = diErrorProvider.getParsedRecordsFromInitialRecords(getOkapiParams(), JOB_EXECUTION_ID, new RawRecordsDto());
    parserRecordsFuture.onComplete(ar -> {
      List<Record> parsedRecords = ar.result();
      assertTrue(parsedRecords.isEmpty());
      async.complete();
    });
  }

  private OkapiConnectionParams getOkapiParams() {
    HashMap<String, String> headers = new HashMap<>();
    headers.put(OKAPI_URL_HEADER, "http://localhost");
    headers.put(OKAPI_TENANT_HEADER, TENANT_ID);
    headers.put(OKAPI_TOKEN_HEADER, TOKEN);
    return new OkapiConnectionParams(headers, mock(Vertx.class));
  }

}
