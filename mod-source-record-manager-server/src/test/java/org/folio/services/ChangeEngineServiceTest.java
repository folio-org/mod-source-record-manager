package org.folio.services;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.drools.core.util.StringUtils;
import org.folio.HttpStatus;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_TOKEN_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class ChangeEngineServiceTest {

  private static final String SOURCE_STORAGE_SERVICE = "/source-storage/records";
  private static final String TENANT = "diku";
  private static final String TOKEN = "token";
  private static final String RAW_RECORD = "01535nam  2200313 a 450000100140000000300080001400500170002200600190003900" +
    "7001500058008004100073020004800114020004500162040002100207050001500228082001900243245006900262260003600331300003" +
    "9003675000078004065200356004845880093008406000036009336000045009696510051010146510037010657000036011027100041011" +
    "38856004201179\u001EEDZ0000082802\u001EStDuBDS\u001E20120619170414.0\u001Em||||||||d||||||||\u001Ecr|||||||||||" +
    "|\u001E120426c20179999enk x d|o|    0|||a2eng|d\u001E  \u001Fa9781780934648 (online resource) :\u001FcNo price" +
    "\u001E  \u001Fa1780934645 (online resource) :\u001FcNo price\u001E  \u001FaStDuBDS\u001FcStDuBDS\u001E 4\u001Fa" +
    "DA566.9.C5\u001E04\u001Fa941.082092\u001F223\u001E00\u001FaChurchill archive\u001Fh[electronic resource] " +
    ":\u001Fba window on history.\u001E  \u001Fa[London] :\u001FbBloomsbury,\u001Fcc2017-\u001E  \u001Fa1 online reso" +
    "urce :\u001Fbillustrations\u001E  \u001FaPrepared by the archivists at the Churchill Archives Centre in Cambridg" +
    "e.\u001E8 \u001Fa'The Churchill Archive' is a massive resource that brings together online nearly a million docu" +
    "ments amassed by Winston Churchill through out his lifetime, including hand-written notes and private letters. I" +
    "t will also offer an expanding range of additional materials - pedagogical resources and secondary materials, vi" +
    "deo and audio content, and more.\u001E  \u001FaDescription based on online resource; title from home page (viewe" +
    "d on December 6, 2017).\u001E10\u001FaChurchill, Winston,\u001Fd1874-1965.\u001E10\u001FaChurchill, Winston" +
    ",\u001Fd1874-1965\u001FvSources.\u001E 0\u001FaGreat Britain\u001FxHistory\u001Fy20th century\u001FvSources." +
    "\u001E 0\u001FaGreat Britain\u001FxHistory\u001FvSources.\u001E1 \u001FaChurchill, Winston,\u001Fd1874-1965." +
    "\u001E2 \u001FaChurchill College.\u001FbArchives Centre.\u001E40\u001F" +
    "uhttp://www.churchillarchive.com/index\u001E\u001D";
  @Rule
  public WireMockRule mockServer =
    new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort().notifier(new Slf4jNotifier(true)));
  private Vertx vertx = Vertx.vertx();
  private Map<String, String> headers = new HashMap<>();

  private HttpClient httpClient = Mockito.spy(Vertx.vertx().createHttpClient());
  private JobExecutionSourceChunkDao jobExecutionSourceChunkDao = Mockito.mock(JobExecutionSourceChunkDao.class);
  private JobExecutionService jobExecutionService = Mockito.mock(JobExecutionService.class);
  private AdditionalFieldsConfig fieldsConfig = new AdditionalFieldsConfig();
  private ChangeEngineService changeEngineService =
    new ChangeEngineServiceImpl(jobExecutionSourceChunkDao, jobExecutionService, fieldsConfig);

  @Before
  public void setUp() {
    headers.put(OKAPI_URL_HEADER, "http://localhost:" + mockServer.port());
    headers.put(OKAPI_TENANT_HEADER, TENANT);
    headers.put(OKAPI_TOKEN_HEADER, TOKEN);
  }

  @Test
  public void shouldCreate999Field(TestContext context) {
    Async async = context.async();
    // given
    RawRecordsDto rawRecords = new RawRecordsDto();
    rawRecords.setRecords(Arrays.asList(RAW_RECORD));
    rawRecords.setCounter(1);
    rawRecords.setLast(true);
    JobExecutionSourceChunk chunk = new JobExecutionSourceChunk().withProcessedAmount(0);
    JobExecution job = new JobExecution();
    job.setJobProfileInfo(new JobProfileInfo().withDataType(JobProfileInfo.DataType.MARC));
    String sourceChunkId = StringUtils.EMPTY;
    WireMock.stubFor(WireMock.post(SOURCE_STORAGE_SERVICE).willReturn(WireMock.ok().withStatus(HttpStatus.HTTP_CREATED.toInt())));
    // when
    when(jobExecutionSourceChunkDao.getById(anyString(), anyString())).thenReturn(Future.succeededFuture(Optional.of(chunk)));
    when(jobExecutionSourceChunkDao.update(any(), anyString())).thenReturn(Future.succeededFuture(chunk));
    Future<List<Record>> future =
      changeEngineService.parseRawRecordsChunkForJobExecution(rawRecords, job, sourceChunkId, new OkapiConnectionParams(headers, vertx));
    // then
    future.setHandler(ar -> {
      context.assertTrue(ar.succeeded());
      List<Record> records = ar.result();
      context.assertEquals(1, records.size());
      Record record = ar.result().get(0);
      JsonObject parsedContent = new JsonObject(record.getParsedRecord().getContent().toString());
      JsonArray fields = parsedContent.getJsonArray("fields");
      for (Object field : fields.getList()) {
        Map fieldMap = (Map)field;
        if (fieldMap.containsKey(AdditionalFieldsConfig.TAG_999)) {
          async.complete();
        }
      }
    });
  }
}
