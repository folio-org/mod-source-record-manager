package org.folio.dao;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.IncomingRecord;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class IncomingRecordDaoImplTest extends AbstractRestTest {

  private static final String TENANT_ID = "diku";

  @Spy
  private PostgresClientFactory postgresClientFactory = new PostgresClientFactory(Vertx.vertx());
  @InjectMocks
  private IncomingRecordDao incomingRecordDao = new IncomingRecordDaoImpl();

  @Before
  public void setUp(TestContext context) throws IOException {
    MockitoAnnotations.openMocks(this);
    super.setUp(context);
  }

  @Test
  public void saveBatch(TestContext context) {
    Async async = context.async();

    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    final String jobExecutionId = createdJobExecutions.get(0).getId();

    String id1 = UUID.randomUUID().toString();
    String id2 = UUID.randomUUID().toString();
    IncomingRecord incomingRecord1 = new IncomingRecord()
      .withId(id1).withJobExecutionId(jobExecutionId).withRecordType(IncomingRecord.RecordType.MARC_BIB).withOrder(0)
      .withRawRecordContent("rawRecord").withParsedRecordContent("parsedRecord");
    IncomingRecord incomingRecord2 = new IncomingRecord()
      .withId(id2).withJobExecutionId(jobExecutionId).withRecordType(IncomingRecord.RecordType.MARC_BIB).withOrder(0)
      .withRawRecordContent("rawRecord").withParsedRecordContent("parsedRecord");

    incomingRecordDao.saveBatch(List.of(incomingRecord1, incomingRecord2), TENANT_ID)
      .onComplete(ar -> {
        context.assertTrue(ar.succeeded());
        context.assertEquals(2, ar.result().size());
        async.complete();
      });
  }
}
