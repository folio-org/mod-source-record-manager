package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.TestUtil;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class MappingRulesSnapshotDaoImplTest extends AbstractRestTest {

  private static final String MARC_BIB_RULES_PATH = "src/test/resources/org/folio/services/marc_bib_rules.json";

  @Spy
  private PostgresClientFactory postgresClientFactory = new PostgresClientFactory(Vertx.vertx());

  @InjectMocks
  private MappingRulesSnapshotDao mappingRulesSnapshotDao = new MappingRulesSnapshotDaoImpl();

  private AutoCloseable mocksCloseable;
  private JsonObject mappingRules;

  @Before
  public void setUp(TestContext context) throws IOException {
    this.mocksCloseable = MockitoAnnotations.openMocks(this);
    super.setUp(context);
    mappingRules = new JsonObject(TestUtil.readFileFromPath(MARC_BIB_RULES_PATH));
  }

  @Test
  public void shouldReturnSucceededFutureOnSnapshotSaveWhenMappingRulesSnapshotWithSameJobIdExists(TestContext context) {
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExecution = createdJobExecutions.get(0);

    Async async = context.async();
    Future<String> future = mappingRulesSnapshotDao.save(mappingRules, jobExecution.getId(), TENANT_ID)
      .compose(v -> mappingRulesSnapshotDao.save(mappingRules, jobExecution.getId(), TENANT_ID));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(jobExecution.getId(), ar.result());
      async.complete();
    });
  }

  @After
  public void tearDown() throws Exception {
    mocksCloseable.close();
  }

}
