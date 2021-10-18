package org.folio.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class MappingParamsSnapshotDaoImplTest extends AbstractRestTest {

  private static final String MARC_PARAMS_PATH = "src/test/resources/org/folio/services/marc_mapping_params.json";

  @Spy
  private PostgresClientFactory postgresClientFactory = new PostgresClientFactory(Vertx.vertx());

  @InjectMocks
  private MappingParamsSnapshotDao mappingParamsSnapshotDao = new MappingParamsSnapshotDaoImpl();

  private MappingParameters mappingParameters;

  @Before
  public void setUp(TestContext context) throws IOException {
    MockitoAnnotations.initMocks(this);
    super.setUp(context);
    mappingParameters = new ObjectMapper().readValue(new File(MARC_PARAMS_PATH), MappingParameters.class);
  }

  @Test
  public void shouldReturnSucceededFutureWhenMappingParametersSnapshotWithSameJobIdExists(TestContext context) {
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExecution = createdJobExecutions.get(0);

    Async async = context.async();
    Future<String> future = mappingParamsSnapshotDao.save(mappingParameters, jobExecution.getId(), TENANT_ID)
      .compose(v -> mappingParamsSnapshotDao.save(mappingParameters, jobExecution.getId(), TENANT_ID));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(jobExecution.getId(), ar.result());
      async.complete();
    });
  }

}
