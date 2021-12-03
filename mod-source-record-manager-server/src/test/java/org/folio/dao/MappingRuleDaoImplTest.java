package org.folio.dao;


import java.io.IOException;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.impl.AbstractRestTest;

@RunWith(VertxUnitRunner.class)
public class MappingRuleDaoImplTest extends AbstractRestTest {

  @Spy
  private PostgresClientFactory postgresClientFactory = new PostgresClientFactory(Vertx.vertx());

  @InjectMocks
  private MappingRuleDao mappingRuleDao = new MappingRuleDaoImpl();

  private AutoCloseable mocks;

  @Before
  public void setUp(TestContext context) throws IOException {
    mocks = MockitoAnnotations.openMocks(this);
    super.setUp(context);
  }

  @After
  public void tearDown() throws Exception {
    mocks.close();
  }

  @Test
  public void testReturnEmptyRulesIfRecordTypeIsNull(TestContext testContext) {
    Async async = testContext.async();

    var future = mappingRuleDao.get(null, TENANT_ID);

    future.onComplete(ar -> {
      testContext.verify(v -> {
        Assert.assertTrue(ar.succeeded());
        Assert.assertTrue(ar.result().isEmpty());
      });
      async.complete();
    });
  }
}
