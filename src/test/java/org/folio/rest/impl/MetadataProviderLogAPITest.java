package org.folio.rest.impl;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * REST tests for MetadataProvider to manager Log entities
 */
@RunWith(VertxUnitRunner.class)
public class MetadataProviderLogAPITest extends AbstractMetadataProviderTest {

  private String servicePath = "/logs";

  @Test
  public void testGetLogs(TestContext context) {
    //TODO Replace testing stub
    String testUrl = baseServicePath + servicePath;
    getDefaultGiven()
      .param("query", "query")
      .param("landingPage", false)
      .when().get(testUrl)
      .then().statusCode(200);
  }

  @Override
  public void setUp(TestContext context) throws Exception {
    // TODO implement me
  }
}
