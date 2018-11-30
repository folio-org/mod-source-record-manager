package org.folio.rest.impl.metadataProvider;

import io.restassured.RestAssured;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.impl.AbstractRestTest;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * REST tests for MetadataProvider to manager Log entities
 */
@RunWith(VertxUnitRunner.class)
public class MetadataProviderLogAPITest extends AbstractRestTest {

  private static final String GET_LOGS_PATH = "/metadata-provider/logs";

  @Test
  public void testGetLogs(TestContext context) {
    //TODO Replace testing stub
  }

}
