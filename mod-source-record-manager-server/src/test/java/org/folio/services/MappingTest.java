package org.folio.services;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.TestUtil;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.services.mappers.RecordToInstanceMapper;
import org.folio.services.mappers.RecordToInstanceMapperBuilder;
import org.folio.services.parsers.RecordFormat;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

@RunWith(VertxUnitRunner.class)
public class MappingTest {

  private RecordToInstanceMapper mapper = RecordToInstanceMapperBuilder.buildMapper(RecordFormat.MARC);

  protected static final String INSTANCES_PATH = "src/test/resources/org/folio/services/mapping/instances.json";
  protected static final String MARCS_PATH = "src/test/resources/org/folio/services/mapping/marcs.json";

  @Test
  public void testMarcToInstance() throws IOException {
    JsonArray marcs = new JsonArray(TestUtil.readFileFromPath(MARCS_PATH));
    JsonArray instances = new JsonArray(TestUtil.readFileFromPath(INSTANCES_PATH));
    Instance instance = mapper.mapRecord(marcs.getJsonObject(0));
    Assert.assertEquals(JsonObject.mapFrom(instance).put("id", "0").encode(), instances.getJsonObject(0).put("id", "0").encode());
    instance = mapper.mapRecord(marcs.getJsonObject(1));
    Assert.assertEquals(JsonObject.mapFrom(instance).put("id", "0").encode(), instances.getJsonObject(1).put("id", "0").encode());
  }
}
