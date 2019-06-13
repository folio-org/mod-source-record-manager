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
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamReader;
import org.marc4j.marc.Record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@RunWith(VertxUnitRunner.class)
public class MappingTest {

  private RecordToInstanceMapper mapper = RecordToInstanceMapperBuilder.buildMapper(RecordFormat.MARC);

  private static final String INSTANCES_PATH = "src/test/resources/org/folio/services/mapping/instances.json";
  private static final String BIBS_PATH = "src/test/resources/org/folio/services/mapping/CornellFOLIOExemplars_Bibs.mrc";

  @Test
  public void testMarcToInstance() throws IOException {
    MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(TestUtil.readFileFromPath(BIBS_PATH).getBytes(StandardCharsets.UTF_8)));
    JsonArray instances = new JsonArray(TestUtil.readFileFromPath(INSTANCES_PATH));
    int i = 0;
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(new String(os.toByteArray()));
      Instance instance = mapper.mapRecord(marc);
      Assert.assertEquals(JsonObject.mapFrom(instance).put("id", "0").encode(), instances.getJsonObject(i).encode());
      i++;
    }
  }
}
