package org.folio.services;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.TestUtil;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.services.mappers.RecordToInstanceMapper;
import org.folio.services.mappers.RecordToInstanceMapperBuilder;
import org.folio.services.mappers.processor.parameters.MappingParameters;
import org.folio.services.parsers.RecordFormat;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamReader;
import org.marc4j.marc.Record;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;

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
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    while (reader.hasNext()) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      MarcJsonWriter writer = new MarcJsonWriter(os);
      Record record = reader.next();
      writer.write(record);
      JsonObject marc = new JsonObject(new String(os.toByteArray()));
      Instance instance = mapper.mapRecord(marc, new MappingParameters());
      Assert.assertNotNull(instance.getTitle());
      Assert.assertNotNull(instance.getSource());
      Assert.assertNotNull(instance.getInstanceTypeId());
      Validator validator = factory.getValidator();
      Set<ConstraintViolation<Instance>> violations = validator.validate(instance);
      Assert.assertTrue(violations.isEmpty());
      Assert.assertEquals(JsonObject.mapFrom(instance).put("id", "0").encode(), instances.getJsonObject(i).encode());
      i++;
    }
  }
}
