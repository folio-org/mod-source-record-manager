package schema;

import io.vertx.core.json.JsonObject;
import org.folio.TestUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import static java.lang.String.format;

public class InstanceSchemaTest {

  private static final String ACTUAL_INSTANCE_SCHEMA_PATH = "schema/instance.json";
  private static final String EXPECTED_INSTANCE_SCHEMA_URL = "https://raw.githubusercontent.com/folio-org/mod-inventory/master/ramls/instance.json";


  /**
   * This test is a kind of notification that the instance schema has been changed.
   * Changing the instance schema breaks file processing workflow (data-import), to avoid file processing failure
   * should update instance schema in mod-source-record-manager module.
   * The test checks for changes only in the instance schema file
   * and does not take into account changes in the schemas referenced by the instance schema.
   * To fix this test one should update instance schema to 'schema' directory in tests resources.
   */
  @Test
  public void instanceSchemaIsNotChanged() throws IOException {
    String expectedSchemaContent = getSchemaByUrl(EXPECTED_INSTANCE_SCHEMA_URL);

    URL expectedInstanceSchemaUrl = InstanceSchemaTest.class.getClassLoader().getResource(ACTUAL_INSTANCE_SCHEMA_PATH);
    Assert.assertNotNull(format("Schema file '%s' not found in test resources", ACTUAL_INSTANCE_SCHEMA_PATH), expectedInstanceSchemaUrl);
    String actualSchemaContent = TestUtil.readFileFromPath((expectedInstanceSchemaUrl.getFile()));

    JsonObject expectedSchemaAsJson = new JsonObject(expectedSchemaContent);
    JsonObject actualSchemaAsJson = new JsonObject(actualSchemaContent);
    Assert.assertEquals(expectedSchemaAsJson, actualSchemaAsJson);
  }

  private String getSchemaByUrl(String url) throws IOException {
    URL schemaUrl = new URL(url);
    try (BufferedReader in = new BufferedReader(new InputStreamReader(schemaUrl.openStream()))) {
      StringBuilder sb = new StringBuilder();

      String inputLine;
      while ((inputLine = in.readLine()) != null) {
        sb.append(inputLine);
      }
      return sb.toString();
    }
  }

}
