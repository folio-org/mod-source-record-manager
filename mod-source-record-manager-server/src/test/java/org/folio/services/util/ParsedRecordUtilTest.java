package org.folio.services.util;

import static org.junit.Assert.assertEquals;

import java.util.List;

import io.vertx.core.json.JsonObject;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

@RunWith(BlockJUnit4ClassRunner.class)
public class ParsedRecordUtilTest {

  private static final String PARSED_CONTENT =
    "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"150\":{\"ind1\":\" \",\"ind2\":\" \","
      + "\"subfields\":[{\"a\":\"Black panther\"},{\"b\":\"comics\"}]}}]}";

  @Test
  public void shouldRetrieveDataWhenContentIsJsonString() {
    var parsedRecord = new ParsedRecord().withContent(PARSED_CONTENT);

    assertEquals("Black panther comics", ParsedRecordUtil.retrieveDataByField(parsedRecord, "150"));
  }

  @Test
  public void shouldRetrieveDataWhenContentIsMap() {
    var parsedRecord = new ParsedRecord().withContent(new JsonObject(PARSED_CONTENT).getMap());

    assertEquals("Black panther comics", ParsedRecordUtil.retrieveDataByField(parsedRecord, "150"));
  }

  @Test
  public void shouldRetrieveDataFromRequestedSubfieldsWhenContentIsMap() {
    var parsedRecord = new ParsedRecord().withContent(new JsonObject(PARSED_CONTENT).getMap());

    assertEquals("Black panther", ParsedRecordUtil.retrieveDataByField(parsedRecord, "150", List.of("a")));
  }

  @Test
  public void shouldReturnEmptyDataWhenContentIsNull() {
    var parsedRecord = new ParsedRecord();

    assertEquals("", ParsedRecordUtil.retrieveDataByField(parsedRecord, "150"));
  }
}
