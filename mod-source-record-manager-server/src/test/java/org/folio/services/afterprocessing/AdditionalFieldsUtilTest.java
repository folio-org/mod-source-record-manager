package org.folio.services.afterprocessing;


import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

@RunWith(BlockJUnit4ClassRunner.class)
public class AdditionalFieldsUtilTest {
  private static final String PARSED_RECORD_PATH = "src/test/resources/org/folio/services/afterprocessing/parsedRecord.json";
  private AdditionalFieldsUtil additionalFieldsUtil = new AdditionalFieldsUtil();

  @Test
  public void shouldAddInstanceIdSubfield() throws IOException {
    // given
    String recordId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();
    RecordProcessingContext recordProcessingContext = new RecordProcessingContext(Record.RecordType.MARC);
    recordProcessingContext.addRecordContext(recordId, instanceId);

    String parsedRecordContent = FileUtils.readFileToString(new File(PARSED_RECORD_PATH), "UTF-8");
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedRecordContent);
    Record record = new Record().withId(recordId).withParsedRecord(parsedRecord);
    // when
    boolean added = additionalFieldsUtil.addInstanceIdToMarcRecord(record, instanceId);
    // then
    Assert.assertTrue(added);
    JsonObject content = new JsonObject((String) record.getParsedRecord().getContent());
    JsonArray fields = content.getJsonArray("fields");
    Assert.assertTrue(!fields.isEmpty());
    for (int i = fields.size(); i-- > 0; ) {
      JsonObject targetField = fields.getJsonObject(i);
      if (targetField.containsKey(AdditionalFieldsConfig.TAG_999)) {
        JsonArray subfields = targetField.getJsonObject(AdditionalFieldsConfig.TAG_999).getJsonArray("subfields");
        for (int j = subfields.size(); j-- > 0; ) {
          JsonObject targetSubfield = subfields.getJsonObject(j);
          if (targetSubfield.containsKey("i")) {
            String actualInstanceId = (String) targetSubfield.getValue("i");
            Assert.assertEquals(instanceId, actualInstanceId);
          }
        }
      }
    }
  }

  @Test
  public void shouldNotAddInstanceIdSubfieldIfNoParsedRecordContent() {
    // given
    Record record = new Record();
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = additionalFieldsUtil.addInstanceIdToMarcRecord(record, instanceId);
    // then
    Assert.assertFalse(added);
    Assert.assertNull(record.getParsedRecord());
  }

  @Test
  public void shouldNotAddInstanceIdSubfieldIfNoFieldsInParsedRecordContent() {
    // given
    Record record = new Record();
    record.setParsedRecord(new ParsedRecord().withContent(StringUtils.EMPTY));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = additionalFieldsUtil.addInstanceIdToMarcRecord(record, instanceId);
    // then
    Assert.assertFalse(added);
    Assert.assertNotNull(record.getParsedRecord());
    Assert.assertNotNull(record.getParsedRecord().getContent());
    Assert.assertEquals(StringUtils.EMPTY, record.getParsedRecord().getContent());
  }
}
