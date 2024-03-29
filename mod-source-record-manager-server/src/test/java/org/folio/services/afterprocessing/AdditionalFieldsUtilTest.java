package org.folio.services.afterprocessing;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.TestUtil;
import org.folio.okapi.common.MetricsUtil;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.marc4j.MarcException;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static org.folio.TestUtil.recordsHaveSameOrder;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.INDICATOR;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.SUBFIELD_I;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.SUBFIELD_S;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.TAG_999;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.addControlledFieldToMarcRecord;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.addDataFieldToMarcRecord;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.addFieldToMarcRecord;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.getControlFieldValue;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.isFieldExist;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.modifyDataFieldsForMarcRecord;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.removeField;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.getCacheStats;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.getValue;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

@RunWith(BlockJUnit4ClassRunner.class)
public class AdditionalFieldsUtilTest {

  private static final String PARSED_RECORD_PATH = "src/test/resources/org/folio/services/afterprocessing/parsedRecord.json";
  private static final String REORDERED_PARSED_RECORD_PATH = "src/test/resources/org/folio/services/afterprocessing/reorderedParsedRecord.json";
  private static final String REORDERING_RESULT_RECORD = "src/test/resources/org/folio/services/parsedRecords/reorderingResultRecord.json";
  private static final String PARSED_RECORD = "src/test/resources/org/folio/services/parsedRecords/parsedRecord.json";
  private static final String REORDERED_PARSED_RECORD = "src/test/resources/org/folio/services/parsedRecords/reorderedParsedRecord.json";

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("vertx.metrics.options.enabled", "true");
    System.setProperty("jmxMetricsOptions", "{\"domain\":\"org.folio\"}");
    MetricsUtil.init(new VertxOptions());
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("vertx.metrics.options.enabled");
    System.clearProperty("jmxMetricsOptions");
  }

  @After
  public void after() {
    AdditionalFieldsUtil.clearCache();
  }

  @Test
  public void shouldAddInstanceIdSubfield() throws IOException {
    // given
    String recordId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();

    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    parsedRecord.setContent(parsedRecordContent);
    Record record = new Record().withId(recordId).withParsedRecord(parsedRecord);
    // when
    boolean addedSourceRecordId = addFieldToMarcRecord(record, TAG_999, SUBFIELD_S, recordId);
    boolean addedInstanceId = addFieldToMarcRecord(record, TAG_999, SUBFIELD_I, instanceId);
    // then
    Assert.assertTrue(addedSourceRecordId);
    Assert.assertTrue(addedInstanceId);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assert.assertNotEquals(leader, newLeader);
    Assert.assertFalse(fields.isEmpty());
    int totalFieldsCount = 0;
    for (int i = fields.size(); i-- > 0; ) {
      JsonObject targetField = fields.getJsonObject(i);
      if (targetField.containsKey(TAG_999)) {
        JsonArray subfields = targetField.getJsonObject(TAG_999).getJsonArray("subfields");
        for (int j = subfields.size(); j-- > 0; ) {
          JsonObject targetSubfield = subfields.getJsonObject(j);
          if (targetSubfield.containsKey("i")) {
            String actualInstanceId = (String) targetSubfield.getValue("i");
            assertEquals(instanceId, actualInstanceId);
          }
          if (targetSubfield.containsKey("s")) {
            String actualSourceRecordId = (String) targetSubfield.getValue("s");
            assertEquals(recordId, actualSourceRecordId);
          }
        }
        totalFieldsCount++;
      }
    }
    assertEquals(2, totalFieldsCount);
  }

  @Test
  public void shouldReorderParsedRecordContentAfterAddingField() throws IOException {
    // given
    String instanceId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(REORDERED_PARSED_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    parsedRecord.setContent(parsedRecordContent);
    Record record = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    var baseRecord = record.getParsedRecord().getContent().toString();
    // when
    boolean added = addFieldToMarcRecord(record, TAG_999, SUBFIELD_I, instanceId);
    // then
    assertTrue(recordsHaveSameOrder(baseRecord, record.getParsedRecord().getContent().toString()));
    assertTrue(added);
    JsonObject content = new JsonObject(record.getParsedRecord().getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assert.assertNotEquals(leader, newLeader);
    Assert.assertFalse(fields.isEmpty());
    boolean existsNewField = false;
    for (int i = 0; i < fields.size(); i++) {
      JsonObject targetField = fields.getJsonObject(i);
      if (targetField.containsKey(TAG_999)) {
        existsNewField = true;
        String currentTag = fields.getJsonObject(i-1).stream().map(Map.Entry::getKey).findFirst().get();
        String nextTag = fields.getJsonObject(i).stream().map(Map.Entry::getKey).findFirst().get();
        Assert.assertThat(currentTag, lessThanOrEqualTo(nextTag));
      }
    }
    assertTrue(existsNewField);
  }

  @Test
  public void shouldNotAddInstanceIdSubfieldIfNoParsedRecordContent() {
    // given
    Record record = new Record();
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = addFieldToMarcRecord(record, TAG_999, SUBFIELD_I, instanceId);
    // then
    Assert.assertFalse(added);
    Assert.assertNull(record.getParsedRecord());
  }

  @Test
  public void shouldReturnNullIfFieldNotFound() {
    // given
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent("{}"));
    // when
    String value = getControlFieldValue(record, "001");
    // then
    Assert.assertNull(value);
  }

  @Test
  public void shouldReturnNullIfFailedToParse() {
    // given
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent("{fake-record}"));
    // when
    String value = getControlFieldValue(record, "001");
    // then
    Assert.assertNull(value);
  }

  @Test
  public void shouldNotAddInstanceIdSubfieldIfNoFieldsInParsedRecordContent() {
    // given
    Record record = new Record();
    String content = StringUtils.EMPTY;
    record.setParsedRecord(new ParsedRecord().withContent(content));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = addFieldToMarcRecord(record, TAG_999, SUBFIELD_I, instanceId);
    // then
    Assert.assertFalse(added);
    Assert.assertNotNull(record.getParsedRecord());
    Assert.assertNotNull(record.getParsedRecord().getContent());
    assertEquals(content, record.getParsedRecord().getContent());
  }

  @Test
  public void shouldNotAddInstanceIdSubfieldIfCanNotConvertParsedContentToJsonObject() {
    // given
    Record record = new Record();
    String content = "{fields}";
    record.setParsedRecord(new ParsedRecord().withContent(content));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = addFieldToMarcRecord(record, TAG_999, SUBFIELD_I, instanceId);
    // then
    Assert.assertFalse(added);
    Assert.assertNotNull(record.getParsedRecord());
    Assert.assertNotNull(record.getParsedRecord().getContent());
    assertEquals(content, record.getParsedRecord().getContent());
  }

  @Test
  public void shouldNotAddInstanceIdSubfieldIfContentHasNoFields() {
    // given
    Record record = new Record();
    String content = "{\"leader\":\"01240cas a2200397\"}";
    record.setParsedRecord(new ParsedRecord().withContent(content));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = addFieldToMarcRecord(record, TAG_999, SUBFIELD_I, instanceId);
    // then
    Assert.assertFalse(added);
    Assert.assertNotNull(record.getParsedRecord());
    Assert.assertNotNull(record.getParsedRecord().getContent());
  }

  @Test
  public void shouldAddDatafieldIfContentHasNoFields() throws IOException {
    // given
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedRecordContent);
    Record record = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    String instanceId = UUID.randomUUID().toString();
    boolean added = false;
    if (!isFieldExist(record, "035", 'a', instanceId)) {
      added = addDataFieldToMarcRecord(record, "035", ' ', ' ', 'a', instanceId);
    }
    // when
    // then
    Assert.assertTrue(added);
    Assert.assertTrue(isFieldExist(record, "035", 'a', instanceId));
    Assert.assertTrue(isFieldExist(record, "001", 'a', "ybp7406411"));
    Assert.assertNotNull(record.getParsedRecord());
    Assert.assertNotNull(record.getParsedRecord().getContent());
  }

  @Test
  public void shouldNotAddInstanceIdSubfieldIfContentIsNull() {
    // given
    Record record = new Record();
    record.setParsedRecord(new ParsedRecord().withContent(null));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = addFieldToMarcRecord(record, TAG_999, SUBFIELD_I, instanceId);
    // then
    Assert.assertFalse(added);
    Assert.assertNotNull(record.getParsedRecord());
    Assert.assertNull(record.getParsedRecord().getContent());
  }

  @Test
  public void shouldRemoveField() throws IOException {
    String recordId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    parsedRecord.setContent(parsedRecordContent);
    Record record = new Record().withId(recordId).withParsedRecord(parsedRecord);
    boolean deleted = removeField(record, "001");
    Assert.assertTrue(deleted);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assert.assertNotEquals(leader, newLeader);
    Assert.assertFalse(fields.isEmpty());
    for (int i = 0; i < fields.size(); i++) {
      JsonObject targetField = fields.getJsonObject(i);
      if (targetField.containsKey("001")) {
        Assert.fail();
      }
    }
  }

  @Test
  public void shouldAddControlledFieldToMarcRecord() throws IOException {
    String recordId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    parsedRecord.setContent(parsedRecordContent);
    Record record = new Record().withId(recordId).withParsedRecord(parsedRecord);
    boolean added = addControlledFieldToMarcRecord(record, "002", "test");
    Assert.assertTrue(added);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assert.assertNotEquals(leader, newLeader);
    Assert.assertFalse(fields.isEmpty());
    boolean passed = false;
    for (int i = 0; i < fields.size(); i++) {
      JsonObject targetField = fields.getJsonObject(i);
      if (targetField.containsKey("002") && targetField.getString("002").equals("test")) {
        passed = true;
        break;
      }
    }
    Assert.assertTrue(passed);
  }

  @Test
  public void shouldAddFieldToMarcRecordInNumericalOrder() throws IOException {
    // given
    String instanceHrId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    parsedRecord.setContent(parsedRecordContent);
    Record record = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    // when
    boolean added = addDataFieldToMarcRecord(record, "035", ' ', ' ', 'a', instanceHrId);
    // then
    Assert.assertTrue(added);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assert.assertNotEquals(leader, newLeader);
    Assert.assertFalse(fields.isEmpty());
    boolean existsNewField = false;
    for (int i = 0; i < fields.size() - 1; i++) {
      JsonObject targetField = fields.getJsonObject(i);
      if (targetField.containsKey("035")) {
        existsNewField = true;
        String currentTag = fields.getJsonObject(i).stream().map(Map.Entry::getKey).findFirst().get();
        String nextTag = fields.getJsonObject(i + 1).stream().map(Map.Entry::getKey).findFirst().get();
        Assert.assertThat(currentTag, lessThanOrEqualTo(nextTag));
      }
    }
    Assert.assertTrue(existsNewField);
  }

  @Test
  public void shouldNotSortExistingFieldsWhenAddFieldToToMarcRecord() {
    // given
    String instanceId = "12345";
    String parsedContent = "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00113nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"i\":\"12345\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);
    Record record = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    // when
    boolean added = addDataFieldToMarcRecord(record, TAG_999, INDICATOR, INDICATOR, SUBFIELD_I, instanceId);
    // then
    Assert.assertTrue(added);
    assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  public void shouldUpdateFieldsWithProvidedAction() {
    // given
    String parsedContent = "{\"leader\":\"00115nam  22000731a 4500\"," +
      "\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"501\":{\"subfields\":[{\"a\":\"data\"},{\"9\":\"remove\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"507\":{\"subfields\":[{\"a\":\"data\"},{\"9\":\"keep\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00118nam  22000731a 4500\"," +
      "\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"501\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"507\":{\"subfields\":[{\"a\":\"data\"},{\"9\":\"keep\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    List<String> tagsForAction = List.of("500", "501");
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);
    Record record = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    // when
    boolean added = modifyDataFieldsForMarcRecord(record, tagsForAction, dataField -> {
      var subfields9 = dataField.getSubfields().stream()
        .filter(subfield -> '9' == subfield.getCode())
        .collect(Collectors.toList());
      subfields9.forEach(dataField::removeSubfield);
    });
    // then
    Assert.assertTrue(added);
    assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Ignore("Test doesn't pass when AdditionalFieldsUtil is initialized without metrics enabled." +
    "This is because other tests class can start without metrics enabled.")
  @Test
  public void caching() throws IOException {
    // given
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedRecordContent);
    Record record = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    String instanceId = UUID.randomUUID().toString();

    CacheStats initialCacheStats = getCacheStats();

    // record with null parsed content
    Assert.assertFalse(
        isFieldExist(new Record().withId(UUID.randomUUID().toString()), "035", 'a', instanceId));
    CacheStats cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(0, cacheStats.hitCount());
    assertEquals(0, cacheStats.missCount());
    assertEquals(0, cacheStats.loadCount());
    // record with empty parsed content
    Assert.assertFalse(
        isFieldExist(
            new Record()
                .withId(UUID.randomUUID().toString())
                .withParsedRecord(new ParsedRecord().withContent("")),
            "035",
            'a',
            instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(0, cacheStats.requestCount());
    assertEquals(0, cacheStats.hitCount());
    assertEquals(0, cacheStats.missCount());
    assertEquals(0, cacheStats.loadCount());
    // record with bad parsed content
    Assert.assertFalse(
      isFieldExist(
        new Record()
          .withId(UUID.randomUUID().toString())
          .withParsedRecord(new ParsedRecord().withContent("test")),
        "035",
        'a',
        instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(1, cacheStats.requestCount());
    assertEquals(0, cacheStats.hitCount());
    assertEquals(1, cacheStats.missCount());
    assertEquals(1, cacheStats.loadCount());
    // does field exists?
    Assert.assertFalse(isFieldExist(record, "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(2, cacheStats.requestCount());
    assertEquals(0, cacheStats.hitCount());
    assertEquals(2, cacheStats.missCount());
    assertEquals(2, cacheStats.loadCount());
    // update field
    addDataFieldToMarcRecord(record, "035", ' ', ' ', 'a', instanceId);
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(3, cacheStats.requestCount());
    assertEquals(1, cacheStats.hitCount());
    assertEquals(2, cacheStats.missCount());
    assertEquals(2, cacheStats.loadCount());
    // verify that field exists
    Assert.assertTrue(isFieldExist(record, "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(4, cacheStats.requestCount());
    assertEquals(2, cacheStats.hitCount());
    assertEquals(2, cacheStats.missCount());
    assertEquals(2, cacheStats.loadCount());
    // verify that field exists again
    Assert.assertTrue(isFieldExist(record, "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(5, cacheStats.requestCount());
    assertEquals(3, cacheStats.hitCount());
    assertEquals(2, cacheStats.missCount());
    assertEquals(2, cacheStats.loadCount());
    // get the field
    assertEquals(instanceId, getValue(record, "035",  'a'));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(6, cacheStats.requestCount());
    assertEquals(4, cacheStats.hitCount());
    assertEquals(2, cacheStats.missCount());
    assertEquals(2, cacheStats.loadCount());
    // get the control field
    assertEquals(null, getControlFieldValue(record, "035"));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(7, cacheStats.requestCount());
    assertEquals(5, cacheStats.hitCount());
    assertEquals(2, cacheStats.missCount());
    assertEquals(2, cacheStats.loadCount());
    // remove the field
    Assert.assertTrue(removeField(record, "035"));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(8, cacheStats.requestCount());
    assertEquals(6, cacheStats.hitCount());
    assertEquals(2, cacheStats.missCount());
    assertEquals(2, cacheStats.loadCount());
  }

  @Test
  public void shouldReorderMarcRecordFields() throws IOException, MarcException {
    var reorderedRecordContent = readFileFromPath(PARSED_RECORD);
    var sourceRecordContent = readFileFromPath(REORDERED_PARSED_RECORD);
    var reorderingResultRecord = readFileFromPath(REORDERING_RESULT_RECORD);

    var resultContent = AdditionalFieldsUtil.reorderMarcRecordFields(sourceRecordContent, reorderedRecordContent);

    assertNotNull(resultContent);
    assertEquals(formatContent(resultContent), formatContent(reorderingResultRecord));
  }

  private static String readFileFromPath(String path) throws IOException {
    return new String(FileUtils.readFileToByteArray(new File(path)));
  }

  private String formatContent(String content) {
    return content.replaceAll("\\s", "");
  }
}
