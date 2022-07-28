package org.folio.services;

import java.util.UUID;

import org.assertj.core.util.Lists;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.afterprocessing.HrIdFieldService;
import org.folio.services.afterprocessing.HrIdFieldServiceImpl;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class HrIdFieldServiceTest {

  @Test
  public void shouldAdd035FieldIf001FieldExists(){
    // given
    String parsedContent = "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00108nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"035\":{\"subfields\":[{\"a\":\"in001\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    // when
    HrIdFieldService hrIdFieldService = new HrIdFieldServiceImpl();
    hrIdFieldService.move001valueTo035Field(Lists.newArrayList(record));
    // then
    Assert.assertEquals(expectedParsedContent, record.getParsedRecord().getContent());
  }

  @Test
  public void shouldNotAdd035FieldIf001FieldNotExists(){
    // given
    String parsedContent = "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"003\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"003\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    // when
    HrIdFieldService hrIdFieldService = new HrIdFieldServiceImpl();
    hrIdFieldService.move001valueTo035Field(Lists.newArrayList(record));
    // then
    Assert.assertEquals(expectedParsedContent, record.getParsedRecord().getContent());
  }

  @Test
  public void shouldNotAdd035FieldIf035FieldExists(){
    // given
    String parsedContent = "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"in001\"},{\"035\":{\"subfields\":[{\"a\":\"(test)data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"in001\"},{\"035\":{\"subfields\":[{\"a\":\"(test)data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    // when
    HrIdFieldService hrIdFieldService = new HrIdFieldServiceImpl();
    hrIdFieldService.move001valueTo035Field(Lists.newArrayList(record));
    // then
    Assert.assertEquals(expectedParsedContent, record.getParsedRecord().getContent());
  }
}
