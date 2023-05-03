package org.folio.services;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.folio.LinkingRuleDto;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.afterprocessing.FieldModificationServiceImpl;
import org.folio.services.mappers.processor.MappingParametersProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FieldModificationServiceTest {

  private static final String PARSED_CONTENT = "{\"leader\":\"00115nam  22000731a 4500\"," +
    "\"fields\":[{\"001\":\"ybp7406411\"}," +
    "{\"500\":{\"subfields\":[{\"a\":\"data\"},{\"9\":\"remove\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
    "{\"501\":{\"subfields\":[{\"a\":\"data\"},{\"9\":\"keep\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
    "{\"507\":{\"subfields\":[{\"a\":\"data\"},{\"9\":\"remove\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

  @Mock
  private MappingParametersProvider mappingParametersProvider;
  @InjectMocks
  private FieldModificationServiceImpl fieldModificationService;

  @Test
  public void shouldRemoveSubfield9FromNotControlledBibFields() {
    // given
    when(mappingParametersProvider.get(any(), any()))
      .thenReturn(Future.succeededFuture(new MappingParameters().withLinkingRules(linkingRules("500", "507"))));

    var expectedParsedContent = "{\"leader\":\"00118nam  22000731a 4500\"," +
      "\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"501\":{\"subfields\":[{\"a\":\"data\"},{\"9\":\"keep\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    var initialRecord = record(Record.RecordType.MARC_BIB);

    var actualRecord = fieldModificationService.remove9Subfields(null, singletonList(initialRecord), null)
      .result().get(0);

    assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent());
  }

  @Test
  public void shouldNotRemoveSubfield9FromNotBib() {
    // given
    when(mappingParametersProvider.get(any(), any()))
      .thenReturn(Future.succeededFuture(new MappingParameters().withLinkingRules(linkingRules("500", "507"))));

    var expectedParsedContent = "{\"leader\":\"00115nam  22000731a 4500\"," +
      "\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"500\":{\"subfields\":[{\"a\":\"data\"},{\"9\":\"remove\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"501\":{\"subfields\":[{\"a\":\"data\"},{\"9\":\"keep\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"507\":{\"subfields\":[{\"a\":\"data\"},{\"9\":\"remove\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    var initialRecord = record(Record.RecordType.MARC_AUTHORITY);

    var actualRecord = fieldModificationService.remove9Subfields(null, singletonList(initialRecord), null)
      .result().get(0);

    assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent());
  }

  private Record record(Record.RecordType recordType) {
    var parsedRecord = new ParsedRecord();
    parsedRecord.setContent(PARSED_CONTENT);
    return new Record().withId(UUID.randomUUID().toString()).withRecordType(recordType)
      .withParsedRecord(parsedRecord);
  }

  private List<LinkingRuleDto> linkingRules(String... tags) {
    return Arrays.stream(tags)
      .map(tag -> new LinkingRuleDto()
        .withBibField(tag))
      .collect(Collectors.toList());
  }
}
