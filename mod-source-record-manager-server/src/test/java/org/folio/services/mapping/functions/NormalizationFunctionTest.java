package org.folio.services.mapping.functions;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.AlternativeTitleType;
import org.folio.rest.jaxrs.model.ClassificationType;
import org.folio.rest.jaxrs.model.ContributorNameType;
import org.folio.rest.jaxrs.model.ContributorType;
import org.folio.rest.jaxrs.model.ElectronicAccessRelationship;
import org.folio.rest.jaxrs.model.IdentifierType;
import org.folio.rest.jaxrs.model.InstanceFormat;
import org.folio.rest.jaxrs.model.InstanceNoteType;
import org.folio.rest.jaxrs.model.InstanceType;
import org.folio.services.mappers.processor.RuleExecutionContext;
import org.folio.services.mappers.processor.parameters.MappingParameters;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.marc4j.marc.DataField;
import org.marc4j.marc.impl.DataFieldImpl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.netty.util.internal.StringUtil.EMPTY_STRING;
import static org.folio.services.mappers.processor.functions.NormalizationFunctionRunner.runFunction;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class NormalizationFunctionTest {
  private static final String STUB_FIELD_TYPE_ID = "fe19bae4-da28-472b-be90-d442e2428ead";
  private static final String UNSPECIFIED_INSTANCE_TYPE_ID = "30fffe0e-e985-4144-b2e2-1e8179bdb41f";


  @Test
  public void CHAR_SELECT_shouldReturnExpectedResult() {
    // given
    String givenSubField = "890411m19309999pau      l    001 0 eng  ";
    String expectedSubField = "eng";
    JsonObject ruleParameter = new JsonObject().put("from", 35).put("to", 38);
    RuleExecutionContext context = new RuleExecutionContext();
    context.setSubFieldValue(givenSubField);
    context.setRuleParameter(ruleParameter);
    // when
    String actualSubField = runFunction("char_select", context);
    // then
    assertEquals(expectedSubField, actualSubField);
  }

  @Test
  public void CHAR_SELECT_shouldReturnGivenSubFieldIfWrongParameterSpecified() {
    // given
    String givenSubField = "890411m19309999pau      l    001 0 eng  ";
    String expectedSubField = givenSubField;
    RuleExecutionContext context = new RuleExecutionContext();
    context.setSubFieldValue(givenSubField);
    // when
    context.setRuleParameter(null);
    String actualSubField_ifParameterNoParameterSpecified = runFunction("char_select", context);

    context.setRuleParameter(new JsonObject().put("from", -5).put("to", -1));
    String actualSubField_ifNegativeArgumentsSpecified = runFunction("char_select", context);
    // then
    assertEquals(expectedSubField, actualSubField_ifParameterNoParameterSpecified);
    assertEquals(expectedSubField, actualSubField_ifNegativeArgumentsSpecified);
  }

  @Test
  public void REMOVE_ENDING_PUNC_shouldReturnExpectedResult() {
    // given
    Map<String, String> givenFieldToExpectedFieldMap = new HashMap<>();
    givenFieldToExpectedFieldMap.put("Research Publications,", "Research Publications");
    givenFieldToExpectedFieldMap.put("Cambridge University Press [etc.]", "Cambridge University Press [etc.]");
    givenFieldToExpectedFieldMap.put("[1982-", "[1982-");
    givenFieldToExpectedFieldMap.put("Woodbridge, Conn. :", "Woodbridge, Conn. ");
    givenFieldToExpectedFieldMap.put("Hodder & Stoughton", "Hodder & Stoughton");
    givenFieldToExpectedFieldMap.put("[1960]", "[1960]");
    givenFieldToExpectedFieldMap.put("C. F. Peters Corp./", "C. F. Peters Corp.");
    givenFieldToExpectedFieldMap.put("0345404475 (pbk.) : $11.00", "0345404475 (pbk.) : $11.00");
    givenFieldToExpectedFieldMap.put("0585098646 (electronic bk.)", "0585098646 (electronic bk.)");
    givenFieldToExpectedFieldMap.put("[England? :", "[England? ");
    givenFieldToExpectedFieldMap.put("[London. ", "[London.");
    givenFieldToExpectedFieldMap.put("George T. Bisel Co. ;", "George T. Bisel Co. ");
    givenFieldToExpectedFieldMap.put("West Publishing Co.,", "West Publishing Co.");
    givenFieldToExpectedFieldMap.put("A6++", "A6+");
    givenFieldToExpectedFieldMap.put("providercode=", "providercode");
    givenFieldToExpectedFieldMap.put("+;", "+");
    givenFieldToExpectedFieldMap.put(EMPTY_STRING, EMPTY_STRING);

    RuleExecutionContext context = new RuleExecutionContext();
    for (Map.Entry<String, String> entry : givenFieldToExpectedFieldMap.entrySet()) {
      String givenSubField = entry.getKey();
      String expectedSubField = entry.getValue();
      context.setSubFieldValue(givenSubField);
      // when
      String actualSubField = runFunction("remove_ending_punc", context);
      // then
      assertEquals(expectedSubField, actualSubField);
    }
  }

  @Test
  public void TRIM_shouldReturnExpectedResult() {
    // given
    Map<String, String> givenFieldToExpectedFieldMap = new HashMap<>();
    givenFieldToExpectedFieldMap.put(" Dugmore, C. W. (Clifford William), ", "Dugmore, C. W. (Clifford William),");
    givenFieldToExpectedFieldMap.put("   58020553 ", "58020553");
    givenFieldToExpectedFieldMap.put(" 0022-0469  ", "0022-0469");
    givenFieldToExpectedFieldMap.put("   55001156/M ", "55001156/M");
    givenFieldToExpectedFieldMap.put(EMPTY_STRING, EMPTY_STRING);

    RuleExecutionContext context = new RuleExecutionContext();
    for (Map.Entry<String, String> entry : givenFieldToExpectedFieldMap.entrySet()) {
      String givenSubField = entry.getKey();
      context.setSubFieldValue(givenSubField);
      String expectedSubField = entry.getValue();
      // when
      String actualSubField = runFunction("trim", context);
      // then
      assertEquals(expectedSubField, actualSubField);
    }
  }

  @Test
  public void TRIM_PERIOD_shouldReturnExpectedResult() {
    // given
    String givenSubField = " . 99.082/x12/. .";
    String expectedSubField = " . 99.082/x12/. ";
    RuleExecutionContext context = new RuleExecutionContext();
    context.setSubFieldValue(givenSubField);
    // when
    String actualSubField = runFunction("trim_period", context);
    // then
    assertEquals(expectedSubField, actualSubField);
  }

  @Test
  public void REMOVE_SUBSTRING_shouldReturnExpectedResult() {
    // given
    String givenSubField = "362 .2/92 ./8";
    Map<String, String> givenRuleParameterToExpectedFieldMap = new HashMap<>();
    givenRuleParameterToExpectedFieldMap.put("/", "362 .292 .8");
    givenRuleParameterToExpectedFieldMap.put(".", "362 2/92 /8");
    givenRuleParameterToExpectedFieldMap.put(" ", "362.2/92./8");
    givenRuleParameterToExpectedFieldMap.put("2 .", "362/9/8");
    givenRuleParameterToExpectedFieldMap.put(EMPTY_STRING, "362 .2/92 ./8");

    RuleExecutionContext context = new RuleExecutionContext();
    context.setSubFieldValue(givenSubField);
    for (Map.Entry<String, String> entry : givenRuleParameterToExpectedFieldMap.entrySet()) {
      JsonObject givenRuleParameter = new JsonObject().put("substring", entry.getKey());
      String expectedSubField = entry.getValue();
      context.setRuleParameter(givenRuleParameter);
      // when
      String actualSubField = runFunction("remove_substring", context);
      // then
      assertEquals(expectedSubField, actualSubField);
    }
  }

  @Test
  public void REMOVE_SUBSTRING_shouldReturnGivenSubFieldIfWrongParameterSpecified() {
    // given
    String givenSubField = "305.2309599";
    String expectedSubField = givenSubField;
    JsonObject ruleParameter = new JsonObject().put("substring", -132);
    RuleExecutionContext context = new RuleExecutionContext();
    context.setSubFieldValue(givenSubField);
    context.setRuleParameter(ruleParameter);
    // when
    String actualSubField = runFunction("remove_substring", context);
    // then
    assertEquals(expectedSubField, actualSubField);
  }

  @Test
  public void REMOVE_PREFIX_BY_INDICATOR_shouldReturnExpectedResult() {
    // given
    String givenSubField = "Dugmore, C. W. (Clifford William),";
    Map<Character, String> givenIndicatorToExpectedFieldMap = new HashMap<>();
    givenIndicatorToExpectedFieldMap.put('0', "Dugmore, C. W. (Clifford William),");
    givenIndicatorToExpectedFieldMap.put('1', "ugmore, C. W. (Clifford William),");
    givenIndicatorToExpectedFieldMap.put('2', "gmore, C. W. (Clifford William),");
    givenIndicatorToExpectedFieldMap.put('3', "more, C. W. (Clifford William),");
    givenIndicatorToExpectedFieldMap.put('5', "re, C. W. (Clifford William),");
    givenIndicatorToExpectedFieldMap.put('9', "C. W. (Clifford William),");

    RuleExecutionContext context = new RuleExecutionContext();
    context.setSubFieldValue(givenSubField);
    for (Map.Entry<Character, String> givenIndicatorAndExpectedSubField : givenIndicatorToExpectedFieldMap.entrySet()) {
      char givenIndicator = givenIndicatorAndExpectedSubField.getKey();
      context.setDataField(new DataFieldImpl("100", '0', givenIndicator));
      String expectedField = givenIndicatorAndExpectedSubField.getValue();
      // when
      String actualSubField = runFunction("remove_prefix_by_indicator", context);
      // then
      assertEquals(expectedField, actualSubField);
    }
  }

  @Test
  public void REMOVE_PREFIX_BY_INDICATOR_shouldReturnGivenSubFieldIfWrongIndicatorSpecified() {
    // given
    String givenSubField = "London.";
    DataField givenDataField = new DataFieldImpl("100", '0', '9');
    String expectedSubField = givenSubField;
    RuleExecutionContext context = new RuleExecutionContext();
    context.setDataField(givenDataField);
    context.setSubFieldValue(givenSubField);
    // when
    String actualSubField = runFunction("remove_prefix_by_indicator", context);
    // then
    assertEquals(expectedSubField, actualSubField);
  }

  @Test
  public void CAPITALIZE_shouldReturnExpectedResult() {
    // given
    Map<String, String> givenAndExpectedSubFieldMap = new HashMap<>();
    givenAndExpectedSubFieldMap.put("journal of ecclesiastical history.", "Journal of ecclesiastical history.");
    givenAndExpectedSubFieldMap.put("mistapim in Cambodia", "Mistapim in Cambodia");
    givenAndExpectedSubFieldMap.put("London", "London");
    givenAndExpectedSubFieldMap.put("london", "London");
    givenAndExpectedSubFieldMap.put("362.2/92./8", "362.2/92./8");
    givenAndExpectedSubFieldMap.put(EMPTY_STRING, EMPTY_STRING);

    RuleExecutionContext context = new RuleExecutionContext();
    for (Map.Entry<String, String> entry : givenAndExpectedSubFieldMap.entrySet()) {
      String givenSubField = entry.getKey();
      context.setSubFieldValue(givenSubField);
      String expectedSubField = entry.getValue();
      // when
      String actualSubField = runFunction("capitalize", context);
      // then
      assertEquals(expectedSubField, actualSubField);
    }
  }

  @Test
  public void SET_PUBLISHER_ROLE_shouldReturnExpectedResult() {
    // given
    Map<Character, String> givenIndicatorToExpectedRoleMap = new HashMap<>();
    givenIndicatorToExpectedRoleMap.put('0', "Production");
    givenIndicatorToExpectedRoleMap.put('1', "Publication");
    givenIndicatorToExpectedRoleMap.put('2', "Distribution");
    givenIndicatorToExpectedRoleMap.put('3', "Manufacture");
    givenIndicatorToExpectedRoleMap.put('4', EMPTY_STRING);

    RuleExecutionContext context = new RuleExecutionContext();
    for (Map.Entry<Character, String> entry : givenIndicatorToExpectedRoleMap.entrySet()) {
      DataField givenDataField = new DataFieldImpl("100", '0', entry.getKey());
      context.setDataField(givenDataField);
      String expectedSubField = entry.getValue();
      // when
      String actualSubField = runFunction("set_publisher_role", context);
      // then
      assertEquals(expectedSubField, actualSubField);
    }
  }

  @Test
  public void SET_CLASSIFICATION_TYPE_ID_shouldReturnExpectedResult() {
    // given
    String expectedClassificationTypeId = UUID.randomUUID().toString();
    ClassificationType givenClassificationType = new ClassificationType()
      .withId(expectedClassificationTypeId)
      .withName("LC")
      .withSource("folio");
    RuleExecutionContext context = new RuleExecutionContext();
    context.setMappingParameters(new MappingParameters().withClassificationTypes(Collections.singletonList(givenClassificationType)));
    context.setRuleParameter(new JsonObject().put("name", "LC"));
    // when
    String actualClassificationTypeId = runFunction("set_classification_type_id", context);
    // then
    assertEquals(expectedClassificationTypeId, actualClassificationTypeId);
  }

  @Test
  public void SET_CLASSIFICATION_TYPE_ID_shouldReturnStubIdIfNoSettingsSpecified() {
    // given
    RuleExecutionContext context = new RuleExecutionContext();
    context.setMappingParameters(new MappingParameters());
    context.setRuleParameter(new JsonObject().put("name", "LC"));
    // when
    String actualClassificationTypeId = runFunction("set_classification_type_id", context);
    // then
    assertEquals(STUB_FIELD_TYPE_ID, actualClassificationTypeId);
  }

  @Test
  public void SET_INSTANCE_TYPE_ID_shouldReturnExpectedResult() {
    // given
    String expectedInstanceTypeId = UUID.randomUUID().toString();
    InstanceType instanceType = new InstanceType()
      .withId(expectedInstanceTypeId)
      .withCode("txt");
    RuleExecutionContext context = new RuleExecutionContext();
    context.setSubFieldValue("txt");
    context.setMappingParameters(new MappingParameters().withInstanceTypes(Collections.singletonList(instanceType)));
    context.setRuleParameter(new JsonObject().put("unspecifiedInstanceTypeCode", "zzz"));
    context.setDataField(new DataFieldImpl("336", ' ', ' '));
    // when
    String actualTypeId = runFunction("set_instance_type_id", context);
    // then
    assertEquals(expectedInstanceTypeId, actualTypeId);
  }

  @Test
  public void SET_INSTANCE_TYPE_ID_shouldReturnStubIdIfNoSettingsSpecified() {
    // given
    InstanceType instanceType = new InstanceType()
      .withId(UUID.randomUUID().toString())
      .withCode("fail");
    RuleExecutionContext context = new RuleExecutionContext();
    context.setSubFieldValue("txt");
    context.setMappingParameters(new MappingParameters().withInstanceTypes(Collections.singletonList(instanceType)));
    context.setRuleParameter(new JsonObject().put("unspecifiedInstanceTypeCode", "zzz"));
    // when
    String actualTypeId = runFunction("set_instance_type_id", context);
    // then
    assertEquals(STUB_FIELD_TYPE_ID, actualTypeId);
  }

  @Test
  public void SET_ELECTRONIC_ACCESS_RELATIONS_ID_shouldReturnStubIdIfNoSettingsSpecified() {
    // given
    ElectronicAccessRelationship electronicAccessRelationship = new ElectronicAccessRelationship()
      .withId(UUID.randomUUID().toString())
      .withName("fail");
    RuleExecutionContext context = new RuleExecutionContext();
    context.setSubFieldValue("txt");
    context.setMappingParameters(new MappingParameters()
      .withElectronicAccessRelationships(Collections.singletonList(electronicAccessRelationship)));
    // when
    String actualTypeId = runFunction("set_electronic_access_relations_id", context);
    // then
    assertEquals(STUB_FIELD_TYPE_ID, actualTypeId);
  }

  @Test
  public void SET_ELECTRONIC_ACCESS_RELATIONS_ID_shouldReturnValidId() {
    // given
    String uuid = UUID.randomUUID().toString();
    ElectronicAccessRelationship electronicAccessRelationship = new ElectronicAccessRelationship()
      .withId(uuid)
      .withName("Related resource");
    RuleExecutionContext context = new RuleExecutionContext();
    context.setDataField(new DataFieldImpl("856", '1', '2'));
    context.setMappingParameters(new MappingParameters()
      .withElectronicAccessRelationships(Collections.singletonList(electronicAccessRelationship)));
    // when
    String actualTypeId = runFunction("set_electronic_access_relations_id", context);
    // then
    assertEquals(uuid, actualTypeId);
  }

  @Test
  public void SET_ELECTRONIC_ACCESS_RELATIONS_ID_shouldReturnValidIdForUnfounded() {
    // given
    String uuid = UUID.randomUUID().toString();
    ElectronicAccessRelationship electronicAccessRelationship = new ElectronicAccessRelationship()
      .withId(uuid)
      .withName("No information provided");
    RuleExecutionContext context = new RuleExecutionContext();
    context.setDataField(new DataFieldImpl("856", '1', '5'));
    context.setMappingParameters(new MappingParameters()
      .withElectronicAccessRelationships(Collections.singletonList(electronicAccessRelationship)));
    // when
    String actualTypeId = runFunction("set_electronic_access_relations_id", context);
    // then
    assertEquals(uuid, actualTypeId);
  }

  @Test
  public void SET_INSTANCE_FORMAT_ID_shouldReturnExpectedResult() {
    // given
    String expectedInstanceFormatId = UUID.randomUUID().toString();
    InstanceFormat instanceFormat = new InstanceFormat()
      .withId(expectedInstanceFormatId)
      .withCode("nc");
    RuleExecutionContext context = new RuleExecutionContext();
    context.setSubFieldValue("nc");
    context.setMappingParameters(new MappingParameters().withInstanceFormats(Collections.singletonList(instanceFormat)));
    // when
    String actualTypeId = runFunction("set_instance_format_id", context);
    // then
    assertEquals(expectedInstanceFormatId, actualTypeId);
  }

  @Test
  public void SET_INSTANCE_FORMAT_ID_shouldReturnEmptyStringIfNoSettingsSpecified() {
    // given
    InstanceFormat instanceFormat = new InstanceFormat()
      .withId(UUID.randomUUID().toString())
      .withCode("fail");
    RuleExecutionContext context = new RuleExecutionContext();
    context.setSubFieldValue("nc");
    context.setMappingParameters(new MappingParameters().withInstanceFormats(Collections.singletonList(instanceFormat)));
    // when
    String actualTypeId = runFunction("set_instance_format_id", context);
    // then
    assertEquals(StringUtils.EMPTY, actualTypeId);
  }

  @Test
  public void SET_CONTRIBUTOR_TYPE_ID_shouldReturnExpectedResult() {
    // given
    String expectedContributorTypeId = UUID.randomUUID().toString();
    ContributorType givenContributorType = new ContributorType()
      .withId(expectedContributorTypeId)
      .withName("Animator")
      .withCode("anm");
    RuleExecutionContext context = new RuleExecutionContext();
    context.setSubFieldValue("anm");
    context.setMappingParameters(new MappingParameters().withContributorTypes(Collections.singletonList(givenContributorType)));
    // when
    String actualContributorTypeId = runFunction("set_contributor_type_id", context);
    // then
    assertEquals(expectedContributorTypeId, actualContributorTypeId);
  }

  @Test
  public void SET_CONTRIBUTOR_TYPE_ID_shouldReturnStubIdIfNoSettingsSpecified() {
    // given
    RuleExecutionContext context = new RuleExecutionContext();
    context.setMappingParameters(new MappingParameters());
    // when
    String actualContributorTypeId = runFunction("set_contributor_type_id", context);
    // then
    assertEquals(StringUtils.EMPTY, actualContributorTypeId);
  }

  @Test
  public void SET_CONTRIBUTOR_TYPE_TEXT_shouldReturnExpectedResult() {
    // given
    String expectedContributorTypeText = "Arranger";
    ContributorType givenContributorType = new ContributorType()
      .withId(UUID.randomUUID().toString())
      .withName("Arranger")
      .withCode("arr");
    RuleExecutionContext context = new RuleExecutionContext();
    context.setSubFieldValue("arr");
    context.setMappingParameters(new MappingParameters().withContributorTypes(Collections.singletonList(givenContributorType)));
    // when
    String actualContributorTypeText = runFunction("set_contributor_type_text", context);
    // then
    assertEquals(expectedContributorTypeText, actualContributorTypeText);
  }

  @Test
  public void SET_CONTRIBUTOR_TYPE_TEXT_shouldReturnGivenSubFieldIfNoSettingsSpecified() {
    // given
    String expectedSubField = "arr";
    RuleExecutionContext context = new RuleExecutionContext();
    context.setSubFieldValue("arr");
    context.setMappingParameters(new MappingParameters());
    // when
    String actualContributorTypeText = runFunction("set_contributor_type_text", context);
    // then
    assertEquals(expectedSubField, actualContributorTypeText);
  }

  @Test
  public void SET_CONTRIBUTOR_TYPE_TEXT_shouldReturnGivenSubFieldIfNoMatchInSettings() {
    // given
    String expectedContributorTypeText = "arr";
    ContributorType givenContributorType = new ContributorType()
      .withId(UUID.randomUUID().toString())
      .withName("Animator")
      .withCode("anm");
    RuleExecutionContext context = new RuleExecutionContext();
    context.setMappingParameters(new MappingParameters().withContributorTypes(Collections.singletonList(givenContributorType)));
    context.setSubFieldValue("arr");
    // when
    String actualContributorTypeText = runFunction("set_contributor_type_text", context);
    // then
    assertEquals(expectedContributorTypeText, actualContributorTypeText);
  }

  @Test
  public void SET_CONTRIBUTOR_NAME_TYPE_ID_shouldReturnExpectedResult() {
    // given
    String expectedContributorNameTypeId = UUID.randomUUID().toString();
    ContributorNameType givenContributorNameType = new ContributorNameType()
      .withId(expectedContributorNameTypeId)
      .withName("Personal name");
    RuleExecutionContext context = new RuleExecutionContext();
    context.setMappingParameters(new MappingParameters().withContributorNameTypes(Collections.singletonList(givenContributorNameType)));
    context.setRuleParameter(new JsonObject().put("name", "Personal name"));
    // when
    String actualContributorNameTypeId = runFunction("set_contributor_name_type_id", context);
    // then
    assertEquals(expectedContributorNameTypeId, actualContributorNameTypeId);
  }

  @Test
  public void SET_CONTRIBUTOR_NAME_TYPE_ID_shouldReturnStubIdIfNoSettingsSpecified() {
    // given
    RuleExecutionContext context = new RuleExecutionContext();
    context.setMappingParameters(new MappingParameters());
    context.setRuleParameter(new JsonObject().put("name", "Personal name"));
    // when
    String actualContributorNameTypeId = runFunction("set_contributor_name_type_id", context);
    // then
    assertEquals(STUB_FIELD_TYPE_ID, actualContributorNameTypeId);
  }

  @Test
  public void SET_IDENTIFIER_TYPE_ID_BY_NAME_shouldReturnExpectedResult() {
    // given
    String expectedIdentifierTypeId = UUID.randomUUID().toString();
    IdentifierType identifierType = new IdentifierType()
      .withId(expectedIdentifierTypeId)
      .withName("GPO item number");
    RuleExecutionContext context = new RuleExecutionContext();
    context.setMappingParameters(new MappingParameters().withIdentifierTypes(Collections.singletonList(identifierType)));
    context.setRuleParameter(new JsonObject().put("name", "GPO item number"));
    // when
    String actualIdentifierTypeId = runFunction("set_identifier_type_id_by_name", context);
    // then
    assertEquals(expectedIdentifierTypeId, actualIdentifierTypeId);
  }

  @Test
  public void SET_IDENTIFIER_TYPE_ID_BY_NAME_shouldReturnStubIdIfNoSettingsSpecified() {
    // given
    RuleExecutionContext context = new RuleExecutionContext();
    context.setMappingParameters(new MappingParameters());
    context.setRuleParameter(new JsonObject().put("name", "GPO item number"));
    // when
    String actualIdentifierTypeId = runFunction("set_identifier_type_id_by_name", context);
    // then
    assertEquals(STUB_FIELD_TYPE_ID, actualIdentifierTypeId);
  }

  @Test
  public void SET_IDENTIFIER_TYPE_ID_BY_VALUE_shouldReturnExpectedResult() {
    // given
    String identifierTypeId = UUID.randomUUID().toString();
    String oclcIdentifierTypeId = UUID.randomUUID().toString();
    IdentifierType oclcIdentifierType = new IdentifierType()
      .withId(oclcIdentifierTypeId)
      .withName("OCLC");
    IdentifierType identifierType = new IdentifierType()
      .withId(identifierTypeId)
      .withName("System control number");
    RuleExecutionContext context = new RuleExecutionContext();
    context.setMappingParameters(new MappingParameters().withIdentifierTypes(Collections.singletonList(identifierType)));
    context.setRuleParameter(new JsonObject().put("names", new JsonArray().add("System control number").add("OCLC"))
      .put("oclc_regex", "(\\(OCoLC\\)|ocm|ocn|on).*"));
    context.setSubFieldValue("910504526");
    RuleExecutionContext oclcContext = new RuleExecutionContext();
    oclcContext.setMappingParameters(new MappingParameters().withIdentifierTypes(Collections.singletonList(oclcIdentifierType)));
    oclcContext.setRuleParameter(new JsonObject().put("names", new JsonArray().add("System control number").add("OCLC"))
      .put("oclc_regex", "(\\(OCoLC\\)|ocm|ocn|on).*"));
    oclcContext.setSubFieldValue("(OCoLC)910504526");
    // when
    String actualIdentifierTypeId = runFunction("set_identifier_type_id_by_value", context);
    String actualOclcIdentifierTypeId = runFunction("set_identifier_type_id_by_value", oclcContext);
    // then
    assertEquals(identifierTypeId, actualIdentifierTypeId);
    assertEquals(oclcIdentifierTypeId, actualOclcIdentifierTypeId);
  }

  @Test
  public void SET_IDENTIFIER_TYPE_ID_BY_VALUE_shouldReturnStubIdIfNoSettingsSpecified() {
    // given
    RuleExecutionContext context = new RuleExecutionContext();
    context.setMappingParameters(new MappingParameters());
    context.setRuleParameter(new JsonObject().put("names", new JsonArray().add("System control number").add("OCLC"))
      .put("identifiers", new JsonArray().add("(OCoLC)")));
    context.setSubFieldValue("(OCoLC)910504526");
    // when
    String actualIdentifierTypeId = runFunction("set_identifier_type_id_by_value", context);
    // then
    assertEquals(STUB_FIELD_TYPE_ID, actualIdentifierTypeId);
  }

  @Test
  public void SET_NOTE_TYPE_ID_shouldReturnExpectedResult() {
    // given
    String expectedInstanceNoteTypeId = UUID.randomUUID().toString();
    InstanceNoteType instanceNoteType = new InstanceNoteType()
      .withId(expectedInstanceNoteTypeId)
      .withName("Summary");
    RuleExecutionContext context = new RuleExecutionContext();
    context.setMappingParameters(new MappingParameters().withInstanceNoteTypes(Collections.singletonList(instanceNoteType)));
    context.setRuleParameter(new JsonObject().put("name", "Summary"));
    // when
    String actualInstanceNoteTypeId = runFunction("set_note_type_id", context);
    // then
    assertEquals(expectedInstanceNoteTypeId, actualInstanceNoteTypeId);
  }

  @Test
  public void SET_NOTE_TYPE_ID_shouldReturnDefaultNoteTypeResultIfNoSettingsSpecified() {
    // given
    String expectedInstanceNoteTypeId = UUID.randomUUID().toString();
    InstanceNoteType defaultNoteType = new InstanceNoteType()
      .withId(expectedInstanceNoteTypeId)
      .withName("General note");
    RuleExecutionContext context = new RuleExecutionContext();
    context.setMappingParameters(new MappingParameters().withInstanceNoteTypes(Collections.singletonList(defaultNoteType)));
    context.setRuleParameter(new JsonObject().put("name", "Summary"));
    // when
    String actualInstanceNoteTypeId = runFunction("set_note_type_id", context);
    // then
    assertEquals(expectedInstanceNoteTypeId, actualInstanceNoteTypeId);
  }

  @Test
  public void SET_NOTE_TYPE_ID_shouldReturnStubIdIfNoSettingsSpecified() {
    // given
    RuleExecutionContext context = new RuleExecutionContext();
    context.setMappingParameters(new MappingParameters());
    context.setRuleParameter(new JsonObject().put("name", "Summary"));
    // when
    String actualInstanceNoteTypeId = runFunction("set_note_type_id", context);
    // then
    assertEquals(STUB_FIELD_TYPE_ID, actualInstanceNoteTypeId);
  }

  @Test
  public void SET_ALTERNATIVE_TITLE_TYPE_ID_shouldReturnExpectedResult() {
    // given
    String expectedAlternativeTitleTypeId = UUID.randomUUID().toString();
    AlternativeTitleType alternativeTitleType = new AlternativeTitleType()
      .withId(expectedAlternativeTitleTypeId)
      .withName("Uniform title");
    RuleExecutionContext context = new RuleExecutionContext();
    context.setMappingParameters(new MappingParameters().withAlternativeTitleTypes(Collections.singletonList(alternativeTitleType)));
    context.setRuleParameter(new JsonObject().put("name", "Uniform title"));
    // when
    String actualAlternativeTitleTypeId = runFunction("set_alternative_title_type_id", context);
    // then
    assertEquals(expectedAlternativeTitleTypeId, actualAlternativeTitleTypeId);
  }

  @Test
  public void SET_ALTERNATIVE_TITLE_TYPE_ID_shouldReturnStubIdIfNoSettingsSpecified() {
    // given
    RuleExecutionContext context = new RuleExecutionContext();
    context.setMappingParameters(new MappingParameters());
    context.setRuleParameter(new JsonObject().put("name", "Uniform title"));
    // when
    String actualAlternativeTitleTypeId = runFunction("set_alternative_title_type_id", context);
    // then
    assertEquals(STUB_FIELD_TYPE_ID, actualAlternativeTitleTypeId);
  }

}
