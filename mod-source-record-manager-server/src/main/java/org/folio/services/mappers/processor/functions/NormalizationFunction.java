package org.folio.services.mappers.processor.functions;

import io.vertx.core.json.JsonObject;
import org.apache.commons.lang.StringUtils;
import org.folio.rest.jaxrs.model.ClassificationType;
import org.folio.rest.jaxrs.model.ElectronicAccessRelationship;
import org.folio.rest.jaxrs.model.InstanceType;
import org.folio.services.mappers.processor.RuleExecutionContext;
import org.folio.services.mappers.processor.publisher.PublisherRole;
import org.marc4j.marc.DataField;

import java.util.List;
import java.util.function.Function;

import static io.netty.util.internal.StringUtil.EMPTY_STRING;
import static org.apache.commons.lang3.math.NumberUtils.INTEGER_ZERO;

/**
 * Enumeration to store normalization functions
 */
public enum NormalizationFunction implements Function<RuleExecutionContext, String> {

  CHAR_SELECT() {
    private static final String FROM_PARAMETER = "from";
    private static final String TO_PARAMETER = "to";

    @Override
    public String apply(RuleExecutionContext context) {
      String subFieldValue = context.getSubFieldValue();
      JsonObject ruleParameter = context.getRuleParameter();
      if (ruleParameter != null && ruleParameter.containsKey(FROM_PARAMETER) && ruleParameter.containsKey(TO_PARAMETER)) {
        Integer from = context.getRuleParameter().getInteger(FROM_PARAMETER);
        Integer to = context.getRuleParameter().getInteger(TO_PARAMETER);
        return subFieldValue.substring(from, to);
      } else {
        return subFieldValue;
      }
    }
  },

  REMOVE_ENDING_PUNC() {
    private static final String PUNCT_2_REMOVE = ";:,/+= ";

    @Override
    public String apply(RuleExecutionContext context) {
      String subFieldValue = context.getSubFieldValue();
      if (!StringUtils.isEmpty(subFieldValue)) {
        int lastPosition = subFieldValue.length() - 1;
        if (PUNCT_2_REMOVE.contains(String.valueOf(subFieldValue.charAt(lastPosition)))) {
          return subFieldValue.substring(INTEGER_ZERO, lastPosition);
        }
      }
      return subFieldValue;
    }
  },

  TRIM() {
    @Override
    public String apply(RuleExecutionContext context) {
      return context.getSubFieldValue().trim();
    }
  },

  TRIM_PERIOD() {
    private static final String PERIOD = ".";

    @Override
    public String apply(RuleExecutionContext context) {
      String subFieldData = context.getSubFieldValue();
      if (subFieldData.endsWith(PERIOD)) {
        return subFieldData.substring(INTEGER_ZERO, subFieldData.length() - 1);
      }
      return subFieldData;
    }
  },

  REMOVE_SUBSTRING() {
    private static final String SUBSTRING_PARAMETER = "substring";

    @Override
    public String apply(RuleExecutionContext context) {
      String subFieldValue = context.getSubFieldValue();
      JsonObject ruleParameter = context.getRuleParameter();
      if (ruleParameter != null && ruleParameter.containsKey(SUBSTRING_PARAMETER)) {
        String substring = context.getRuleParameter().getString(SUBSTRING_PARAMETER);
        return StringUtils.remove(subFieldValue, substring);
      } else {
        return subFieldValue;
      }
    }
  },

  REMOVE_PREFIX_BY_INDICATOR() {
    @Override
    public String apply(RuleExecutionContext context) {
      String subFieldData = context.getSubFieldValue();
      DataField dataField = context.getDataField();
      int from = INTEGER_ZERO;
      int to = Character.getNumericValue(dataField.getIndicator2());
      if (to < subFieldData.length()) {
        String prefixToRemove = subFieldData.substring(from, to);
        return StringUtils.remove(subFieldData, prefixToRemove);
      } else {
        return subFieldData;
      }
    }
  },

  CAPITALIZE() {
    @Override
    public String apply(RuleExecutionContext context) {
      return StringUtils.capitalize(context.getSubFieldValue());
    }
  },

  SET_PUBLISHER_ROLE() {
    @Override
    public String apply(RuleExecutionContext context) {
      DataField dataField = context.getDataField();
      int indicator = Character.getNumericValue(dataField.getIndicator2());
      PublisherRole publisherRole = PublisherRole.getByIndicator(indicator);
      if (publisherRole == null) {
        return EMPTY_STRING;
      } else {
        return publisherRole.getCaption();
      }
    }
  },

  SET_CLASSIFICATION_TYPE_ID() {
    private static final String NAME_PARAMETER = "name";

    @Override
    public String apply(RuleExecutionContext context) {
      String typeName = context.getRuleParameter().getString(NAME_PARAMETER);
      List<ClassificationType> types = context.getMappingParameters().getClassificationTypes();
      if (types == null || typeName == null) {
        return STUB_FIELD_TYPE_ID;
      }
      return types.stream()
        .filter(classificationType -> classificationType.getName().equals(typeName))
        .findFirst()
        .map(ClassificationType::getId)
        .orElse(STUB_FIELD_TYPE_ID);
    }
  },

  SET_INSTANCE_TYPE_ID() {
    @Override
    public String apply(RuleExecutionContext context) {
      List<InstanceType> types = context.getMappingParameters().getInstanceTypes();
      if (types == null) {
        return STUB_FIELD_TYPE_ID;
      }
      return types.stream()
        .filter(instanceType -> instanceType.getCode().equals(context.getSubFieldValue()))
        .findFirst()
        .map(InstanceType::getId)
        .orElse(STUB_FIELD_TYPE_ID);
    }
  },
  SET_ELECTRONIC_ACCESS_RELATIONS_ID() {
    @Override
    public String apply(RuleExecutionContext context) {
      List<ElectronicAccessRelationship> electronicAccessRelationships = context.getMappingParameters().getElectronicAccessRelationships();
      if (electronicAccessRelationships == null || context.getDataField() == null) {
        return STUB_FIELD_TYPE_ID;
      }
      char ind2 = context.getDataField().getIndicator2();
      String name = ElectronicAccessRelationshipEnum.getNameByIndicator(ind2);
      return electronicAccessRelationships
        .stream()
        .filter(electronicAccessRelationship -> electronicAccessRelationship
          .getName().toLowerCase().equals(name)
        ).findFirst()
        .map(ElectronicAccessRelationship::getId)
        .orElse(STUB_FIELD_TYPE_ID);
    }
  };

  protected enum ElectronicAccessRelationshipEnum {
    RESOURCE('0', "resource"),
    VERSION_OF_RESOURCE('1', "version of resource"),
    RELATED_RESOURCE('2', "related resource"),
    NO_INFORMATION_PROVIDED('3', "no information provided");

    ElectronicAccessRelationshipEnum(char indicator2value, String name) {
      this.indicator2value = indicator2value;
      this.name = name;
    }

    private char indicator2value;
    private String name;

    public static String getNameByIndicator(char indicatorValue) {
      for (ElectronicAccessRelationshipEnum enumValue : values()) {
        if (indicatorValue == enumValue.indicator2value) {
          return enumValue.name;
        }
      }
      return NO_INFORMATION_PROVIDED.name;
    }
  }

  private static final String STUB_FIELD_TYPE_ID = "fe19bae4-da28-472b-be90-d442e2428ead";
}
