package org.folio.services.mappers.processor.functions;

import io.vertx.core.json.JsonObject;
import org.apache.commons.lang.StringUtils;
import org.folio.services.mappers.processor.RuleExecutionContext;
import org.folio.services.mappers.processor.publisher.PublisherRole;
import org.marc4j.marc.DataField;

import java.util.List;
import java.util.function.Function;

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
      Integer from = context.getRuleParameter().getInteger(FROM_PARAMETER);
      Integer to = context.getRuleParameter().getInteger(TO_PARAMETER);
      return subFieldValue.substring(from, to);
    }
  },

  REMOVE_ENDING_PUNC() {
    private static final String PUNCT_2_REMOVE = ";:,/+= ";

    @Override
    public String apply(RuleExecutionContext context) {
      String subFieldValue = context.getSubFieldValue();
      int lastPosition = subFieldValue.length() - 1;
      if (PUNCT_2_REMOVE.contains(String.valueOf(subFieldValue.charAt(lastPosition)))) {
        return subFieldValue.substring(INTEGER_ZERO, lastPosition);
      } else {
        return subFieldValue;
      }
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
      if (context.getRuleParameter().containsKey(SUBSTRING_PARAMETER)) {
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
      String prefixToRemove = subFieldData.substring(from, to);
      return StringUtils.remove(subFieldData, prefixToRemove);
    }
  },

  CAPITALIZE() {
    @Override
    public String apply(RuleExecutionContext context) {
      return StringUtils.capitalize(context.getSubFieldValue());
    }
  },

  SET_VALUE_IF_SUBFIELD_HAS_PREFIX() {
    private static final String PREFIXES_PARAMETER = "prefixes";
    private static final String VALUE_CONDITION_IS_TRUE_PARAMETER = "valueIfConditionIsTrue";
    private static final String VALUE_CONDITION_IS_FALSE_PARAMETER = "valueIfConditionIsFalse";

    @Override
    public String apply(RuleExecutionContext context) {
      JsonObject parameters = context.getRuleParameter();
      List<String> prefixes = parameters.getJsonArray(PREFIXES_PARAMETER).getList();
      String valueIfConditionIsTrue = parameters.getString(VALUE_CONDITION_IS_TRUE_PARAMETER);
      String valueIfConditionIsFalse = parameters.getString(VALUE_CONDITION_IS_FALSE_PARAMETER);
      if (prefixes.stream().anyMatch(context.getSubFieldValue()::startsWith)) {
        return valueIfConditionIsTrue;
      } else {
        return valueIfConditionIsFalse;
      }
    }
  },

  SET_PUBLICATION_ROLE() {
    @Override
    public String apply(RuleExecutionContext context) {
      DataField dataField = context.getDataField();
      int indicator = Character.getNumericValue(dataField.getIndicator2());
      PublisherRole publisherRole = PublisherRole.getByIndicator(indicator);
      if (publisherRole == null) {
        return StringUtils.EMPTY;
      } else {
        return publisherRole.getCaption();
      }
    }
  },

  REMOVE_ENDING_STRING() {
    private static final String PARAM = "string";

    @Override
    public String apply(RuleExecutionContext context) {
      String suffix = context.getRuleParameter().getString(PARAM);
      String subFieldValue = context.getSubFieldValue();
      return StringUtils.removeEnd(subFieldValue, suffix);
    }
  }
}
