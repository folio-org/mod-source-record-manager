package org.folio.services.mappers.processor.functions;

import io.vertx.core.json.JsonObject;
import org.apache.commons.lang.StringUtils;
import org.folio.services.mappers.processor.RuleExecutionContext;
import org.marc4j.marc.DataField;

import java.util.List;
import java.util.function.Function;

import static org.apache.commons.lang.StringUtils.EMPTY;

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
    private static final char DOT = '.';
    private static final String DOTS_THREE = "...";
    private static final String DOTS_FOUR = "....";

    @Override
    public String apply(RuleExecutionContext context) {
      String subFieldValue = context.getSubFieldValue();
      int pos = subFieldValue.length() - 1;
      if (PUNCT_2_REMOVE.contains(String.valueOf(subFieldValue.charAt(pos)))) {
        return subFieldValue.substring(0, subFieldValue.length() - 1);
      } else if (subFieldValue.charAt(pos) == DOT) {
        try {
          if (subFieldValue.substring(subFieldValue.length() - 4).equals(DOTS_FOUR)) {
            return subFieldValue.substring(0, subFieldValue.length() - 1);
          } else if (subFieldValue.substring(subFieldValue.length() - 3).equals(DOTS_THREE)) {
            return subFieldValue;
          } else {
            //if ends with .. or . remove just one .
            return subFieldValue.substring(0, subFieldValue.length() - 1);
          }
        } catch (IndexOutOfBoundsException ioob) {
          return subFieldValue;
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
    private static final String DOT = ".";

    @Override
    public String apply(RuleExecutionContext context) {
      String subFieldData = context.getSubFieldValue();
      if (subFieldData.endsWith(DOT)) {
        return subFieldData.substring(0, subFieldData.length() - 1);
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
      int from = 0;
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
  }
}
