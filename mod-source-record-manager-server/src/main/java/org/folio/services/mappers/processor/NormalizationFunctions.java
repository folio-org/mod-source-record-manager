package org.folio.services.mappers.processor;

import com.google.common.base.Splitter;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang.StringUtils;
import org.marc4j.marc.DataField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

import static org.drools.core.util.StringUtils.EMPTY;

/**
 * Run a splitter on a string or run a function.
 */
public class NormalizationFunctions {
  private static final Logger LOGGER = LoggerFactory.getLogger(NormalizationFunctions.class);

  private static final String CHAR_SELECT = "char_select";
  private static final String REMOVE_ENDING_PUNC = "remove_ending_punc";
  private static final String TRIM = "trim";
  private static final String TRIM_PERIOD = "trim_period";
  private static final String REMOVE_SUBSTRING = "remove_substring";
  private static final String REMOVE_PREFIX_BY_INDICATOR = "remove_prefix_by_indicator";
  private static final String SPLIT_FUNCTION_SPLIT_EVERY = "split_every";
  private static final String SET_VALUE_IF_SUBFIELD_HAS_PREFIX = "set_value_if_subfield_has_prefix";
  private static final String PUNCT_2_REMOVE = ";:,/+= ";

  private NormalizationFunctions() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * Split val into chunks of param characters if funcName is "split_every".
   * Return null if val is null or funcName is not "split_every".
   *
   * @return the chunks
   */
  public static Iterator<String> runSplitFunction(String funcName, String subFieldData, String param) {
    if (subFieldData == null) {
      return null;
    }
    if (SPLIT_FUNCTION_SPLIT_EVERY.equals(funcName)) {
      return splitEvery(subFieldData, param);
    }
    return null;
  }

  /**
   * Run the function funcName on val and param.
   *
   * @return the function's result
   */
  public static String runFunction(String functionName, RuleExecutionContext ruleExecutionContext, JsonObject parameters) {
    String subFieldData = ruleExecutionContext.getData();
    try {
      if (subFieldData == null) {
        return EMPTY;
      }
      if (CHAR_SELECT.equals(functionName)) {
        return charSelect(subFieldData, parameters);
      } else if (REMOVE_ENDING_PUNC.equals(functionName) && !subFieldData.equals("")) {
        return removeEndingPunc(subFieldData);
      } else if (TRIM.equals(functionName)) {
        return subFieldData.trim();
      } else if (TRIM_PERIOD.equals(functionName)) {
        return trimPeriod(subFieldData);
      } else if (REMOVE_SUBSTRING.equals(functionName)) {
        return removeSubstring(subFieldData, parameters);
      } else if (REMOVE_PREFIX_BY_INDICATOR.equals(functionName)) {
        return removePrefixByIndicator(ruleExecutionContext);
      } else if (SET_VALUE_IF_SUBFIELD_HAS_PREFIX.equals(functionName)) {
        return setValueIfSubfieldHasPrefix(subFieldData, parameters);
      }
    } catch (Exception e) {
      LOGGER.error("Error while running normalization functions, cause: {}", e.getLocalizedMessage());
      return subFieldData;
    }
    return EMPTY;
  }

  private static String setValueIfSubfieldHasPrefix(String subFieldData, JsonObject parameters) {
    List<String> prefixes = parameters.getJsonArray("prefixes").getList();
    String valueIfConditionIsTrue = parameters.getString("valueIfConditionIsTrue");
    String valueIfConditionIsFalse = parameters.getString("valueIfConditionIsFalse");
    if (prefixes.stream().anyMatch(subFieldData::startsWith)) {
      return valueIfConditionIsTrue;
    } else {
      return valueIfConditionIsFalse;
    }
  }

  private static Iterator<String> splitEvery(String subFieldData, String param) {
    return Splitter.fixedLength(Integer.parseInt(param)).split(subFieldData).iterator();
  }

  private static String charSelect(String subFieldData, JsonObject parameter) {
    Integer from = parameter.getInteger("from");
    Integer to = parameter.getInteger("to");
    return subFieldData.substring(from, to);
  }

  private static String trimPeriod(final String subFieldData) {
    if (subFieldData.endsWith(".")) {
      return subFieldData.substring(0, subFieldData.length() - 1);
    }
    return subFieldData;
  }

  private static String removeEndingPunc(String subFieldData) {
    return modified(subFieldData, (subFieldData.length() - 1));
  }

  private static String modified(final String subFieldData, int pos) {
    if (PUNCT_2_REMOVE.contains(String.valueOf(subFieldData.charAt(pos)))) {
      return subFieldData.substring(0, subFieldData.length() - 1);
    } else if (subFieldData.charAt(pos) == '.') {
      try {
        if (subFieldData.substring(subFieldData.length() - 4).equals("....")) {
          return subFieldData.substring(0, subFieldData.length() - 1);
        } else if (subFieldData.substring(subFieldData.length() - 3).equals("...")) {
          return subFieldData;
        } else {
          //if ends with .. or . remove just one .
          return subFieldData.substring(0, subFieldData.length() - 1);
        }
      } catch (IndexOutOfBoundsException ioob) {
        return subFieldData;
      }
    }
    return subFieldData;
  }

  private static String removeSubstring(String subFieldData, JsonObject parameter) {
    if (parameter.containsKey("substring")) {
      String substring = parameter.getString("substring");
      return StringUtils.remove(subFieldData, substring);
    } else {
      return subFieldData;
    }
  }

  private static String removePrefixByIndicator(RuleExecutionContext ruleExecutionContext) {
    String subFieldData = ruleExecutionContext.getData();
    DataField dataField = ruleExecutionContext.getDataField();
    int from = 0;
    int to = Character.getNumericValue(dataField.getIndicator2());
    String prefixToRemove = subFieldData.substring(from, to);
    String result = StringUtils.remove(subFieldData, prefixToRemove);
    return StringUtils.capitalize(result);
  }
}
