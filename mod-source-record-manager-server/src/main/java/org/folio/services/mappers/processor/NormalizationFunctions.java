package org.folio.services.mappers.processor;

import com.google.common.base.Splitter;
import io.vertx.core.json.JsonObject;

import java.util.Iterator;

import static org.drools.core.util.StringUtils.EMPTY;

/**
 * Run a splitter on a string or run a function.
 */
public class NormalizationFunctions {

  private static final String CHAR_SELECT = "char_select";
  private static final String REMOVE_ENDING_PUNC = "remove_ending_punc";
  private static final String TRIM = "trim";
  private static final String TRIM_PERIOD = "trim_period";
  private static final String REMOVE_SUBSTRING = "remove_substring";
  private static final String SPLIT_FUNCTION_SPLIT_EVERY = "split_every";
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
  public static String runFunction(String functionName, RuleExecutionContext ruleExecutionContext, JsonObject parameter) {
    String subFieldData = ruleExecutionContext.getData();
    if (subFieldData == null) {
      return EMPTY;
    }
    if (CHAR_SELECT.equals(functionName)) {
      return charSelect(subFieldData, parameter);
    } else if (REMOVE_ENDING_PUNC.equals(functionName) && !subFieldData.equals("")) {
      return removeEndingPunc(subFieldData);
    } else if (TRIM.equals(functionName)) {
      return subFieldData.trim();
    } else if (TRIM_PERIOD.equals(functionName)) {
      return trimPeriod(subFieldData);
    } else if (REMOVE_SUBSTRING.equals(functionName)) {
      return removeSubstring(subFieldData, parameter);
    }
    return EMPTY;
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
      return subFieldData.replace(substring, EMPTY);
    } else {
      return subFieldData;
    }
  }
}
