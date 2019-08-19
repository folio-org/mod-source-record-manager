package org.folio.services.mappers.processor;

import com.google.common.base.Splitter;
import io.vertx.core.json.JsonObject;

import java.util.Iterator;
import java.util.List;

/**
 * Run a splitter on a string or run a function.
 */
public class NormalizationFunctions {

  private static final String CHAR_SELECT = "char_select";
  private static final String REMOVE_ENDING_PUNC = "remove_ending_punc";
  private static final String TRIM = "trim";
  private static final String TRIM_PERIOD = "trim_period";
  private static final String SPLIT_FUNCTION_SPLIT_EVERY = "split_every";
  private static final String ADD_PREFIX = "add_prefix";
  private static final String ADD_PREFIX_IF_SUBFIELD_STARTS_WITH = "add_prefix_if_subfield_starts_with";
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
  public static Iterator<String> runSplitFunction(String funcName, String val, String param) {
    if (val == null) {
      return null;
    }
    if (SPLIT_FUNCTION_SPLIT_EVERY.equals(funcName)) {
      return splitEvery(val, param);
    }
    return null;
  }

  /**
   * Run the function funcName on val and param.
   *
   * @return the function's result
   */
  public static String runFunction(String functionName, String subField, JsonObject parameter) {
    if (subField == null) {
      return "";
    }
    if (CHAR_SELECT.equals(functionName)) {
      return charSelect(subField, parameter);
    } else if (REMOVE_ENDING_PUNC.equals(functionName) && !subField.equals("")) {
      return removeEndingPunc(subField);
    } else if (TRIM.equals(functionName)) {
      return subField.trim();
    } else if (TRIM_PERIOD.equals(functionName)) {
      return trimPeriod(subField);
    } else if (ADD_PREFIX.equals(functionName)) {
      return addPrefix(subField, parameter);
    } else if (ADD_PREFIX_IF_SUBFIELD_STARTS_WITH.equals(functionName)) {
      return addPrefixIfSubfieldStartsWith(subField, parameter);
    }
    return "";
  }

  private static Iterator<String> splitEvery(String val, String param) {
    return Splitter.fixedLength(Integer.parseInt(param)).split(val).iterator();
  }

  private static String charSelect(String subField, JsonObject parameter) {
    Integer from = parameter.getInteger("from");
    Integer to = parameter.getInteger("to");
    return subField.substring(from, to);
  }

  private static String trimPeriod(final String subField) {
    if (subField.endsWith(".")) {
      return subField.substring(0, subField.length() - 1);
    }
    return subField;
  }

  private static String removeEndingPunc(String subField) {
    return modified(subField, (subField.length() - 1));
  }

  private static String modified(final String subField, int pos) {
    if (PUNCT_2_REMOVE.contains(String.valueOf(subField.charAt(pos)))) {
      return subField.substring(0, subField.length() - 1);
    } else if (subField.charAt(pos) == '.') {
      try {
        if (subField.substring(subField.length() - 4).equals("....")) {
          return subField.substring(0, subField.length() - 1);
        } else if (subField.substring(subField.length() - 3).equals("...")) {
          return subField;
        } else {
          //if ends with .. or . remove just one .
          return subField.substring(0, subField.length() - 1);
        }
      } catch (IndexOutOfBoundsException ioob) {
        return subField;
      }
    }
    return subField;
  }

  private static String addPrefix(String subField, JsonObject parameter) {
    String prefix = parameter.getString("prefix");
    return prefix + subField;
  }

  private static String addPrefixIfSubfieldStartsWith(String subField, JsonObject parameter) {
    List<String> startsWith = parameter.getJsonArray("startsWith").getList();
    String prefixTrue = parameter.getString("prefixTrue");
    String prefixFalse = parameter.getString("prefixFalse");
    if (startsWith.stream().anyMatch(prefix -> subField.startsWith(prefix))) {
      return prefixTrue + subField;
    } else {
      return prefixFalse + subField;
    }
  }
}
