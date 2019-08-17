package org.folio.services.mappers.processor;

import com.google.common.base.Splitter;
import io.vertx.core.json.JsonObject;

import java.util.Iterator;

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
  private static final String PUNCT_2_REMOVE = ";:,/+= ";

  private NormalizationFunctions() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * Split val into chunks of param characters if funcName is "split_every".
   * Return null if val is null or funcName is not "split_every".
   * @return the chunks
   */
  public static Iterator<String> runSplitFunction(String funcName, String val, String param){
    if(val == null){
      return null;
    }
    if(SPLIT_FUNCTION_SPLIT_EVERY.equals(funcName)){
      return splitEvery(val, param);
    }
    return null;
  }

  /**
   * Run the function funcName on val and param.
   * @return the function's result
   */
  public static String runFunction(String functionName, String fieldValue, JsonObject parameter){
    if(fieldValue == null){
      return "";
    }
    if(CHAR_SELECT.equals(functionName)){
      return charSelect(fieldValue, parameter);
    }
    else if(REMOVE_ENDING_PUNC.equals(functionName) && !fieldValue.equals("")){
      return removeEndingPunc(fieldValue);
    }
    else if(TRIM.equals(functionName)){
      return fieldValue.trim();
    }
    else if(TRIM_PERIOD.equals(functionName)){
      return trimPeriod(fieldValue);
    } else if (ADD_PREFIX.equals(functionName)) {
      return addPrefix(fieldValue, parameter);
    }
    return "";
  }

  private static Iterator<String> splitEvery(String val, String param) {
    return Splitter.fixedLength(Integer.parseInt(param)).split(val).iterator();
  }

  private static String charSelect(String fieldValue, JsonObject parameter){
    Integer from = parameter.getInteger("from");
    Integer to = parameter.getInteger("to");
    return fieldValue.substring(from, to);
  }

  private static String trimPeriod(final String input) {
    if (input.endsWith(".")) {
      return input.substring(0, input.length()-1);
    }
    return input;
  }

  private static String removeEndingPunc(String val){
    return modified(val, (val.length()-1));
  }

  private static String modified(final String input, int pos){
    if(PUNCT_2_REMOVE.contains(String.valueOf(input.charAt(pos)))){
      return input.substring(0, input.length()-1);
    }
    else if(input.charAt(pos) == '.'){
      try{
        if(input.substring(input.length()-4).equals("....")){
          return input.substring(0, input.length()-1);
        }
        else if(input.substring(input.length()-3).equals("...")){
          return input;
        }
        else {
          //if ends with .. or . remove just one .
          return input.substring(0, input.length()-1);
        }
      }
      catch(IndexOutOfBoundsException ioob){
        return input;
      }
    }
    return input;
  }

  private static String addPrefix(String fieldValue, JsonObject parameter) {
    String prefix = parameter.getString("prefix");
    return prefix + fieldValue;
  }
}
