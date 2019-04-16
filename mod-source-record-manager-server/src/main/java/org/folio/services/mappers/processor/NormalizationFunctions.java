package org.folio.services.mappers.processor;

import com.google.common.base.Splitter;

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
  public static String runFunction(String funcName, String val, String param){
    if(val == null){
      return "";
    }
    if(CHAR_SELECT.equals(funcName)){
      return charSelect(val, param);
    }
    else if(REMOVE_ENDING_PUNC.equals(funcName)){
      return removeEndingPunc(val);
    }
    else if(TRIM.equals(funcName)){
      return val.trim();
    }
    else if(TRIM_PERIOD.equals(funcName)){
      return trimPeriod(val);
    }
    return "";
  }

  private static Iterator<String> splitEvery(String val, String param) {
    return Splitter.fixedLength(Integer.parseInt(param)).split(val).iterator();
  }

  private static String charSelect(String val, String pos){
    try{
      if(pos.contains("-")){
        String []range = pos.split("-");
        return val.substring(Integer.parseInt(range[0]), Integer.parseInt(range[1])+1);
      }
      int p = Integer.parseInt(pos);
      return val.substring(p,p+1);
    }
    catch(Exception e){
      return val;
    }
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
}
