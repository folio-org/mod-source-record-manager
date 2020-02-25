package org.folio.services.mappers.processor.functions;

import com.google.common.base.Splitter;

import org.apache.commons.lang.StringUtils;
import org.folio.services.mappers.processor.RuleExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Run a splitter on a string or run a function.
 */
public class NormalizationFunctionRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(NormalizationFunctionRunner.class);
  private static final String SPLIT_FUNCTION_SPLIT_EVERY = "split_every";

  private NormalizationFunctionRunner() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * Split val into chunks of param characters if funcName is "split_every".
   * Return null if val is null or funcName is not "split_every".
   *
   * @return the chunks
   */
  public static Iterator<String> runSplitFunction(String funcName, String subFieldData, String param) {
    if (SPLIT_FUNCTION_SPLIT_EVERY.equalsIgnoreCase(funcName) && subFieldData != null) {
      return Splitter.fixedLength(Integer.parseInt(param)).split(subFieldData).iterator();
    }
    return null;
  }

  /**
   * Run the function funcName on val and param.
   *
   * @return the function's result
   */
  public static String runFunction(String functionName, RuleExecutionContext ruleExecutionContext) {
    try {
      String result = NormalizationFunction.valueOf(functionName.trim().toUpperCase()).apply(ruleExecutionContext);
      if(result.equals(StringUtils.EMPTY)){
        LOGGER.debug("Result of {} function is empty", functionName);
      }
      return result;
    } catch (RuntimeException e) {
      LOGGER.error("Error while running normalization functions", e);
      return ruleExecutionContext.getSubFieldValue();
    }
  }
}
