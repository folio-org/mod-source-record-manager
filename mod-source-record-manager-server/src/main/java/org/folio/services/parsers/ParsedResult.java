package org.folio.services.parsers;

import io.vertx.core.json.JsonObject;

/**
 * Wrapper for parsing result
 */
public class ParsedResult {

  /**
   * Record in json representation
   */
  private JsonObject parsedRecord;

  /**
   * Errors array in json representation
   */
  private JsonObject errors;

  /**
   * Flag of has parsed result errors or not
   */
  private boolean isHasError = false;

  public JsonObject getParsedRecord() {
    return parsedRecord;
  }

  public void setParsedRecord(JsonObject parsedRecord) {
    this.parsedRecord = parsedRecord;
  }

  public JsonObject getErrors() {
    return errors;
  }

  public void setErrors(JsonObject errors) {
    this.errors = errors;
  }

  public boolean isHasError() {
    return errors != null && !errors.isEmpty();
  }

  public void setHasError(boolean hasError) {
    isHasError = hasError;
  }

  @Override
  public String toString() {
    return "ParsedResult{" +
      "parsedRecord=" + parsedRecord +
      ", errors=" + errors +
      ", isHasError=" + isHasError +
      '}';
  }
}
