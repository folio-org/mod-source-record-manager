package org.folio.services.mappers.processor;

import io.vertx.core.json.JsonObject;
import org.folio.services.mappers.processor.parameters.MappingParameters;
import org.marc4j.marc.DataField;

/**
 * Class serves as context to store parameters for rule execution
 */
public class RuleExecutionContext {

  private DataField dataField;
  private String subFieldValue;
  private MappingParameters mappingParameters;
  private JsonObject ruleParameter;


  public DataField getDataField() {
    return dataField;
  }

  public void setDataField(DataField dataField) {
    this.dataField = dataField;
  }

  public String getSubFieldValue() {
    return subFieldValue;
  }

  public void setSubFieldValue(String subFieldValue) {
    this.subFieldValue = subFieldValue;
  }

  public JsonObject getRuleParameter() {
    return ruleParameter;
  }

  public void setRuleParameter(JsonObject ruleParameter) {
    this.ruleParameter = ruleParameter;
  }

  public MappingParameters getMappingParameters() {
    return mappingParameters;
  }

  public void setMappingParameters(MappingParameters mappingParameters) {
    this.mappingParameters = mappingParameters;
  }
}
