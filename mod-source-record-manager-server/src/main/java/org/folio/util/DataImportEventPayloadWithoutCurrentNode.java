package org.folio.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.folio.DataImportEventPayload;

/**
 * Another version of DataImportEventPayload that will not deserialize currentNode and currentNodePath.
 * currentNode is expensive in CPU and memory. If business logic does not require the use of currentNode,
 * this class can be used for deserialization instead.
 */
@JsonIgnoreProperties({"currentNode", "currentNodePath"})
public class DataImportEventPayloadWithoutCurrentNode extends DataImportEventPayload {

}
