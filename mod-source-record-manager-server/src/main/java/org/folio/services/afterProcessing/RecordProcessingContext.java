package org.folio.services.afterProcessing;

import org.folio.rest.jaxrs.model.Record;

import java.util.ArrayList;
import java.util.List;

/**
 * Class for holding records and their related properties
 */
public class RecordProcessingContext {

  private List<RecordContext> recordsContext = new ArrayList<>();
  private Record.RecordType recordsType;

  public RecordProcessingContext(Record.RecordType recordsType) {
    this.recordsType = recordsType;
  }

  public List<RecordContext> getRecordsContext() {
    return recordsContext;
  }

  public Record.RecordType getRecordsType() {
    return recordsType;
  }

  public void addRecordContext(String recordId, String instanceId) {
    recordsContext.add(new RecordContext(recordId, instanceId));
  }

  public class RecordContext {
    private String recordId;
    private String instanceId;

    public RecordContext(String recordId, String instanceId) {
      this.recordId = recordId;
      this.instanceId = instanceId;
    }

    public String getRecordId() {
      return recordId;
    }

    public void setRecordId(String recordId) {
      this.recordId = recordId;
    }

    public String getInstanceId() {
      return instanceId;
    }

    public void setInstanceId(String instanceId) {
      this.instanceId = instanceId;
    }
  }
}
