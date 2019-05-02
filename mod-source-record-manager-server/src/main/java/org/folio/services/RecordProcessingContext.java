package org.folio.services;

import org.folio.rest.jaxrs.model.Record;

import java.util.ArrayList;
import java.util.List;

/**
 * Context class for holding Records and their properties
 */
public class RecordProcessingContext {

  private List<RecordContext> recordsContext = new ArrayList<>();

  public RecordProcessingContext(List<Record> parsedRecords) {
    for (Record record : parsedRecords) {
      recordsContext.add(new RecordContext(record));
    }
  }

  public List<RecordContext> getRecordsContext() {
    return recordsContext;
  }

  public class RecordContext {
    private Record record;
    private String instanceId;

    public RecordContext(Record record) {
      this.record = record;
    }

    public Record getRecord() {
      return record;
    }

    public void setRecord(Record record) {
      this.record = record;
    }

    public String getInstanceId() {
      return instanceId;
    }

    public void setInstanceId(String instanceId) {
      this.instanceId = instanceId;
    }
  }
}
