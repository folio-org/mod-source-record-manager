package org.folio.services.parsers;

import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.JobProfileInfo.DataType;
import org.folio.rest.jaxrs.model.Record;

/**
 * Source Record formats
 */
public enum RecordFormat {
  MARC_BIB("MARC_BIB", DataType.MARC_BIB),
  MARC_AUTHORITY("MARC_AUTHORITY", DataType.MARC_AUTHORITY),
  MARC_HOLDING("MARC_HOLDING", DataType.MARC_HOLDING);

  private String format;
  private JobProfileInfo.DataType dataType;

  RecordFormat(String format, JobProfileInfo.DataType dataType) {
    this.dataType = dataType;
    this.format = format;
  }

  public JobProfileInfo.DataType getDataType() {
    return dataType;
  }

  public String getFormat() {
    return format;
  }

  public static RecordFormat getByDataType(JobProfileInfo.DataType dataType) {
    for (RecordFormat format : RecordFormat.values()) {
      if (format.getDataType().equals(dataType)) {
        return format;
      }
    }
    return null;
  }

  public static RecordFormat getByDataType(Record.RecordType recordType) {
    return getByDataType(JobProfileInfo.DataType.fromValue(recordType.value()));
  }
}
