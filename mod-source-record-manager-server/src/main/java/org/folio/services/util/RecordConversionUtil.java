package org.folio.services.util;

import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Record;

public class RecordConversionUtil {
  public static final String RECORDS = "records";

  private RecordConversionUtil() { }

  /**
   * Gets entity type by the record type.
   *
   * @param record the incoming record
   * @return entity type
   */
  public static EntityType getEntityType(Record record) {
    switch (record.getRecordType()) {
      case MARC_BIB:
        return EntityType.MARC_BIBLIOGRAPHIC;
      case MARC_AUTHORITY:
        return EntityType.MARC_AUTHORITY;
      case MARC_HOLDING:
        return EntityType.MARC_HOLDINGS;
      case EDIFACT:
      default:
        return EntityType.EDIFACT_INVOICE;
    }
  }
}
