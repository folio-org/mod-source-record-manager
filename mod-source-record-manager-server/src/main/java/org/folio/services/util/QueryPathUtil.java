package org.folio.services.util;

import javax.ws.rs.BadRequestException;

import org.folio.Record;

public class QueryPathUtil {

  private QueryPathUtil() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Convert string query param to {@link Record.RecordType}
   *
   * @param recordType String param from URL path
   * @return org.folio.Record.RecordType
   */
  public static Record.RecordType convert(String recordType) {
    return Optional.ofNullable(queryToRecordTypeMap.get(recordType));
  }
}
