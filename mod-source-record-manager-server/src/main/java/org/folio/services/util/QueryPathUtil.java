package org.folio.services.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.folio.Record;

public final class QueryPathUtil {
  private static final Map<String, Record.RecordType> queryToRecordTypeMap;

  static {
    queryToRecordTypeMap = new HashMap<>();
    queryToRecordTypeMap.put("marc-bib", Record.RecordType.MARC_BIB);
    queryToRecordTypeMap.put("marc-holdings", Record.RecordType.MARC_HOLDING);
    queryToRecordTypeMap.put("marc-authority", Record.RecordType.MARC_AUTHORITY);
  }

  private QueryPathUtil() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Convert string query param to {@link Record.RecordType}
   *
   * @param queryParam String param from URL path
   * @return org.folio.Record.RecordType
   */
  public static Optional<Record.RecordType> toRecordType(String queryParam) {
    return Optional.ofNullable(queryToRecordTypeMap.get(queryParam));
  }
}
