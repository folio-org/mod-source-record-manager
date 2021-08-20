package org.folio.services.util;

import javax.ws.rs.BadRequestException;

import org.folio.Record;

public class QueryPathUtil {
  /**
   * Convert Sttong param to Folio RecordType enum
   *
   * @param recordType String param from URL path
   * @return org.folio.Record.RecordType
   */
  public static Record.RecordType convert(String recordType){
    switch (recordType){
      case "marc-bib": return Record.RecordType.MARC_BIB;
      case "marc-holdings": return Record.RecordType.MARC_HOLDING;
      default: throw new BadRequestException("Only marc-bib or marc-holdings supports");
    }
  }
}
