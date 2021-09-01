package org.folio.services.entity;

import javax.ws.rs.BadRequestException;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import org.folio.Record;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.ParsedRecordDto;

@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class MappingRuleCacheKey {
  private String tenantId;
  private Record.RecordType recordType;

  private static final String ERROR_MESSAGE = "Only marc-bib or marc-holdings supported";

  public MappingRuleCacheKey(String tenantId, ParsedRecordDto.RecordType recordType) {
    this.tenantId = tenantId;
    if (ParsedRecordDto.RecordType.MARC_BIB.equals(recordType)) {
      this.recordType = Record.RecordType.MARC_BIB;
    } else if (ParsedRecordDto.RecordType.MARC_HOLDING.equals(recordType)) {
      this.recordType = Record.RecordType.MARC_HOLDING;
    } else {
      throw new BadRequestException(ERROR_MESSAGE);
    }
  }

  public MappingRuleCacheKey(String tenantId, JournalRecord.EntityType entityType) {
    this.tenantId = tenantId;
    if (JournalRecord.EntityType.MARC_BIBLIOGRAPHIC.equals(entityType)) {
      this.recordType = Record.RecordType.MARC_BIB;
    } else if (JournalRecord.EntityType.MARC_HOLDINGS.equals(entityType)) {
      this.recordType = Record.RecordType.MARC_HOLDING;
    } else {
      throw new BadRequestException(ERROR_MESSAGE);
    }
  }

  public MappingRuleCacheKey(String tenantId, org.folio.rest.jaxrs.model.Record.RecordType recordType) {
    this.tenantId = tenantId;
    if (org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB.equals(recordType)) {
      this.recordType = Record.RecordType.MARC_BIB;
    } else if (org.folio.rest.jaxrs.model.Record.RecordType.MARC_HOLDING.equals(recordType)) {
      this.recordType = Record.RecordType.MARC_HOLDING;
    } else {
      throw new BadRequestException(ERROR_MESSAGE);
    }
  }
}
