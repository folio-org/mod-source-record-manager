package org.folio.services.entity;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import org.folio.Record;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.ParsedRecordDto;

@Getter
@EqualsAndHashCode
public class MappingRuleCacheKey {

  private String tenantId;
  private Record.RecordType recordType;

  public MappingRuleCacheKey(String tenantId, Record.RecordType recordType) {
    this.tenantId = tenantId;
    if (Record.RecordType.MARC_BIB.equals(recordType) || Record.RecordType.MARC_HOLDING.equals(recordType)
      || Record.RecordType.MARC_AUTHORITY.equals(recordType)) {
      this.recordType = recordType;
    }
  }

  public MappingRuleCacheKey(String tenantId, ParsedRecordDto.RecordType recordType) {
    this.tenantId = tenantId;
    if (ParsedRecordDto.RecordType.MARC_BIB.equals(recordType)) {
      this.recordType = Record.RecordType.MARC_BIB;
    } else if (ParsedRecordDto.RecordType.MARC_HOLDING.equals(recordType)) {
      this.recordType = Record.RecordType.MARC_HOLDING;
    } else if (ParsedRecordDto.RecordType.MARC_AUTHORITY.equals(recordType)) {
      this.recordType = Record.RecordType.MARC_AUTHORITY;
    }
  }

  public MappingRuleCacheKey(String tenantId, JournalRecord.EntityType entityType) {
    this.tenantId = tenantId;
    if (JournalRecord.EntityType.MARC_BIBLIOGRAPHIC.equals(entityType)) {
      this.recordType = Record.RecordType.MARC_BIB;
    } else if (JournalRecord.EntityType.MARC_HOLDINGS.equals(entityType)) {
      this.recordType = Record.RecordType.MARC_HOLDING;
    } else if (JournalRecord.EntityType.MARC_AUTHORITY.equals(entityType)) {
      this.recordType = Record.RecordType.MARC_AUTHORITY;
    }
  }

  public MappingRuleCacheKey(String tenantId, org.folio.rest.jaxrs.model.Record.RecordType recordType) {
    this.tenantId = tenantId;
    if (org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB.equals(recordType)) {
      this.recordType = Record.RecordType.MARC_BIB;
    } else if (org.folio.rest.jaxrs.model.Record.RecordType.MARC_HOLDING.equals(recordType)) {
      this.recordType = Record.RecordType.MARC_HOLDING;
    } else if (org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY.equals(recordType)) {
      this.recordType = Record.RecordType.MARC_AUTHORITY;
    }
  }
}
