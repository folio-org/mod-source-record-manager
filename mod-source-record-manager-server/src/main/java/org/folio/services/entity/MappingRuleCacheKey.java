package org.folio.services.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;

import org.folio.Record;


@AllArgsConstructor
@Getter
public class MappingRuleCacheKey {
  private String tenantId;

  private Record.RecordType recordType;
}
