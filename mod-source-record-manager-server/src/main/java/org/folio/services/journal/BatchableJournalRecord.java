package org.folio.services.journal;

import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.JournalRecord;

public class BatchableJournalRecord {
  private final JournalRecord journalRecord;
  private final String tenantId;

  public BatchableJournalRecord(JournalRecord journalRecord, String tenantId) {
    if (StringUtils.isBlank(tenantId)) {
      throw new IllegalArgumentException("Tenant ID must be set");
    }
    this.journalRecord = journalRecord;
    this.tenantId = tenantId;
  }

  public JournalRecord getJournalRecord() {
    return journalRecord;
  }

  public String getTenantId() { return tenantId; }
}
