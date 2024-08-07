package org.folio.services.journal;

import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.JournalRecord;

public class BatchableJournalRecord {
  private final JournalRecord journalRecord;

  public BatchableJournalRecord(JournalRecord journalRecord) {
    if (StringUtils.isBlank(journalRecord.getTenantId())) {
      throw new IllegalArgumentException("Tenant ID must be set");
    }
    this.journalRecord = journalRecord;
  }

  public JournalRecord getJournalRecord() {
    return journalRecord;
  }
}
