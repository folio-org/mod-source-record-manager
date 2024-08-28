package org.folio.services.journal;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

import java.util.Collection;

/**
 * Codec that won't cause any serialization
 */
public class BatchableJournalRecordCodec implements MessageCodec<Collection<BatchableJournalRecord>, Collection<BatchableJournalRecord>> {
  @Override
  public void encodeToWire(Buffer buffer, Collection<BatchableJournalRecord> journalRecords) {
    throw new UnsupportedOperationException(journalRecords.toString());
  }

  @Override
  public Collection<BatchableJournalRecord> decodeFromWire(int pos, Buffer buffer) {
    throw new UnsupportedOperationException(buffer.toString());
  }

  @Override
  public Collection<BatchableJournalRecord> transform(Collection<BatchableJournalRecord> journalRecords) {
    return journalRecords;
  }

  @Override
  public String name() {
    return BatchableJournalRecordCodec.class.getSimpleName();
  }

  @Override
  public byte systemCodecID() {
    return -1;
  }
}
