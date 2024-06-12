package org.folio.services.progress;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

/**
 * Codec that won't cause any serialization/deserialization. It should only be used locally, exception will be thrown if
 * message need to be sent over the wire for a clustered event bus.
 */
public class BatchableJobExecutionProgressCodec implements MessageCodec<BatchableJobExecutionProgress, BatchableJobExecutionProgress> {
  @Override
  public void encodeToWire(Buffer buffer, BatchableJobExecutionProgress progress) {
    throw new UnsupportedOperationException(progress.toString());
  }

  @Override
  public BatchableJobExecutionProgress decodeFromWire(int pos, Buffer buffer) {
    throw new UnsupportedOperationException(buffer.toString());
  }

  @Override
  public BatchableJobExecutionProgress transform(BatchableJobExecutionProgress progress) {
    return progress;
  }

  @Override
  public String name() {
    return BatchableJobExecutionProgressCodec.class.getSimpleName();
  }

  @Override
  public byte systemCodecID() {
    return -1;
  }
}
