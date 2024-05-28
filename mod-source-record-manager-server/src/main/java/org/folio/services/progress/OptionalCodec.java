package org.folio.services.progress;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

import java.util.Optional;

/**
 * Codec that won't cause any serialization/deserialization
 */
public class OptionalCodec implements MessageCodec<Optional<?>, Optional<?>> {
  @Override
  public void encodeToWire(Buffer buffer, Optional progress) {
    throw new UnsupportedOperationException(progress.toString());
  }

  @Override
  public Optional<?> decodeFromWire(int pos, Buffer buffer) {
    throw new UnsupportedOperationException(buffer.toString());
  }

  @Override
  public Optional<?> transform(Optional optional) {
    return optional;
  }

  @Override
  public String name() {
    return OptionalCodec.class.getSimpleName();
  }

  @Override
  public byte systemCodecID() {
    return -1;
  }
}
