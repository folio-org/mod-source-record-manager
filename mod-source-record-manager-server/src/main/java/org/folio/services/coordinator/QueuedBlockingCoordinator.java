package org.folio.services.coordinator;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Implementation of the BlockingCoordinator based on bounded blocking queue {@link ArrayBlockingQueue}:
 * the producing thread will keep inserting the new objects into the queue calling {@link #acceptLock()}
 * until the queue reaches some upper bound on what it can contain.
 * If the blocking queue reaches its upper limit,
 * the producing thread gets blocked while trying to insert the new object.
 * It remains blocked until a consuming thread takes an object out of the queue calling {@link #acceptUnlock()}.
 */
public class QueuedBlockingCoordinator implements BlockingCoordinator {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueuedBlockingCoordinator.class);
  private static final Object QUEUE_ITEM = new Object();
  private BlockingQueue<Object> blockingQueue = null;

  public QueuedBlockingCoordinator(int limit) {
    blockingQueue = new ArrayBlockingQueue<>(limit, true);
  }

  @Override
  public void acceptLock() {
    try {
      blockingQueue.put(QUEUE_ITEM);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.info("Failed to accept lock. The current thread {} is interrupted. Cause: {}", Thread.currentThread().getName(), e.getCause());
    }
  }

  @Override
  public void acceptUnlock() {
    try {
      if (!blockingQueue.isEmpty()) {
        blockingQueue.take();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.info("Failed to accept unlock. The current thread {} is interrupted. Cause: {}", Thread.currentThread().getName(), e.getCause());
    }
  }
}
