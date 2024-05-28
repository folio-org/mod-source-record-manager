package org.folio.services.progress;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageProducer;

public class JobExecutionProgressUtil {

  public static final String BATCH_JOB_PROGRESS_ADDRESS = "batchJobProgress";

  /**
   * Creates a message producer that will enqueue {@link BatchableJobExecutionProgress} objects
   * to be processed asynchronously. Messages are only processed locally.
   */
  public static MessageProducer<BatchableJobExecutionProgress> getBatchJobProgressProducer(Vertx vertx) {
    return vertx.eventBus().sender(BATCH_JOB_PROGRESS_ADDRESS, new DeliveryOptions()
      .setCodecName(BatchableJobExecutionProgressCodec.class.getSimpleName())
      .setLocalOnly(true));
  }

}
