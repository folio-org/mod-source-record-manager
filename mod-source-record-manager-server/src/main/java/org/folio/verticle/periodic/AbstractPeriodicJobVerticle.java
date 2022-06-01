package org.folio.verticle.periodic;

import io.vertx.core.AbstractVerticle;

/**
 * Abstract class that provides common info for running periodic jobs.
 * Subclasses will work only with business tasks of running periodic jobs.
 */
public abstract class AbstractPeriodicJobVerticle extends AbstractVerticle {
  protected long timerId;

  @Override
  public void start() {
    timerId = vertx.setPeriodic(getExecutionIntervalInMs(), handler -> executePeriodicJob());
  }

  @Override
  public void stop() throws Exception {
    vertx.cancelTimer(timerId);
    super.stop();
  }

  /**
   * Setups periodic job interval in milliseconds.
   */
  protected abstract long getExecutionIntervalInMs();

  /**
   * Executes periodic job.
   */
  protected abstract void executePeriodicJob();
}
