package org.folio.verticle.periodic;

import io.vertx.core.AbstractVerticle;
import org.folio.spring.SpringContextUtil;
import org.springframework.context.support.AbstractApplicationContext;

/**
 * Abstract class that provides common info for running periodic jobs.
 * Encapsulate all related spring stuff in this class.
 * Subclasses will work only with business tasks.
 */
public abstract class AbstractPeriodicJobVerticle extends AbstractVerticle {
  private static AbstractApplicationContext springGlobalContext;

  //TODO: get rid of this workaround with global spring context
  public static void setSpringGlobalContext(AbstractApplicationContext springGlobalContext) {
    AbstractPeriodicJobVerticle.springGlobalContext = springGlobalContext;
  }

  @Override
  public void start() {
    context.put("springContext", springGlobalContext);
    SpringContextUtil.autowireDependencies(this, context);

    startPeriodicJob();
  }

  /**
   * Setup periodic job to execution.
   */
  protected abstract void startPeriodicJob();
}
