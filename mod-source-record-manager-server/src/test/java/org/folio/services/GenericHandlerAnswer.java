package org.folio.services;

import io.vertx.core.Handler;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Answer for mocking handler results
 */
public class GenericHandlerAnswer<H, R> implements Answer<R> {

  private H handlerResult;
  private int argumentIndex;
  private R returnResult;

  /**
   * Constructor
   *
   * @param handlerResult        result to pass to handler
   * @param handlerArgumentIndex index of handler in mocked method
   */
  public GenericHandlerAnswer(H handlerResult, int handlerArgumentIndex) {
    this.handlerResult = handlerResult;
    this.argumentIndex = handlerArgumentIndex;
  }

  /**
   * Constructor
   *
   * @param handlerResult        result to pass to handler
   * @param handlerArgumentIndex index of handler in mocked method
   * @param returnResult         result to return
   */
  public GenericHandlerAnswer(H handlerResult, int handlerArgumentIndex, R returnResult) {
    this(handlerResult, handlerArgumentIndex);
    this.returnResult = returnResult;
  }


  @Override
  public R answer(InvocationOnMock invocation) {
    Handler<H> handler = invocation.getArgument(argumentIndex);
    handler.handle(handlerResult);
    return returnResult;
  }
}
