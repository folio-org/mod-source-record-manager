package org.folio.services;

import io.vertx.core.Future;

import java.util.function.Supplier;

public class Try {

  public static <T> Future<T> itGet(Supplier<T> task) {
    Future<T> future = Future.future();
    try {
      future.complete(task.get());
    } catch (Exception e) {
      future.fail(e);
    }
    return future;
  }

  public static <T> Future<T> itDo(Job<Future<T>> job) {
    Future<T> future = Future.future();
    try {
      job.accept(future);
    } catch (Exception e) {
      future.fail(e);
    }
    return future;
  }

  @FunctionalInterface
  public interface Job<T> {
    void accept(T t) throws Exception;//NOSONAR
  }

}
