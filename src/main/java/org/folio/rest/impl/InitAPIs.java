package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.serviceproxy.ServiceBinder;
import org.folio.rest.resource.interfaces.InitAPI;
import org.folio.services.provider.MetadataService;
import org.folio.services.repository.MetadataRepository;
import org.folio.util.SourceRecordManagerConstants;

/**
 * Performs preprocessing operations before the verticle is deployed,
 * e.g. components registration, initializing, binding.
 */
public class InitAPIs implements InitAPI {

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> handler) {
    new ServiceBinder(vertx)
      .setAddress(SourceRecordManagerConstants.METADATA_SERVICE_ADDRESS)
      .register(MetadataService.class, MetadataService.create(vertx));
    new ServiceBinder(vertx)
      .setAddress(SourceRecordManagerConstants.METADATA_REPOSITORY_ADDRESS)
      .register(MetadataRepository.class, MetadataRepository.create(vertx));

    handler.handle(Future.succeededFuture(true));
  }
}
