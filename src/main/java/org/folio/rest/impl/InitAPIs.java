package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.SharedData;
import org.folio.rest.jaxrs.model.PingMessage;
import org.folio.rest.resource.interfaces.InitAPI;

public class InitAPIs implements InitAPI {

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> handler) {
    //TODO replace stub init
    SharedData sharedData = vertx.sharedData();
    LocalMap<String, JsonObject> pingMessages = sharedData.getLocalMap("pingMessages");
    pingMessages.put("094fdeac-97ad-4f76-9cad-d5be9f3c3759",
      JsonObject.mapFrom(new PingMessage()
        .withId("094fdeac-97ad-4f76-9cad-d5be9f3c3759")
        .withMessage("Ping works")));
    pingMessages.put("3a24fdbf-e6ad-43ff-afd6-54f9517e5d17",
      JsonObject.mapFrom(new PingMessage()
        .withId("3a24fdbf-e6ad-43ff-afd6-54f9517e5d17")
        .withMessage("Ping works again")));

    handler.handle(Future.succeededFuture(true));
  }
}
