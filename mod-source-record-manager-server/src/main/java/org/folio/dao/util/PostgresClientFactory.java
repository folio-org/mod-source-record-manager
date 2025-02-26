
package org.folio.dao.util;

import io.vertx.core.Vertx;
import lombok.Getter;
import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Getter
@Component
public class PostgresClientFactory {

  private final Vertx vertx;

  public PostgresClientFactory(@Autowired Vertx vertx) {
    this.vertx = vertx;
  }

  /**
   * Creates instance of Postgres Client
   *
   * @param tenantId tenant id
   * @return Postgres Client
   */
  public PostgresClient createInstance(String tenantId) {
    return PostgresClient.getInstance(vertx, tenantId);
  }
}
