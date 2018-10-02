package org.folio.rest.impl;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.RestVerticle;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class RestVerticleTest {

  private Vertx vertx;
  private int port;

  @Before
  public void setUp(TestContext context) {
    vertx = Vertx.vertx();
    port = NetworkUtils.nextFreePort();
    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject().put("http.port", port));
    vertx.deployVerticle(RestVerticle.class.getName(), options, context.asyncAssertSuccess());
  }

  @After
  public void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void testGetStub(TestContext context) {
    //TODO Replace testing stub
    final Async async = context.async();
    String url = "http://localhost:" + port;
    String testUrl = url + "/source-record-manager-ping";

    Handler<HttpClientResponse> handler = response -> {
      context.assertEquals(response.statusCode(), 200);
      context.assertEquals(response.headers().get("content-type"), "application/json");
      async.complete();
    };
    sendRequest(testUrl, HttpMethod.GET, handler);
  }

  private void sendRequest(String url, HttpMethod method, Handler<HttpClientResponse> handler) {
    sendRequest(url, method, handler, "");
  }

  private void sendRequest(String url, HttpMethod method, Handler<HttpClientResponse> handler, String content) {
    Buffer buffer = Buffer.buffer(content);
    vertx.createHttpClient()
      .requestAbs(method, url, handler)
      .putHeader("x-okapi-tenant", "diku")
      .putHeader("Accept", "application/json,text/plain")
      .end(buffer);
  }
}
