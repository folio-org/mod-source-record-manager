package org.folio.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.http.CaseInsensitiveHeaders;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;

import java.util.Map;

/**
 * Util class with static method for sending http request
 */
public class RestUtil {

  public static final String OKAPI_TENANT_HEADER = "x-okapi-tenant";
  public static final String OKAPI_TOKEN_HEADER = "x-okapi-token";
  public static final String OKAPI_URL_HEADER = "x-okapi-url";
  public static final int CREATED_STATUS_CODE = 201;

  public static class WrappedResponse {
    private int code;
    private String body;
    private JsonObject json;
    private HttpClientResponse response;

    WrappedResponse(int code, String body,
                    HttpClientResponse response) {
      this.code = code;
      this.body = body;
      this.response = response;
      try {
        json = new JsonObject(body);
      } catch (Exception e) {
        json = null;
      }
    }

    public int getCode() {
      return code;
    }

    public String getBody() {
      return body;
    }

    public HttpClientResponse getResponse() {
      return response;
    }

    public JsonObject getJson() {
      return json;
    }
  }

  private RestUtil() {
  }

  /**
   * Create http request
   *
   * @param url     - url for http request
   * @param method  - http method
   * @param payload - body of request
   * @return - async http response
   */
  public static <T> Future<WrappedResponse> doRequest(OkapiConnectionParams params, String url,
                                                  HttpMethod method, T payload) {
    Future<WrappedResponse> future = Future.future();
    try {
      CaseInsensitiveHeaders headers = params.getHeaders();
      String requestUrl = params.getOkapiUrl() + url;
      HttpClientRequest request = getHttpClient(params).requestAbs(method, requestUrl);
      if (headers != null) {
        headers.add("Content-type", "application/json")
          .add("Accept", "application/json, text/plain");
        for (Map.Entry entry : headers.entries()) {
          request.putHeader((String) entry.getKey(), (String) entry.getValue());
        }
      }
      request.exceptionHandler(future::fail);
      request.handler(req -> req.bodyHandler(buf -> {
        WrappedResponse wr = new WrappedResponse(req.statusCode(), buf.toString(), req);
        future.complete(wr);
      }));
      if (method == HttpMethod.PUT || method == HttpMethod.POST) {
        request.end(new ObjectMapper().writeValueAsString(payload));
      } else {
        request.end();
      }
      return future;
    } catch (Exception e) {
      future.fail(e);
      return future;
    }
  }

  /**
   * Prepare HttpClient from OkapiConnection params
   *
   * @param params - Okapi connection params
   * @return - Vertx Http Client
   */
  private static HttpClient getHttpClient(OkapiConnectionParams params) {
    HttpClientOptions options = new HttpClientOptions();
    options.setConnectTimeout(params.getTimeout());
    options.setIdleTimeout(params.getTimeout());
    return params.getVertx().createHttpClient(options);
  }
}
