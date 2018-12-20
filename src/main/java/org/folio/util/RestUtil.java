package org.folio.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.http.CaseInsensitiveHeaders;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import java.util.Map;

/**
 * Util class with static method for sending http request
 */
public class RestUtil {

  public static final String OKAPI_TENANT_HEADER = "x-okapi-tenant";
  public static final String OKAPI_TOKEN_HEADER = "x-okapi-token";
  public static final String OKAPI_URL_HEADER = "x-okapi-url";
  public static final int CREATED_STATUS_CODE = 201;
  private static final String HTTP_ERROR_MESSAGE = "Response HTTP code not equals 200. Response code: ";
  private static final Logger LOGGER = LoggerFactory.getLogger(RestUtil.class);

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

  /**
   * Validate http response and fail future if need
   *
   * @param asyncResult - http response callback
   * @param future      - future of callback
   * @return - boolean value is response ok
   */
  public static boolean validateAsyncResult(AsyncResult<WrappedResponse> asyncResult, Future future) {
    if (asyncResult.failed()) {
      LOGGER.error("Error during HTTP request to source-storage", asyncResult.cause());
      future.fail(asyncResult.cause());
      return false;
    } else if (asyncResult.result() == null) {
      LOGGER.error("Error during get response from source-storage");
      future.fail(new BadRequestException());
      return false;
    } else if (asyncResult.result().getCode() == 404) {
      LOGGER.error(HTTP_ERROR_MESSAGE + asyncResult.result().getCode());
      future.fail(new NotFoundException());
      return false;
    } else if (asyncResult.result().getCode() == 500) {
      LOGGER.error(HTTP_ERROR_MESSAGE + asyncResult.result().getCode());
      future.fail(new InternalServerErrorException());
      return false;
    } else if (asyncResult.result().getCode() == 200 || asyncResult.result().getCode() == 201) {
      return true;
    }
    LOGGER.error(HTTP_ERROR_MESSAGE + asyncResult.result().getCode());
    future.fail(new BadRequestException());
    return false;
  }
}
