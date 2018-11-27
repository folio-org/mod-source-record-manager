package org.folio.util;

import io.vertx.core.Vertx;
import io.vertx.core.http.CaseInsensitiveHeaders;

import java.util.Map;

import static org.folio.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.util.RestUtil.OKAPI_TOKEN_HEADER;
import static org.folio.util.RestUtil.OKAPI_URL_HEADER;

/**
 * Wrapper class for Okapi connection params
 */
public class OkapiConnectionParams {

  private static final int DEF_TIMEOUT = 2000;
  private String okapiUrl;
  private String tenantId;
  private String token;
  private Vertx vertx;
  private Integer timeout;
  private CaseInsensitiveHeaders headers = new CaseInsensitiveHeaders();

  public OkapiConnectionParams(Map<String, String> okapiHeaders, Vertx vertx, Integer timeout) {
    this.okapiUrl = okapiHeaders.getOrDefault(OKAPI_URL_HEADER, "localhost");
    this.tenantId = okapiHeaders.getOrDefault(OKAPI_TENANT_HEADER, "");
    this.token = okapiHeaders.getOrDefault(OKAPI_TOKEN_HEADER, "dummy");
    this.vertx = vertx;
    this.timeout = timeout != null ? timeout : DEF_TIMEOUT;
    this.headers.addAll(okapiHeaders);
  }

  public OkapiConnectionParams(Map<String, String> okapiHeaders, Vertx vertx) {
    this(okapiHeaders, vertx, null);
  }

  public String getOkapiUrl() {
    return okapiUrl;
  }

  public String getTenantId() {
    return tenantId;
  }

  public String getToken() {
    return token;
  }

  public Vertx getVertx() {
    return vertx;
  }

  public int getTimeout() {
    return timeout;
  }

  public CaseInsensitiveHeaders getHeaders() {
    return headers;
  }

  public void setHeaders(CaseInsensitiveHeaders headers) {
    this.headers = headers;
  }
}
