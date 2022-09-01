package org.folio.services.mappers.processor;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;

@RunWith(VertxUnitRunner.class)
public class MappingParametersProviderTest {

  protected static final String IDENTIFIER_TYPES_URL = "/identifier-types?limit=0";
  protected static final String INSTANCE_TYPES_URL = "/instance-types?limit=0";
  protected static final String CLASSIFICATION_TYPES_URL = "/classification-types?limit=0";
  protected static final String INSTANCE_FORMATS_URL = "/instance-formats?limit=0";
  protected static final String CONTRIBUTOR_TYPES_URL = "/contributor-types?limit=0";
  protected static final String CONTRIBUTOR_NAME_TYPES_URL = "/contributor-name-types?limit=0";
  protected static final String ELECTRONIC_ACCESS_URL = "/electronic-access-relationships?limit=0";
  protected static final String INSTANCE_NOTE_TYPES_URL = "/instance-note-types?limit=0";
  protected static final String INSTANCE_ALTERNATIVE_TITLE_TYPES_URL =
    "/alternative-title-types?limit=0";
  protected static final String MODE_OF_ISSUANCE_TYPES_URL = "/modes-of-issuance?limit=0";
  protected static final String INSTANCE_STATUSES_URL = "/instance-statuses?limit=0";
  protected static final String NATURE_OF_CONTENT_TERMS_URL = "/nature-of-content-terms?limit=0";
  protected static final String INSTANCE_RELATIONSHIP_TYPES_URL =
    "/instance-relationship-types?limit=0";
  protected static final String HOLDINGS_TYPES_URL = "/holdings-types?limit=0";
  protected static final String HOLDINGS_NOTE_TYPES_URL = "/holdings-note-types?limit=0";
  protected static final String ILL_POLICIES_URL = "/ill-policies?limit=0";
  protected static final String CALL_NUMBER_TYPES_URL = "/call-number-types?limit=0";
  protected static final String STATISTICAL_CODES_URL = "/statistical-codes?limit=0";
  protected static final String STATISTICAL_CODE_TYPES_URL = "/statistical-code-types?limit=0";
  protected static final String LOCATIONS_URL = "/locations?limit=0";
  protected static final String MATERIAL_TYPES_URL = "/material-types?limit=0";
  protected static final String ITEM_DAMAGED_STATUSES_URL = "/item-damaged-statuses?limit=0";
  protected static final String LOAN_TYPES_URL = "/loan-types?limit=0";
  protected static final String ITEM_NOTE_TYPES_URL = "/item-note-types?limit=0";
  protected static final String AUTHORITY_NOTE_TYPES_URL = "/authority-note-types?limit=0";
  protected static final String AUTHORITY_SOURCE_FILES_URL = "/authority-source-files?limit=0";
  protected static final String FIELD_PROTECTION_SETTINGS_URL =
    "/field-protection-settings/marc?limit=0";
  protected static final String TENANT_CONFIGURATIONS_SETTINGS_URL =
    "/configurations/entries?query="
      + URLEncoder.encode(
      "(module==ORG and configName==localeSettings)", StandardCharsets.UTF_8);

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  @Rule
  public WireMockRule snapshotMockServer =
    new WireMockRule(
      WireMockConfiguration.wireMockConfig().dynamicPort().notifier(new ConsoleNotifier(false))
      //      .extensions(new AbstractRestTest.RequestToResponseTransformer())
    );

  private MappingParametersProvider mappingParametersProvider;
  private OkapiConnectionParams okapiConnectionParams;

  @Before
  public void setUp() throws Exception {
    Vertx vertx = rule.vertx();
    mappingParametersProvider = new MappingParametersProvider(vertx);

    HashMap<String, String> headers = new HashMap<>();
    headers.put(OKAPI_URL_HEADER, "http://localhost:" + snapshotMockServer.port());
    headers.put(OKAPI_TENANT_HEADER, "diku");
    headers.put(OKAPI_TOKEN_HEADER, "token");
    okapiConnectionParams = new OkapiConnectionParams(headers, vertx);

    WireMock.stubFor(
      get(IDENTIFIER_TYPES_URL)
        .willReturn(
          okJson(new JsonObject().put("identifierTypes", new JsonArray()).toString())));
    WireMock.stubFor(
      get(INSTANCE_TYPES_URL)
        .willReturn(okJson(new JsonObject().put("instanceTypes", new JsonArray()).toString())));
    WireMock.stubFor(
      get(CLASSIFICATION_TYPES_URL)
        .willReturn(
          okJson(new JsonObject().put("classificationTypes", new JsonArray()).toString())));
    WireMock.stubFor(
      get(ELECTRONIC_ACCESS_URL)
        .willReturn(
          okJson(
            new JsonObject()
              .put("electronicAccessRelationships", new JsonArray())
              .toString())));
    WireMock.stubFor(
      get(INSTANCE_FORMATS_URL)
        .willReturn(
          okJson(new JsonObject().put("instanceFormats", new JsonArray()).toString())));
    WireMock.stubFor(
      get(CONTRIBUTOR_NAME_TYPES_URL)
        .willReturn(
          okJson(new JsonObject().put("contributorNameTypes", new JsonArray()).toString())));
    WireMock.stubFor(
      get(CONTRIBUTOR_TYPES_URL)
        .willReturn(
          okJson(new JsonObject().put("contributorTypes", new JsonArray()).toString())));
    WireMock.stubFor(
      get(INSTANCE_NOTE_TYPES_URL)
        .willReturn(
          okJson(new JsonObject().put("instanceNoteTypes", new JsonArray()).toString())));
    WireMock.stubFor(
      get(INSTANCE_ALTERNATIVE_TITLE_TYPES_URL)
        .willReturn(
          okJson(new JsonObject().put("alternativeTitleTypes", new JsonArray()).toString())));
    WireMock.stubFor(
      get(MODE_OF_ISSUANCE_TYPES_URL)
        .willReturn(okJson(new JsonObject().put("issuanceModes", new JsonArray()).toString())));
    WireMock.stubFor(
      get(INSTANCE_STATUSES_URL)
        .willReturn(
          okJson(new JsonObject().put("instanceStatuses", new JsonArray()).toString())));
    WireMock.stubFor(
      get(NATURE_OF_CONTENT_TERMS_URL)
        .willReturn(
          okJson(new JsonObject().put("natureOfContentTerms", new JsonArray()).toString())));
    WireMock.stubFor(
      get(INSTANCE_RELATIONSHIP_TYPES_URL)
        .willReturn(
          okJson(
            new JsonObject()
              .put("instanceRelationshipTypes", new JsonArray())
              .toString())));
    WireMock.stubFor(
      get(HOLDINGS_TYPES_URL)
        .willReturn(okJson(new JsonObject().put("holdingsTypes", new JsonArray()).toString())));
    WireMock.stubFor(
      get(HOLDINGS_NOTE_TYPES_URL)
        .willReturn(
          okJson(new JsonObject().put("holdingsNoteTypes", new JsonArray()).toString())));
    WireMock.stubFor(
      get(ILL_POLICIES_URL)
        .willReturn(okJson(new JsonObject().put("illPolicies", new JsonArray()).toString())));
    WireMock.stubFor(
      get(CALL_NUMBER_TYPES_URL)
        .willReturn(
          okJson(new JsonObject().put("callNumberTypes", new JsonArray()).toString())));
    WireMock.stubFor(
      get(STATISTICAL_CODES_URL)
        .willReturn(
          okJson(new JsonObject().put("statisticalCodes", new JsonArray()).toString())));
    WireMock.stubFor(
      get(STATISTICAL_CODE_TYPES_URL)
        .willReturn(
          okJson(new JsonObject().put("statisticalCodeTypes", new JsonArray()).toString())));
    WireMock.stubFor(
      get(LOCATIONS_URL)
        .willReturn(okJson(new JsonObject().put("locations", new JsonArray()).toString())));
    WireMock.stubFor(
      get(MATERIAL_TYPES_URL)
        .willReturn(okJson(new JsonObject().put("mtypes", new JsonArray()).toString())));
    WireMock.stubFor(
      get(ITEM_DAMAGED_STATUSES_URL)
        .willReturn(
          okJson(new JsonObject().put("itemDamageStatuses", new JsonArray()).toString())));
    WireMock.stubFor(
      get(LOAN_TYPES_URL)
        .willReturn(okJson(new JsonObject().put("loantypes", new JsonArray()).toString())));
    WireMock.stubFor(
      get(ITEM_NOTE_TYPES_URL)
        .willReturn(okJson(new JsonObject().put("itemNoteTypes", new JsonArray()).toString())));
    WireMock.stubFor(
      get(AUTHORITY_NOTE_TYPES_URL)
        .willReturn(
          okJson(new JsonObject().put("authorityNoteTypes", new JsonArray()).toString())));
    WireMock.stubFor(
      get(AUTHORITY_SOURCE_FILES_URL)
        .willReturn(
          okJson(new JsonObject().put("authoritySourceFiles", new JsonArray()).toString())));
    WireMock.stubFor(
      get(FIELD_PROTECTION_SETTINGS_URL)
        .willReturn(
          okJson(
            new JsonObject()
              .put("marcFieldProtectionSettings", new JsonArray())
              .toString())));
    WireMock.stubFor(
      get(TENANT_CONFIGURATIONS_SETTINGS_URL)
        .willReturn(okJson(new JsonObject().put("configs", new JsonArray()).toString())));
  }

  @Test
  public void getItemFromCache(TestContext context) {
    Async async = context.async();

    mappingParametersProvider
      .get("1", okapiConnectionParams)
      .onComplete(
        ar -> {
          MappingParameters result = ar.result();
          context.assertNotNull(result);
          context.assertTrue(result.isInitialized());
          async.complete();
        });
  }

  /**
   * Test that multiple requests to get the same item concurrently do not attempt to load mapping parameters concurrently.
   * only one load should occur. All requests for the same cache key should return the same cache value.
   */
  @Test
  public void manyRequests(TestContext context) {
    Async async = context.async();
    String key = "1";
    CompositeFuture.all(mappingParametersProvider
          .get(key, okapiConnectionParams),
        mappingParametersProvider
          .get(key, okapiConnectionParams),
        mappingParametersProvider
          .get(key, okapiConnectionParams))
      .onComplete(ar -> {
        context.verify(v -> {
          // should verify all the endpoints were called once, but that is a lot.
          verify(1, getRequestedFor(urlEqualTo(IDENTIFIER_TYPES_URL)));

          // verify that the same mapping parameters is returned
          Assert.assertSame(ar.result().<MappingParameters>resultAt(0), ar.result().<MappingParameters>resultAt(1));
          Assert.assertSame(ar.result().<MappingParameters>resultAt(1), ar.result().<MappingParameters>resultAt(2));
        });
        async.complete();
      });
  }

  /**
   * Test that cache keys with different okapi connection params are the equal. This is critical for the cache to work
   * efficiently.
   */
  @Test
  public void cacheKeyEquality() {
    OkapiConnectionParams otherParams = new OkapiConnectionParams(Collections.emptyMap(), rule.vertx());
    Assert.assertNotEquals(okapiConnectionParams, otherParams);

    MappingParametersProvider.MappingParameterKey key1 =
      new MappingParametersProvider.MappingParameterKey("key",
        otherParams);
    MappingParametersProvider.MappingParameterKey key2 =
      new MappingParametersProvider.MappingParameterKey("key",
        okapiConnectionParams);
    Assert.assertEquals(key1, key2);
  }
}
