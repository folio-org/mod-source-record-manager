package org.folio.services.mappers.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Objects;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import io.vertx.core.json.jackson.DatabindCodec;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.AlternativeTitleType;
import org.folio.Alternativetitletypes;
import org.folio.AuthorityNoteType;
import org.folio.AuthoritySourceFile;
import org.folio.Authoritynotetypes;
import org.folio.Authoritysourcefiles;
import org.folio.CallNumberType;
import org.folio.Callnumbertypes;
import org.folio.ClassificationType;
import org.folio.Classificationtypes;
import org.folio.ContributorNameType;
import org.folio.ContributorType;
import org.folio.Contributornametypes;
import org.folio.Contributortypes;
import org.folio.ElectronicAccessRelationship;
import org.folio.Electronicaccessrelationships;
import org.folio.HoldingsNoteType;
import org.folio.HoldingsType;
import org.folio.Holdingsnotetypes;
import org.folio.Holdingstypes;
import org.folio.IdentifierType;
import org.folio.Identifiertypes;
import org.folio.IllPolicy;
import org.folio.Illpolicies;
import org.folio.InstanceFormat;
import org.folio.InstanceNoteType;
import org.folio.InstanceRelationshipType;
import org.folio.InstanceStatus;
import org.folio.InstanceType;
import org.folio.Instanceformats;
import org.folio.Instancenotetypes;
import org.folio.Instancerelationshiptypes;
import org.folio.Instancestatuses;
import org.folio.Instancetypes;
import org.folio.IssuanceMode;
import org.folio.Issuancemodes;
import org.folio.ItemDamageStatus;
import org.folio.ItemNoteType;
import org.folio.Itemdamagedstatuses;
import org.folio.Itemnotetypes;
import org.folio.LinkingRuleDto;
import org.folio.Loantype;
import org.folio.Loantypes;
import org.folio.Location;
import org.folio.Locations;
import org.folio.MarcFieldProtectionSettingsCollection;
import org.folio.Materialtypes;
import org.folio.Mtype;
import org.folio.NatureOfContentTerm;
import org.folio.Natureofcontentterms;
import org.folio.StatisticalCode;
import org.folio.StatisticalCodeType;
import org.folio.Statisticalcodes;
import org.folio.Statisticalcodetypes;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Provider for mapping parameters, uses in-memory cache to store parameters there
 */
@Component
public class MappingParametersProvider {

  private static final Logger LOGGER = LogManager.getLogger();

  @Value("${srm.mapping.parameters.settings.limit:1000}")
  private int settingsLimit;

  private static final String TENANT_CONFIGURATION_ZONE_URL = "/configurations/entries?query=" + URLEncoder.encode("(module==ORG and configName==localeSettings)", StandardCharsets.UTF_8);
  private static final String LINKING_RULES_URL = "/linking-rules/instance-authority";

  private static final String ELECTRONIC_ACCESS_PARAM = "electronicAccessRelationships";
  private static final String IDENTIFIER_TYPES_RESPONSE_PARAM = "identifierTypes";
  private static final String CLASSIFICATION_TYPES_RESPONSE_PARAM = "classificationTypes";
  private static final String INSTANCE_TYPES_RESPONSE_PARAM = "instanceTypes";
  private static final String INSTANCE_FORMATS_RESPONSE_PARAM = "instanceFormats";
  private static final String CONTRIBUTOR_TYPES_RESPONSE_PARAM = "contributorTypes";
  private static final String CONTRIBUTOR_NAME_TYPES_RESPONSE_PARAM = "contributorNameTypes";
  private static final String INSTANCE_NOTE_TYPES_RESPONSE_PARAM = "instanceNoteTypes";
  private static final String INSTANCE_ALTERNATIVE_TITLE_TYPES_RESPONSE_PARAM = "alternativeTitleTypes";
  private static final String ISSUANCE_MODES_RESPONSE_PARAM = "issuanceModes";
  private static final String INSTANCE_STATUSES_RESPONSE_PARAM = "instanceStatuses";
  private static final String NATURE_OF_CONTENT_TERMS_RESPONSE_PARAM = "natureOfContentTerms";
  private static final String INSTANCE_RELATIONSHIP_TYPES_RESPONSE_PARAM = "instanceRelationshipTypes";
  private static final String HOLDINGS_TYPES_RESPONSE_PARAM = "holdingsTypes";
  private static final String HOLDINGS_NOTE_TYPES_RESPONSE_PARAM = "holdingsNoteTypes";
  private static final String ILL_POLICIES_RESPONSE_PARAM = "illPolicies";
  private static final String CALL_NUMBER_TYPES_RESPONSE_PARAM = "callNumberTypes";
  private static final String STATISTICAL_CODES_RESPONSE_PARAM = "statisticalCodes";
  private static final String STATISTICAL_CODE_TYPES_RESPONSE_PARAM = "statisticalCodeTypes";
  private static final String LOCATIONS_RESPONSE_PARAM = "locations";
  private static final String MATERIALS_TYPES_RESPONSE_PARAM = "mtypes";
  private static final String ITEM_DAMAGED_STATUSES_RESPONSE_PARAM = "itemDamageStatuses";
  private static final String LOAN_TYPES_RESPONSE_PARAM = "loantypes";
  private static final String ITEM_NOTE_TYPES_RESPONSE_PARAM = "itemNoteTypes";
  private static final String FIELD_PROTECTION_SETTINGS_RESPONSE_PARAM = "marcFieldProtectionSettings";
  private static final String AUTHORITY_NOTE_TYPES_RESPONSE_PARAM = "authorityNoteTypes";
  private static final String AUTHORITY_SOURCE_FILES_RESPONSE_PARAM = "authoritySourceFiles";

  private static final String CONFIGS_VALUE_RESPONSE = "configs";
  private static final String VALUE_RESPONSE = "value";

  private static final int CACHE_EXPIRATION_TIME_IN_SECONDS = 60;

  private final InternalCache internalCache;

  public MappingParametersProvider(@Autowired Vertx vertx) {
    this.internalCache = new InternalCache(vertx);
  }

  /**
   * Provides mapping parameters by the given key.
   *
   * @param key key with which the specified MappingParameters are associated
   * @return mapping params for the given key
   */
  public Future<MappingParameters> get(String key, OkapiConnectionParams okapiParams) {
    return this.internalCache.get(new MappingParameterKey(key, okapiParams));
  }

  /**
   * Performs initialization for mapping parameters
   *
   * @param mappingParams given params to initialize
   * @param okapiParams   okapi connection params
   * @return initialized mapping params
   */
  private Future<MappingParameters> initializeParameters(MappingParameters mappingParams, OkapiConnectionParams okapiParams) {
    LOGGER.debug("initializeParameters:: initializing mapping parameters...");
    Future<List<IdentifierType>> identifierTypesFuture = getIdentifierTypes(okapiParams);
    Future<List<ClassificationType>> classificationTypesFuture = getClassificationTypes(okapiParams);
    Future<List<InstanceType>> instanceTypesFuture = getInstanceTypes(okapiParams);
    Future<List<ElectronicAccessRelationship>> electronicAccessRelationshipsFuture = getElectronicAccessRelationships(okapiParams);
    Future<List<InstanceFormat>> instanceFormatsFuture = getInstanceFormats(okapiParams);
    Future<List<ContributorType>> contributorTypesFuture = getContributorTypes(okapiParams);
    Future<List<ContributorNameType>> contributorNameTypesFuture = getContributorNameTypes(okapiParams);
    Future<List<InstanceNoteType>> instanceNoteTypesFuture = getInstanceNoteTypes(okapiParams);
    Future<List<AlternativeTitleType>> alternativeTitleTypesFuture = getAlternativeTitleTypes(okapiParams);
    Future<List<IssuanceMode>> issuanceModesFuture = getIssuanceModes(okapiParams);
    Future<List<InstanceStatus>> instanceStatusesFuture = getInstanceStatuses(okapiParams);
    Future<List<NatureOfContentTerm>> natureOfContentTermsFuture = getNatureOfContentTerms(okapiParams);
    Future<List<InstanceRelationshipType>> instanceRelationshipTypesFuture = getInstanceRelationshipTypes(okapiParams);
    Future<List<HoldingsType>> holdingsTypesFuture = getHoldingsTypes(okapiParams);
    Future<List<HoldingsNoteType>> holdingsNoteTypesFuture = getHoldingsNoteTypes(okapiParams);
    Future<List<IllPolicy>> illPoliciesFuture = getIllPolicies(okapiParams);
    Future<List<CallNumberType>> callNumberTypesFuture = getCallNumberTypes(okapiParams);
    Future<List<StatisticalCode>> statisticalCodesFuture = getStatisticalCodes(okapiParams);
    Future<List<StatisticalCodeType>> statisticalCodeTypesFuture = getStatisticalCodeTypes(okapiParams);
    Future<List<Location>> locationsFuture = getLocations(okapiParams);
    Future<List<Mtype>> materialTypesFuture = getMaterialTypes(okapiParams);
    Future<List<ItemDamageStatus>> itemDamagedStatusesFuture = getItemDamagedStatuses(okapiParams);
    Future<List<Loantype>> loanTypesFuture = getLoanTypes(okapiParams);
    Future<List<ItemNoteType>> itemNoteTypesFuture = getItemNoteTypes(okapiParams);
    Future<List<AuthorityNoteType>> authorityNoteTypesFuture = getAuthorityNoteTypes(okapiParams);
    Future<List<AuthoritySourceFile>> authoritySourceFilesFuture = getAuthoritySourceFiles(okapiParams);
    Future<List<MarcFieldProtectionSetting>> marcFieldProtectionSettingsFuture = getMarcFieldProtectionSettings(okapiParams);
    Future<String> tenantConfigurationFuture = getTenantConfiguration(okapiParams);
    Future<List<LinkingRuleDto>> linkingRulesFuture = getLinkingRules(okapiParams);


    return GenericCompositeFuture.join(Arrays.asList(identifierTypesFuture, classificationTypesFuture, instanceTypesFuture, instanceFormatsFuture,
        contributorTypesFuture, contributorNameTypesFuture, electronicAccessRelationshipsFuture, instanceNoteTypesFuture, alternativeTitleTypesFuture,
        issuanceModesFuture, instanceStatusesFuture, natureOfContentTermsFuture, instanceRelationshipTypesFuture, holdingsTypesFuture, holdingsNoteTypesFuture,
        illPoliciesFuture, callNumberTypesFuture, statisticalCodesFuture, statisticalCodeTypesFuture, locationsFuture, materialTypesFuture, itemDamagedStatusesFuture,
        loanTypesFuture, itemNoteTypesFuture, authorityNoteTypesFuture, authoritySourceFilesFuture, marcFieldProtectionSettingsFuture, tenantConfigurationFuture,
        linkingRulesFuture))
      .map(ar ->
        mappingParams
          .withInitializedState(true)
          .withIdentifierTypes(identifierTypesFuture.result())
          .withClassificationTypes(classificationTypesFuture.result())
          .withInstanceTypes(instanceTypesFuture.result())
          .withElectronicAccessRelationships(electronicAccessRelationshipsFuture.result())
          .withInstanceFormats(instanceFormatsFuture.result())
          .withContributorTypes(contributorTypesFuture.result())
          .withContributorNameTypes(contributorNameTypesFuture.result())
          .withInstanceNoteTypes(instanceNoteTypesFuture.result())
          .withAlternativeTitleTypes(alternativeTitleTypesFuture.result())
          .withIssuanceModes(issuanceModesFuture.result())
          .withInstanceStatuses(instanceStatusesFuture.result())
          .withNatureOfContentTerms(natureOfContentTermsFuture.result())
          .withInstanceRelationshipTypes(instanceRelationshipTypesFuture.result())
          .withInstanceRelationshipTypes(instanceRelationshipTypesFuture.result())
          .withHoldingsTypes(holdingsTypesFuture.result())
          .withHoldingsNoteTypes(holdingsNoteTypesFuture.result())
          .withIllPolicies(illPoliciesFuture.result())
          .withCallNumberTypes(callNumberTypesFuture.result())
          .withStatisticalCodes(statisticalCodesFuture.result())
          .withStatisticalCodeTypes(statisticalCodeTypesFuture.result())
          .withLocations(locationsFuture.result())
          .withMaterialTypes(materialTypesFuture.result())
          .withItemDamagedStatuses(itemDamagedStatusesFuture.result())
          .withLoanTypes(loanTypesFuture.result())
          .withItemNoteTypes(itemNoteTypesFuture.result())
          .withAuthorityNoteTypes(authorityNoteTypesFuture.result())
          .withAuthoritySourceFiles(authoritySourceFilesFuture.result())
          .withMarcFieldProtectionSettings(marcFieldProtectionSettingsFuture.result())
          .withTenantConfiguration(tenantConfigurationFuture.result())
          .withLinkingRules(linkingRulesFuture.result())
      ).onFailure(e -> LOGGER.error("initializeParameters:: Something happened while initializing mapping parameters", e));
  }

  /**
   * Requests for Identifier types from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Identifier types
   */
  private Future<List<IdentifierType>> getIdentifierTypes(OkapiConnectionParams params) {
    String identifierTypesUrl = "/identifier-types?limit=" + settingsLimit;
    return loadData(params, identifierTypesUrl, IDENTIFIER_TYPES_RESPONSE_PARAM,
      response -> response.mapTo(Identifiertypes.class).getIdentifierTypes());
  }

  /**
   * Requests for Classification types from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Classification types
   */
  private Future<List<ClassificationType>> getClassificationTypes(OkapiConnectionParams params) {
    String classificationTypesUrl = "/classification-types?limit=" + settingsLimit;
    return loadData(params, classificationTypesUrl, CLASSIFICATION_TYPES_RESPONSE_PARAM,
      response -> response.mapTo(Classificationtypes.class).getClassificationTypes());
  }

  /**
   * Requests for Instance types from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Instance types
   */
  private Future<List<InstanceType>> getInstanceTypes(OkapiConnectionParams params) {
    String instanceTypesUrl = "/instance-types?limit=" + settingsLimit;
    return loadData(params, instanceTypesUrl, INSTANCE_TYPES_RESPONSE_PARAM,
      response -> response.mapTo(Instancetypes.class).getInstanceTypes());
  }

  /**
   * Requests for Electronic access relationships from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Electronic Access Relationships
   */
  private Future<List<ElectronicAccessRelationship>> getElectronicAccessRelationships(OkapiConnectionParams params) {
    String electronicAccessUrl = "/electronic-access-relationships?limit=" + settingsLimit;
    return loadData(params, electronicAccessUrl, ELECTRONIC_ACCESS_PARAM,
      response -> response.mapTo(Electronicaccessrelationships.class).getElectronicAccessRelationships());
  }

  /**
   * Requests for Instance formats from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Instance formats
   */
  private Future<List<InstanceFormat>> getInstanceFormats(OkapiConnectionParams params) {
    String instanceFormatsUrl = "/instance-formats?limit=" + settingsLimit;
    return loadData(params, instanceFormatsUrl, INSTANCE_FORMATS_RESPONSE_PARAM,
      response -> response.mapTo(Instanceformats.class).getInstanceFormats());
  }

  /**
   * Requests for Contributor types from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Contributor types
   */
  private Future<List<ContributorType>> getContributorTypes(OkapiConnectionParams params) {
    String contributorTypesUrl = "/contributor-types?limit=" + settingsLimit;
    return loadData(params, contributorTypesUrl, CONTRIBUTOR_TYPES_RESPONSE_PARAM,
      response -> response.mapTo(Contributortypes.class).getContributorTypes());
  }

  /**
   * Requests for Contributor name types from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Contributor name types
   */
  private Future<List<ContributorNameType>> getContributorNameTypes(OkapiConnectionParams params) {
    String contributorNameTypesUrl = "/contributor-name-types?limit=" + settingsLimit;
    return loadData(params, contributorNameTypesUrl, CONTRIBUTOR_NAME_TYPES_RESPONSE_PARAM,
      response -> response.mapTo(Contributornametypes.class).getContributorNameTypes());
  }

  /**
   * Requests for Instance note types from application Settings (mod-inventory-storage)
   * *
   *
   * @param params Okapi connection parameters
   * @return List of Contributor name types
   */
  private Future<List<InstanceNoteType>> getInstanceNoteTypes(OkapiConnectionParams params) {
    String instanceNoteTypesUrl = "/instance-note-types?limit=" + settingsLimit;
    return loadData(params, instanceNoteTypesUrl, INSTANCE_NOTE_TYPES_RESPONSE_PARAM,
      response -> response.mapTo(Instancenotetypes.class).getInstanceNoteTypes());
  }

  private Future<List<AlternativeTitleType>> getAlternativeTitleTypes(OkapiConnectionParams params) {
    String instanceAlternativeTitleTypesUrl = "/alternative-title-types?limit=" + settingsLimit;
    return loadData(params, instanceAlternativeTitleTypesUrl, INSTANCE_ALTERNATIVE_TITLE_TYPES_RESPONSE_PARAM,
      response -> response.mapTo(Alternativetitletypes.class).getAlternativeTitleTypes());
  }

  private Future<List<NatureOfContentTerm>> getNatureOfContentTerms(OkapiConnectionParams params) {
    String natureOfContentTermsUrl = "/nature-of-content-terms?limit=" + settingsLimit;
    return loadData(params, natureOfContentTermsUrl, NATURE_OF_CONTENT_TERMS_RESPONSE_PARAM,
      response -> response.mapTo(Natureofcontentterms.class).getNatureOfContentTerms());
  }

  private Future<List<InstanceStatus>> getInstanceStatuses(OkapiConnectionParams params) {
    String instanceStatusesUrl = "/instance-statuses?limit=" + settingsLimit;
    return loadData(params, instanceStatusesUrl, INSTANCE_STATUSES_RESPONSE_PARAM,
      response -> response.mapTo(Instancestatuses.class).getInstanceStatuses());
  }

  private Future<List<InstanceRelationshipType>> getInstanceRelationshipTypes(OkapiConnectionParams params) {
    String instanceRelationshipTypesUrl = "/instance-relationship-types?limit=" + settingsLimit;
    return loadData(params, instanceRelationshipTypesUrl, INSTANCE_RELATIONSHIP_TYPES_RESPONSE_PARAM,
      response -> response.mapTo(Instancerelationshiptypes.class).getInstanceRelationshipTypes());
  }

  private Future<List<HoldingsType>> getHoldingsTypes(OkapiConnectionParams params) {
    String holdingsTypesUrl = "/holdings-types?limit=" + settingsLimit;
    return loadData(params, holdingsTypesUrl, HOLDINGS_TYPES_RESPONSE_PARAM,
      response -> response.mapTo(Holdingstypes.class).getHoldingsTypes());
  }

  private Future<List<HoldingsNoteType>> getHoldingsNoteTypes(OkapiConnectionParams params) {
    String holdingsNoteTypesUrl = "/holdings-note-types?limit=" + settingsLimit;
    return loadData(params, holdingsNoteTypesUrl, HOLDINGS_NOTE_TYPES_RESPONSE_PARAM,
      response -> response.mapTo(Holdingsnotetypes.class).getHoldingsNoteTypes());
  }

  private Future<List<IllPolicy>> getIllPolicies(OkapiConnectionParams params) {
    String illPoliciesUrl = "/ill-policies?limit=" + settingsLimit;
    return loadData(params, illPoliciesUrl, ILL_POLICIES_RESPONSE_PARAM,
      response -> response.mapTo(Illpolicies.class).getIllPolicies());
  }

  private Future<List<CallNumberType>> getCallNumberTypes(OkapiConnectionParams params) {
    String callNumberTypesUrl = "/call-number-types?limit=" + settingsLimit;
    return loadData(params, callNumberTypesUrl, CALL_NUMBER_TYPES_RESPONSE_PARAM,
      response -> response.mapTo(Callnumbertypes.class).getCallNumberTypes());
  }

  private Future<List<StatisticalCode>> getStatisticalCodes(OkapiConnectionParams params) {
    String statisticalCodesUrl = "/statistical-codes?limit=" + settingsLimit;
    return loadData(params, statisticalCodesUrl, STATISTICAL_CODES_RESPONSE_PARAM,
      response -> response.mapTo(Statisticalcodes.class).getStatisticalCodes());
  }

  private Future<List<StatisticalCodeType>> getStatisticalCodeTypes(OkapiConnectionParams params) {
    String statisticalCodeTypesUrl = "/statistical-code-types?limit=" + settingsLimit;
    return loadData(params, statisticalCodeTypesUrl, STATISTICAL_CODE_TYPES_RESPONSE_PARAM,
      response -> response.mapTo(Statisticalcodetypes.class).getStatisticalCodeTypes());
  }

  private Future<List<Location>> getLocations(OkapiConnectionParams params) {
    String locationsUrl = "/locations?limit=" + settingsLimit;
    return loadData(params, locationsUrl, LOCATIONS_RESPONSE_PARAM,
      response -> response.mapTo(Locations.class).getLocations());
  }

  private Future<List<Mtype>> getMaterialTypes(OkapiConnectionParams params) {
    String materialTypesUrl = "/material-types?limit=" + settingsLimit;
    return loadData(params, materialTypesUrl, MATERIALS_TYPES_RESPONSE_PARAM,
      response -> response.mapTo(Materialtypes.class).getMtypes());
  }

  private Future<List<ItemDamageStatus>> getItemDamagedStatuses(OkapiConnectionParams params) {
    String itemDamagedStatusesUrl = "/item-damaged-statuses?limit=" + settingsLimit;
    return loadData(params, itemDamagedStatusesUrl, ITEM_DAMAGED_STATUSES_RESPONSE_PARAM,
      response -> response.mapTo(Itemdamagedstatuses.class).getItemDamageStatuses());
  }

  private Future<List<Loantype>> getLoanTypes(OkapiConnectionParams params) {
    String loanTypesUrl = "/loan-types?limit=" + settingsLimit;
    return loadData(params, loanTypesUrl, LOAN_TYPES_RESPONSE_PARAM,
      response -> response.mapTo(Loantypes.class).getLoantypes());
  }

  private Future<List<ItemNoteType>> getItemNoteTypes(OkapiConnectionParams params) {
    String itemNoteTypesUrl = "/item-note-types?limit=" + settingsLimit;
    return loadData(params, itemNoteTypesUrl, ITEM_NOTE_TYPES_RESPONSE_PARAM,
      response -> response.mapTo(Itemnotetypes.class).getItemNoteTypes());
  }

  private Future<List<MarcFieldProtectionSetting>> getMarcFieldProtectionSettings(OkapiConnectionParams params) {
    String fieldProtectionSettingsUrl = "/field-protection-settings/marc?limit=" + settingsLimit;
    return loadData(params, fieldProtectionSettingsUrl, FIELD_PROTECTION_SETTINGS_RESPONSE_PARAM,
      response -> response.mapTo(MarcFieldProtectionSettingsCollection.class).getMarcFieldProtectionSettings());
  }

  private Future<List<AuthorityNoteType>> getAuthorityNoteTypes(OkapiConnectionParams params) {
    var authorityNoteTypesUrl = "/authority-note-types?limit=" + settingsLimit;
    return loadData(params, authorityNoteTypesUrl, AUTHORITY_NOTE_TYPES_RESPONSE_PARAM,
      response -> response.mapTo(Authoritynotetypes.class).getAuthorityNoteTypes());
  }

  private Future<List<AuthoritySourceFile>> getAuthoritySourceFiles(OkapiConnectionParams params) {
    var authoritySourceFilesUrl = "/authority-source-files?limit=" + settingsLimit;
    return loadData(params, authoritySourceFilesUrl, AUTHORITY_SOURCE_FILES_RESPONSE_PARAM,
      response -> response.mapTo(Authoritysourcefiles.class).getAuthoritySourceFiles());
  }

  /**
   * Requests for Issuance modes from application Settings (mod-inventory-storage)
   * *
   *
   * @param params Okapi connection parameters
   * @return List of Issuance modes
   */
  private Future<List<IssuanceMode>> getIssuanceModes(OkapiConnectionParams params) {
    String issuanceModesUrl = "/modes-of-issuance?limit=" + settingsLimit;
    return loadData(params, issuanceModesUrl, ISSUANCE_MODES_RESPONSE_PARAM,
      response -> response.mapTo(Issuancemodes.class).getIssuanceModes());
  }

  /**
   * Requests for tenant configuration from mod-configuration.
   * *
   *
   * @param params Okapi connection parameters
   * @return tenant configuration
   */
  private Future<String> getTenantConfiguration(OkapiConnectionParams params) {
    Promise<String> promise = Promise.promise();
    RestUtil.doRequest(params, TENANT_CONFIGURATION_ZONE_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (ifConfigResponseIsValid(response)) {
          String timeZone = response.getJsonArray(CONFIGS_VALUE_RESPONSE).getJsonObject(0).getString(VALUE_RESPONSE);
          promise.complete(timeZone);
        } else {
          promise.complete(StringUtils.EMPTY);
        }
      }
    });
    return promise.future();
  }

  /**
   * Requests for linking rules from mod-entities-links.
   * *
   *
   * @param params Okapi connection parameters
   * @return linking rules
   */
  private Future<List<LinkingRuleDto>> getLinkingRules(OkapiConnectionParams params) {
    Promise<List<LinkingRuleDto>> promise = Promise.promise();
    RestUtil.doRequest(params, LINKING_RULES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        var result = ar.result();
        String response = result.getBody();
        if (StringUtils.isNotBlank(response)) {
          List<LinkingRuleDto> linkingRules = new LinkedList<>();
          try {
            linkingRules = DatabindCodec.mapper().readValue(response, new TypeReference<>(){});
          } catch (JsonProcessingException e) {
            LOGGER.warn("Unable to parse linking rules response: {}", e.getMessage());
            promise.complete(Collections.emptyList());
          }
          promise.complete(linkingRules);
        } else {
          LOGGER.warn("Retrieve linking rules fail: {}", result.getCode());
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  private <T> Future<List<T>> loadData(OkapiConnectionParams params, String requestUrl, String dataCollectionField,
                                       Function<JsonObject, List<T>> dataExtractor) {
    Promise<List<T>> promise = Promise.promise();
    RestUtil.doRequest(params, requestUrl, HttpMethod.GET, null).onComplete(responseAr -> {
      try {
        if (RestUtil.validateAsyncResult(responseAr, promise)) {
          JsonObject response = responseAr.result().getJson();
          if (response != null && response.containsKey(dataCollectionField)) {
            promise.complete(dataExtractor.apply(response));
          } else {
            promise.complete(Collections.emptyList());
          }
        }
      } catch (Exception e) {
        LOGGER.warn("loadData:: Failed to load {}", dataCollectionField, e);
        promise.fail(e);
      }
    });
    return promise.future();
  }

  private boolean ifConfigResponseIsValid(JsonObject response) {
    return response != null && response.containsKey(CONFIGS_VALUE_RESPONSE)
      && response.getJsonArray(CONFIGS_VALUE_RESPONSE) != null
      && !response.getJsonArray(CONFIGS_VALUE_RESPONSE).isEmpty()
      && response.getJsonArray(CONFIGS_VALUE_RESPONSE).getJsonObject(0) != null;
  }

  /**
   * This class is used as a composite key that holds the intended key and okapi connection parameters that will be used
   * to load missing parameters from the cache. Equality for this class is determined by only comparing the intended key
   * . This means that two MappingParameterKey objects with the same key but different okapiParams will the equal.
   */
  public static class MappingParameterKey {
    private String key;
    private OkapiConnectionParams okapiConnectionParams;

    public MappingParameterKey(String key, OkapiConnectionParams okapiParams) {
      this.key = key;
      this.okapiConnectionParams = okapiParams;
    }

    public String getKey() {
      return key;
    }

    public OkapiConnectionParams getOkapiConnectionParams() {
      return okapiConnectionParams;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      MappingParameterKey that = (MappingParameterKey) o;
      return Objects.equal(key, that.key);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(key);
    }
  }

  /**
   * In-memory cache to store mapping params
   */
  private class InternalCache {
    private AsyncLoadingCache<MappingParameterKey, MappingParameters> cache;

    public InternalCache(Vertx vertx) {
      this.cache =
          Caffeine.newBuilder()
              /*
                  In order to do not break down Vert.x threading model
                  we need to delegate cache internal activities to the event-loop thread.
              */
              .executor(serviceExecutor -> vertx.runOnContext(ar -> serviceExecutor.run()))
              .expireAfterAccess(CACHE_EXPIRATION_TIME_IN_SECONDS, TimeUnit.SECONDS)
              .buildAsync(
                  (key, executor) -> {
                    CompletableFuture<MappingParameters> future = new CompletableFuture<>();
                    executor.execute(
                        () ->
                            initializeParameters(
                                    new MappingParameters().withInitializedState(false),
                                    key.getOkapiConnectionParams())
                                .onComplete(ar -> future.complete(ar.result())));
                    return future;
                  });
    }

    /**
     * Provides mapping parameters by the given key.
     *
     * @param key key with which the specified MappingParameters are associated
     * @return mapping params for the given key
     */
    public Future<MappingParameters> get(MappingParameterKey key) {
      Promise<MappingParameters> promise = Promise.promise();
      this.cache
          .get(key)
          .whenComplete(
              (mappingParameters, exception) -> {
                if (exception != null) {
                  promise.fail(exception);
                } else {
                  promise.complete(mappingParameters);
                }
              });
      return promise.future();
    }
  }
}
