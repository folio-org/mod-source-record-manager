package org.folio.services.mappers.processor;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import org.folio.AlternativeTitleType;
import org.folio.Alternativetitletypes;
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
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Provider for mapping parameters, uses in-memory cache to store parameters there
 */
@Component
public class MappingParametersProvider {
  private static final int SETTING_LIMIT = 500;
  private static final String IDENTIFIER_TYPES_URL = "/identifier-types?limit=" + SETTING_LIMIT;
  private static final String CLASSIFICATION_TYPES_URL = "/classification-types?limit=" + SETTING_LIMIT;
  private static final String INSTANCE_TYPES_URL = "/instance-types?limit=" + SETTING_LIMIT;
  private static final String INSTANCE_FORMATS_URL = "/instance-formats?limit=" + SETTING_LIMIT;
  private static final String CONTRIBUTOR_TYPES_URL = "/contributor-types?limit=" + SETTING_LIMIT;
  private static final String CONTRIBUTOR_NAME_TYPES_URL = "/contributor-name-types?limit=" + SETTING_LIMIT;
  private static final String ELECTRONIC_ACCESS_URL = "/electronic-access-relationships?limit=" + SETTING_LIMIT;
  private static final String INSTANCE_NOTE_TYPES_URL = "/instance-note-types?limit=" + SETTING_LIMIT;
  private static final String INSTANCE_ALTERNATIVE_TITLE_TYPES_URL = "/alternative-title-types?limit=" + SETTING_LIMIT;
  private static final String ISSUANCE_MODES_URL = "/modes-of-issuance?limit=" + SETTING_LIMIT;
  private static final String INSTANCE_STATUSES_URL = "/instance-statuses?limit=" + SETTING_LIMIT;
  private static final String NATURE_OF_CONTENT_TERMS_URL = "/nature-of-content-terms?limit=" + SETTING_LIMIT;
  private static final String INSTANCE_RELATIONSHIP_TYPES_URL = "/instance-relationship-types?limit=" + SETTING_LIMIT;
  private static final String HOLDINGS_TYPES_URL = "/holdings-types?limit=" + SETTING_LIMIT;
  private static final String HOLDINGS_NOTE_TYPES_URL = "/holdings-note-types?limit=" + SETTING_LIMIT;
  private static final String ILL_POLICIES_URL = "/ill-policies?limit=" + SETTING_LIMIT;
  private static final String CALL_NUMBER_TYPES_URL = "/call-number-types?limit=" + SETTING_LIMIT;
  private static final String STATISTICAL_CODES_URL = "/statistical-codes?limit=" + SETTING_LIMIT;
  private static final String STATISTICAL_CODE_TYPES_URL = "/statistical-code-types?limit=" + SETTING_LIMIT;
  private static final String LOCATIONS_URL = "/locations?limit=" + SETTING_LIMIT;
  private static final String MATERIAL_TYPES_URL = "/material-types?limit=" + SETTING_LIMIT;
  private static final String ITEM_DAMAGED_STATUSES_URL = "/item-damaged-statuses?limit=" + SETTING_LIMIT;
  private static final String LOAN_TYPES_URL = "/loan-types?limit=" + SETTING_LIMIT;
  private static final String ITEM_NOTE_TYPES_URL = "/item-note-types?limit=" + SETTING_LIMIT;
  private static final String FIELD_PROTECTION_SETTINGS_URL = "/field-protection-settings/marc?limit=" + SETTING_LIMIT;

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

  private static final int CACHE_EXPIRATION_TIME_IN_SECONDS = 60;

  private InternalCache internalCache;

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
    return this.internalCache.get(key, mappingParameters -> initializeParameters(mappingParameters, okapiParams));
  }

  /**
   * Performs initialization for mapping parameters
   *
   * @param mappingParams given params to initialize
   * @param okapiParams   okapi connection params
   * @return initialized mapping params
   */
  private Future<MappingParameters> initializeParameters(MappingParameters mappingParams, OkapiConnectionParams okapiParams) {
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
    Future<List<MarcFieldProtectionSetting>> marcFieldProtectionSettingsFuture = getMarcFieldProtectionSettings(okapiParams);


    return CompositeFuture.all(Arrays.asList(identifierTypesFuture, classificationTypesFuture, instanceTypesFuture, instanceFormatsFuture,
      contributorTypesFuture, contributorNameTypesFuture, electronicAccessRelationshipsFuture, instanceNoteTypesFuture, alternativeTitleTypesFuture,
      issuanceModesFuture, instanceStatusesFuture, natureOfContentTermsFuture, instanceRelationshipTypesFuture, holdingsTypesFuture, holdingsNoteTypesFuture,
      illPoliciesFuture, callNumberTypesFuture, statisticalCodesFuture, statisticalCodeTypesFuture, locationsFuture, materialTypesFuture, itemDamagedStatusesFuture,
      loanTypesFuture, itemNoteTypesFuture, marcFieldProtectionSettingsFuture))
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
          .withMarcFieldProtectionSettings(marcFieldProtectionSettingsFuture.result())
      );
  }

  /**
   * Requests for Identifier types from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Identifier types
   */
  private Future<List<IdentifierType>> getIdentifierTypes(OkapiConnectionParams params) {
    Promise<List<IdentifierType>> promise = Promise.promise();
    RestUtil.doRequest(params, IDENTIFIER_TYPES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(IDENTIFIER_TYPES_RESPONSE_PARAM)) {
          List<IdentifierType> identifierTypeList = response.mapTo(Identifiertypes.class).getIdentifierTypes();
          promise.complete(identifierTypeList);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  /**
   * Requests for Classification types from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Classification types
   */
  private Future<List<ClassificationType>> getClassificationTypes(OkapiConnectionParams params) {
    Promise<List<ClassificationType>> promise = Promise.promise();
    RestUtil.doRequest(params, CLASSIFICATION_TYPES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(CLASSIFICATION_TYPES_RESPONSE_PARAM)) {
          List<ClassificationType> classificationTypeList = response.mapTo(Classificationtypes.class).getClassificationTypes();
          promise.complete(classificationTypeList);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  /**
   * Requests for Instance types from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Instance types
   */
  private Future<List<InstanceType>> getInstanceTypes(OkapiConnectionParams params) {
    Promise<List<InstanceType>> promise = Promise.promise();
    RestUtil.doRequest(params, INSTANCE_TYPES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(INSTANCE_TYPES_RESPONSE_PARAM)) {
          List<InstanceType> instanceTypeList = response.mapTo(Instancetypes.class).getInstanceTypes();
          promise.complete(instanceTypeList);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  /**
   * Requests for Electronic access relationships from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Electronic Access Relationships
   */
  private Future<List<ElectronicAccessRelationship>> getElectronicAccessRelationships(OkapiConnectionParams params) {
    Promise<List<ElectronicAccessRelationship>> promise = Promise.promise();
    RestUtil.doRequest(params, ELECTRONIC_ACCESS_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(ELECTRONIC_ACCESS_PARAM)) {
          List<ElectronicAccessRelationship> electronicAccessRelationshipList = response.mapTo(Electronicaccessrelationships.class).getElectronicAccessRelationships();
          promise.complete(electronicAccessRelationshipList);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  /**
   * Requests for Instance formats from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Instance formats
   */
  private Future<List<InstanceFormat>> getInstanceFormats(OkapiConnectionParams params) {
    Promise<List<InstanceFormat>> promise = Promise.promise();
    RestUtil.doRequest(params, INSTANCE_FORMATS_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(INSTANCE_FORMATS_RESPONSE_PARAM)) {
          List<InstanceFormat> instanceFormatList = response.mapTo(Instanceformats.class).getInstanceFormats();
          promise.complete(instanceFormatList);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  /**
   * Requests for Contributor types from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Contributor types
   */
  private Future<List<ContributorType>> getContributorTypes(OkapiConnectionParams params) {
    Promise<List<ContributorType>> promise = Promise.promise();
    RestUtil.doRequest(params, CONTRIBUTOR_TYPES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(CONTRIBUTOR_TYPES_RESPONSE_PARAM)) {
          List<ContributorType> contributorTypes = response.mapTo(Contributortypes.class).getContributorTypes();
          promise.complete(contributorTypes);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  /**
   * Requests for Contributor name types from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Contributor name types
   */
  private Future<List<ContributorNameType>> getContributorNameTypes(OkapiConnectionParams params) {
    Promise<List<ContributorNameType>> promise = Promise.promise();
    RestUtil.doRequest(params, CONTRIBUTOR_NAME_TYPES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(CONTRIBUTOR_NAME_TYPES_RESPONSE_PARAM)) {
          List<ContributorNameType> contributorNameTypes = response.mapTo(Contributornametypes.class).getContributorNameTypes();
          promise.complete(contributorNameTypes);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  /**
   * Requests for Instance note types from application Settings (mod-inventory-storage)
   * *
   *
   * @param params Okapi connection parameters
   * @return List of Contributor name types
   */
  private Future<List<InstanceNoteType>> getInstanceNoteTypes(OkapiConnectionParams params) {
    Promise<List<InstanceNoteType>> promise = Promise.promise();
    RestUtil.doRequest(params, INSTANCE_NOTE_TYPES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(INSTANCE_NOTE_TYPES_RESPONSE_PARAM)) {
          List<InstanceNoteType> contributorNameTypes = response.mapTo(Instancenotetypes.class).getInstanceNoteTypes();
          promise.complete(contributorNameTypes);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  private Future<List<AlternativeTitleType>> getAlternativeTitleTypes(OkapiConnectionParams params) {
    Promise<List<AlternativeTitleType>> promise = Promise.promise();
    RestUtil.doRequest(params, INSTANCE_ALTERNATIVE_TITLE_TYPES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(INSTANCE_ALTERNATIVE_TITLE_TYPES_RESPONSE_PARAM)) {
          List<AlternativeTitleType> alternativeTitleTypes = response.mapTo(Alternativetitletypes.class).getAlternativeTitleTypes();
          promise.complete(alternativeTitleTypes);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  private Future<List<NatureOfContentTerm>> getNatureOfContentTerms(OkapiConnectionParams params) {
    Promise<List<NatureOfContentTerm>> promise = Promise.promise();
    RestUtil.doRequest(params, NATURE_OF_CONTENT_TERMS_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(NATURE_OF_CONTENT_TERMS_RESPONSE_PARAM)) {
          List<NatureOfContentTerm> natureOfContentTerms = response.mapTo(Natureofcontentterms.class).getNatureOfContentTerms();
          promise.complete(natureOfContentTerms);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  private Future<List<InstanceStatus>> getInstanceStatuses(OkapiConnectionParams params) {
    Promise<List<InstanceStatus>> promise = Promise.promise();
    RestUtil.doRequest(params, INSTANCE_STATUSES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(INSTANCE_STATUSES_RESPONSE_PARAM)) {
          List<InstanceStatus> instanceStatuses = response.mapTo(Instancestatuses.class).getInstanceStatuses();
          promise.complete(instanceStatuses);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  private Future<List<InstanceRelationshipType>> getInstanceRelationshipTypes(OkapiConnectionParams params) {
    Promise<List<InstanceRelationshipType>> promise = Promise.promise();
    RestUtil.doRequest(params, INSTANCE_RELATIONSHIP_TYPES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(INSTANCE_RELATIONSHIP_TYPES_RESPONSE_PARAM)) {
          List<InstanceRelationshipType> instanceRelationshipTypes = response.mapTo(Instancerelationshiptypes.class).getInstanceRelationshipTypes();
          promise.complete(instanceRelationshipTypes);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  private Future<List<HoldingsType>> getHoldingsTypes(OkapiConnectionParams params) {
    Promise<List<HoldingsType>> promise = Promise.promise();
    RestUtil.doRequest(params, HOLDINGS_TYPES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(HOLDINGS_TYPES_RESPONSE_PARAM)) {
          List<HoldingsType> holdingsTypes = response.mapTo(Holdingstypes.class).getHoldingsTypes();
          promise.complete(holdingsTypes);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  private Future<List<HoldingsNoteType>> getHoldingsNoteTypes(OkapiConnectionParams params) {
    Promise<List<HoldingsNoteType>> promise = Promise.promise();
    RestUtil.doRequest(params, HOLDINGS_NOTE_TYPES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(HOLDINGS_NOTE_TYPES_RESPONSE_PARAM)) {
          List<HoldingsNoteType> holdingsNoteTypes = response.mapTo(Holdingsnotetypes.class).getHoldingsNoteTypes();
          promise.complete(holdingsNoteTypes);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  private Future<List<IllPolicy>> getIllPolicies(OkapiConnectionParams params) {
    Promise<List<IllPolicy>> promise = Promise.promise();
    RestUtil.doRequest(params, ILL_POLICIES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(ILL_POLICIES_RESPONSE_PARAM)) {
          List<IllPolicy> holdingsNoteTypes = response.mapTo(Illpolicies.class).getIllPolicies();
          promise.complete(holdingsNoteTypes);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  private Future<List<CallNumberType>> getCallNumberTypes(OkapiConnectionParams params) {
    Promise<List<CallNumberType>> promise = Promise.promise();
    RestUtil.doRequest(params, CALL_NUMBER_TYPES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(CALL_NUMBER_TYPES_RESPONSE_PARAM)) {
          List<CallNumberType> callNumberTypes = response.mapTo(Callnumbertypes.class).getCallNumberTypes();
          promise.complete(callNumberTypes);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  private Future<List<StatisticalCode>> getStatisticalCodes(OkapiConnectionParams params) {
    Promise<List<StatisticalCode>> promise = Promise.promise();
    RestUtil.doRequest(params, STATISTICAL_CODES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(STATISTICAL_CODES_RESPONSE_PARAM)) {
          List<StatisticalCode> statisticalCodes = response.mapTo(Statisticalcodes.class).getStatisticalCodes();
          promise.complete(statisticalCodes);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  private Future<List<StatisticalCodeType>> getStatisticalCodeTypes(OkapiConnectionParams params) {
    Promise<List<StatisticalCodeType>> promise = Promise.promise();
    RestUtil.doRequest(params, STATISTICAL_CODE_TYPES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(STATISTICAL_CODE_TYPES_RESPONSE_PARAM)) {
          List<StatisticalCodeType> statisticalCodeTypes = response.mapTo(Statisticalcodetypes.class).getStatisticalCodeTypes();
          promise.complete(statisticalCodeTypes);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  private Future<List<Location>> getLocations(OkapiConnectionParams params) {
    Promise<List<Location>> promise = Promise.promise();
    RestUtil.doRequest(params, LOCATIONS_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(LOCATIONS_RESPONSE_PARAM)) {
          List<Location> locations = response.mapTo(Locations.class).getLocations();
          promise.complete(locations);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  private Future<List<Mtype>> getMaterialTypes(OkapiConnectionParams params) {
    Promise<List<Mtype>> promise = Promise.promise();
    RestUtil.doRequest(params, MATERIAL_TYPES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(MATERIALS_TYPES_RESPONSE_PARAM)) {
          List<Mtype> materialTypes = response.mapTo(Materialtypes.class).getMtypes();
          promise.complete(materialTypes);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  private Future<List<ItemDamageStatus>> getItemDamagedStatuses(OkapiConnectionParams params) {
    Promise<List<ItemDamageStatus>> promise = Promise.promise();
    RestUtil.doRequest(params, ITEM_DAMAGED_STATUSES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(ITEM_DAMAGED_STATUSES_RESPONSE_PARAM)) {
          List<ItemDamageStatus> itemDamageStatuses = response.mapTo(Itemdamagedstatuses.class).getItemDamageStatuses();
          promise.complete(itemDamageStatuses);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  private Future<List<Loantype>> getLoanTypes(OkapiConnectionParams params) {
    Promise<List<Loantype>> promise = Promise.promise();
    RestUtil.doRequest(params, LOAN_TYPES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(LOAN_TYPES_RESPONSE_PARAM)) {
          List<Loantype> loantypes = response.mapTo(Loantypes.class).getLoantypes();
          promise.complete(loantypes);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  private Future<List<ItemNoteType>> getItemNoteTypes(OkapiConnectionParams params) {
    Promise<List<ItemNoteType>> promise = Promise.promise();
    RestUtil.doRequest(params, ITEM_NOTE_TYPES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(ITEM_NOTE_TYPES_RESPONSE_PARAM)) {
          List<ItemNoteType> itemNoteTypes = response.mapTo(Itemnotetypes.class).getItemNoteTypes();
          promise.complete(itemNoteTypes);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  private Future<List<MarcFieldProtectionSetting>> getMarcFieldProtectionSettings(OkapiConnectionParams params) {
    Promise<List<MarcFieldProtectionSetting>> promise = Promise.promise();
    RestUtil.doRequest(params, FIELD_PROTECTION_SETTINGS_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(FIELD_PROTECTION_SETTINGS_RESPONSE_PARAM)) {
          List<MarcFieldProtectionSetting> itemNoteTypes = response.mapTo(MarcFieldProtectionSettingsCollection.class).getMarcFieldProtectionSettings();
          promise.complete(itemNoteTypes);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }
  /**
   * Requests for Issuance modes from application Settings (mod-inventory-storage)
   * *
   *
   * @param params Okapi connection parameters
   * @return List of Issuance modes
   */
  private Future<List<IssuanceMode>> getIssuanceModes(OkapiConnectionParams params) {
    Promise<List<IssuanceMode>> promise = Promise.promise();
    RestUtil.doRequest(params, ISSUANCE_MODES_URL, HttpMethod.GET, null).onComplete(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(ISSUANCE_MODES_RESPONSE_PARAM)) {
          List<IssuanceMode> issuanceModes = response.mapTo(Issuancemodes.class).getIssuanceModes();
          promise.complete(issuanceModes);
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  /**
   * In-memory cache to store mapping params
   */
  private class InternalCache {
    private AsyncLoadingCache<String, MappingParameters> cache;

    public InternalCache(Vertx vertx) {
      this.cache = Caffeine.newBuilder()
        /*
            In order to do not break down Vert.x threading model
            we need to delegate cache internal activities to the event-loop thread.
        */
        .executor(serviceExecutor -> vertx.runOnContext(ar -> serviceExecutor.run()))
        .expireAfterAccess(CACHE_EXPIRATION_TIME_IN_SECONDS, TimeUnit.SECONDS)
        .buildAsync(key -> new MappingParameters().withInitializedState(false));
    }

    /**
     * Provides mapping parameters by the given key.
     *
     * @param key        key with which the specified MappingParameters are associated
     * @param initAction action to initialize mapping params
     * @return mapping params for the given key
     */
    public Future<MappingParameters> get(String key, Function<MappingParameters, Future<MappingParameters>> initAction) {
      Promise<MappingParameters> promise = Promise.promise();
      this.cache.get(key).whenComplete((mappingParameters, exception) -> {
        if (exception != null) {
          promise.fail(exception);
        } else {
          if (mappingParameters.isInitialized()) {
            promise.complete(mappingParameters);
          } else {
            // Complete future to continue with mapping even if request for MappingParameters is failed
            initAction.apply(mappingParameters).onComplete(ar -> promise.complete(mappingParameters));
          }
        }
      });
      return promise.future();
    }
  }
}
