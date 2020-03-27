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
import org.folio.ClassificationType;
import org.folio.Classificationtypes;
import org.folio.ContributorNameType;
import org.folio.ContributorType;
import org.folio.Contributornametypes;
import org.folio.Contributortypes;
import org.folio.ElectronicAccessRelationship;
import org.folio.Electronicaccessrelationships;
import org.folio.IdentifierType;
import org.folio.Identifiertypes;
import org.folio.InstanceFormat;
import org.folio.InstanceNoteType;
import org.folio.InstanceType;
import org.folio.Instanceformats;
import org.folio.Instancenotetypes;
import org.folio.Instancetypes;
import org.folio.IssuanceMode;
import org.folio.Issuancemodes;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
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
    return CompositeFuture.all(Arrays.asList(identifierTypesFuture, classificationTypesFuture, instanceTypesFuture, instanceFormatsFuture,
      contributorTypesFuture, contributorNameTypesFuture, electronicAccessRelationshipsFuture, instanceNoteTypesFuture, alternativeTitleTypesFuture,
      issuanceModesFuture))
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
      );
  }

  /**
   * Requests for Identifier types from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Identifier types
   */
  private Future<List<IdentifierType>> getIdentifierTypes(OkapiConnectionParams params) {
    Future<List<IdentifierType>> future = Future.future();
    RestUtil.doRequest(params, IDENTIFIER_TYPES_URL, HttpMethod.GET, null).setHandler(ar -> {
      if (RestUtil.validateAsyncResult(ar, future)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(IDENTIFIER_TYPES_RESPONSE_PARAM)) {
          List<IdentifierType> identifierTypeList = response.mapTo(Identifiertypes.class).getIdentifierTypes();
          future.complete(identifierTypeList);
        } else {
          future.complete(Collections.emptyList());
        }
      }
    });
    return future;
  }

  /**
   * Requests for Classification types from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Classification types
   */
  private Future<List<ClassificationType>> getClassificationTypes(OkapiConnectionParams params) {
    Future<List<ClassificationType>> future = Future.future();
    RestUtil.doRequest(params, CLASSIFICATION_TYPES_URL, HttpMethod.GET, null).setHandler(ar -> {
      if (RestUtil.validateAsyncResult(ar, future)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(CLASSIFICATION_TYPES_RESPONSE_PARAM)) {
          List<ClassificationType> classificationTypeList = response.mapTo(Classificationtypes.class).getClassificationTypes();
          future.complete(classificationTypeList);
        } else {
          future.complete(Collections.emptyList());
        }
      }
    });
    return future;
  }

  /**
   * Requests for Instance types from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Instance types
   */
  private Future<List<InstanceType>> getInstanceTypes(OkapiConnectionParams params) {
    Future<List<InstanceType>> future = Future.future();
    RestUtil.doRequest(params, INSTANCE_TYPES_URL, HttpMethod.GET, null).setHandler(ar -> {
      if (RestUtil.validateAsyncResult(ar, future)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(INSTANCE_TYPES_RESPONSE_PARAM)) {
          List<InstanceType> instanceTypeList = response.mapTo(Instancetypes.class).getInstanceTypes();
          future.complete(instanceTypeList);
        } else {
          future.complete(Collections.emptyList());
        }
      }
    });
    return future;
  }

  /**
   * Requests for Electronic access relationships from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Electronic Access Relationships
   */
  private Future<List<ElectronicAccessRelationship>> getElectronicAccessRelationships(OkapiConnectionParams params) {
    Future<List<ElectronicAccessRelationship>> future = Future.future();
    RestUtil.doRequest(params, ELECTRONIC_ACCESS_URL, HttpMethod.GET, null).setHandler(ar -> {
      if (RestUtil.validateAsyncResult(ar, future)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(ELECTRONIC_ACCESS_PARAM)) {
          List<ElectronicAccessRelationship> electronicAccessRelationshipList = response.mapTo(Electronicaccessrelationships.class).getElectronicAccessRelationships();
          future.complete(electronicAccessRelationshipList);
        } else {
          future.complete(Collections.emptyList());
        }
      }
    });
    return future;
  }

  /**
   * Requests for Instance formats from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Instance formats
   */
  private Future<List<InstanceFormat>> getInstanceFormats(OkapiConnectionParams params) {
    Future<List<InstanceFormat>> future = Future.future();
    RestUtil.doRequest(params, INSTANCE_FORMATS_URL, HttpMethod.GET, null).setHandler(ar -> {
      if (RestUtil.validateAsyncResult(ar, future)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(INSTANCE_FORMATS_RESPONSE_PARAM)) {
          List<InstanceFormat> instanceFormatList = response.mapTo(Instanceformats.class).getInstanceFormats();
          future.complete(instanceFormatList);
        } else {
          future.complete(Collections.emptyList());
        }
      }
    });
    return future;
  }

  /**
   * Requests for Contributor types from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Contributor types
   */
  private Future<List<ContributorType>> getContributorTypes(OkapiConnectionParams params) {
    Future<List<ContributorType>> future = Future.future();
    RestUtil.doRequest(params, CONTRIBUTOR_TYPES_URL, HttpMethod.GET, null).setHandler(ar -> {
      if (RestUtil.validateAsyncResult(ar, future)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(CONTRIBUTOR_TYPES_RESPONSE_PARAM)) {
          List<ContributorType> contributorTypes = response.mapTo(Contributortypes.class).getContributorTypes();
          future.complete(contributorTypes);
        } else {
          future.complete(Collections.emptyList());
        }
      }
    });
    return future;
  }

  /**
   * Requests for Contributor name types from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Contributor name types
   */
  private Future<List<ContributorNameType>> getContributorNameTypes(OkapiConnectionParams params) {
    Future<List<ContributorNameType>> future = Future.future();
    RestUtil.doRequest(params, CONTRIBUTOR_NAME_TYPES_URL, HttpMethod.GET, null).setHandler(ar -> {
      if (RestUtil.validateAsyncResult(ar, future)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(CONTRIBUTOR_NAME_TYPES_RESPONSE_PARAM)) {
          List<ContributorNameType> contributorNameTypes = response.mapTo(Contributornametypes.class).getContributorNameTypes();
          future.complete(contributorNameTypes);
        } else {
          future.complete(Collections.emptyList());
        }
      }
    });
    return future;
  }

  /**
   * Requests for Instance note types from application Settings (mod-inventory-storage)
   * *
   *
   * @param params Okapi connection parameters
   * @return List of Contributor name types
   */
  private Future<List<InstanceNoteType>> getInstanceNoteTypes(OkapiConnectionParams params) {
    Future<List<InstanceNoteType>> future = Future.future();
    RestUtil.doRequest(params, INSTANCE_NOTE_TYPES_URL, HttpMethod.GET, null).setHandler(ar -> {
      if (RestUtil.validateAsyncResult(ar, future)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(INSTANCE_NOTE_TYPES_RESPONSE_PARAM)) {
          List<InstanceNoteType> contributorNameTypes = response.mapTo(Instancenotetypes.class).getInstanceNoteTypes();
          future.complete(contributorNameTypes);
        } else {
          future.complete(Collections.emptyList());
        }
      }
    });
    return future;
  }

  private Future<List<AlternativeTitleType>> getAlternativeTitleTypes(OkapiConnectionParams params) {
    Future<List<AlternativeTitleType>> future = Future.future();
    RestUtil.doRequest(params, INSTANCE_ALTERNATIVE_TITLE_TYPES_URL, HttpMethod.GET, null).setHandler(ar -> {
      if (RestUtil.validateAsyncResult(ar, future)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(INSTANCE_ALTERNATIVE_TITLE_TYPES_RESPONSE_PARAM)) {
          List<AlternativeTitleType> alternativeTitleTypes = response.mapTo(Alternativetitletypes.class).getAlternativeTitleTypes();
          future.complete(alternativeTitleTypes);
        } else {
          future.complete(Collections.emptyList());
        }
      }
    });
    return future;
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
    RestUtil.doRequest(params, ISSUANCE_MODES_URL, HttpMethod.GET, null).setHandler(ar -> {
      if (RestUtil.validateAsyncResult(ar, promise.future())) {
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
      Future<MappingParameters> future = Future.future();
      this.cache.get(key).whenComplete((mappingParameters, exception) -> {
        if (exception != null) {
          future.fail(exception);
        } else {
          if (mappingParameters.isInitialized()) {
            future.complete(mappingParameters);
          } else {
            // Complete future to continue with mapping even if request for MappingParameters is failed
            initAction.apply(mappingParameters).setHandler(ar -> future.complete(mappingParameters));
          }
        }
      });
      return future;
    }
  }
}
