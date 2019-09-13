package org.folio.services.mappers.processor.parameters;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.rest.jaxrs.model.ClassificationType;
import org.folio.rest.jaxrs.model.ClassificationTypes;
import org.folio.rest.jaxrs.model.ElectronicAccessRelationship;
import org.folio.rest.jaxrs.model.ElectronicAccessRelationships;
import org.folio.rest.jaxrs.model.IdentifierType;
import org.folio.rest.jaxrs.model.IdentifierTypes;
import org.folio.rest.jaxrs.model.InstanceType;
import org.folio.rest.jaxrs.model.InstanceTypes;

import java.util.Collections;
import java.util.List;

/**
 * Builder for mapping parameters.
 */
public class MappingParametersBuilder {
  private static final String IDENTIFIER_TYPES_URL = "/identifier-types";
  private static final String CLASSIFICATION_TYPES_URL = "/classification-types";
  private static final String INSTANCE_TYPES_URL = "/instance-types";
  private static final String ELECTRONIC_ACCESS_URL = "/electronic-access-relationships";
  private static final String IDENTIFIER_TYPES_RESPONSE_PARAM = "identifierTypes";
  private static final String CLASSIFICATION_TYPES_RESPONSE_PARAM = "classificationTypes";
  private static final String INSTANCE_TYPES_RESPONSE_PARAM = "instanceTypes";
  private static final String ELECTRONIC_ACCESS_PARAM = "electronicAccessRelationships";

  private MappingParametersBuilder() {
  }

  public static Future<MappingParameters> build(OkapiConnectionParams params) {
    Future<List<IdentifierType>> identifierTypesFuture = getIdentifierTypes(params);
    Future<List<ClassificationType>> classificationTypesFuture = getClassificationTypes(params);
    Future<List<InstanceType>> instanceTypesFuture = getInstanceTypes(params);
    Future<List<ElectronicAccessRelationship>> electronicAccessRelationshipsFuture = getElectronicAccessRelationships(params);
    return CompositeFuture.all(identifierTypesFuture, classificationTypesFuture, instanceTypesFuture, electronicAccessRelationshipsFuture)
      .map(ar ->
        new MappingParameters()
          .withIdentifierTypes(identifierTypesFuture.result())
          .withClassificationTypes(classificationTypesFuture.result())
          .withInstanceTypes(instanceTypesFuture.result())
          .withElectronicAccessRelationships(electronicAccessRelationshipsFuture.result())
      );
  }

  /**
   * Requests for Identifier types from application Settings (mod-inventory-storage)
   *
   * @param params Okapi connection parameters
   * @return List of Identifier types
   */
  private static Future<List<IdentifierType>> getIdentifierTypes(OkapiConnectionParams params) {
    Future<List<IdentifierType>> future = Future.future();
    RestUtil.doRequest(params, IDENTIFIER_TYPES_URL, HttpMethod.GET, null).setHandler(ar -> {
      if (RestUtil.validateAsyncResult(ar, future)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(IDENTIFIER_TYPES_RESPONSE_PARAM)) {
          List<IdentifierType> identifierTypeList = response.mapTo(IdentifierTypes.class).getIdentifierTypes();
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
  private static Future<List<ClassificationType>> getClassificationTypes(OkapiConnectionParams params) {
    Future<List<ClassificationType>> future = Future.future();
    RestUtil.doRequest(params, CLASSIFICATION_TYPES_URL, HttpMethod.GET, null).setHandler(ar -> {
      if (RestUtil.validateAsyncResult(ar, future)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(CLASSIFICATION_TYPES_RESPONSE_PARAM)) {
          List<ClassificationType> classificationTypeList = response.mapTo(ClassificationTypes.class).getClassificationTypes();
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
  private static Future<List<InstanceType>> getInstanceTypes(OkapiConnectionParams params) {
    Future<List<InstanceType>> future = Future.future();
    RestUtil.doRequest(params, INSTANCE_TYPES_URL, HttpMethod.GET, null).setHandler(ar -> {
      if (RestUtil.validateAsyncResult(ar, future)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(INSTANCE_TYPES_RESPONSE_PARAM)) {
          List<InstanceType> instanceTypeList = response.mapTo(InstanceTypes.class).getInstanceTypes();
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
  private static Future<List<ElectronicAccessRelationship>> getElectronicAccessRelationships(OkapiConnectionParams params) {
    Future<List<ElectronicAccessRelationship>> future = Future.future();
    RestUtil.doRequest(params, ELECTRONIC_ACCESS_URL, HttpMethod.GET, null).setHandler(ar -> {
      if (RestUtil.validateAsyncResult(ar, future)) {
        JsonObject response = ar.result().getJson();
        if (response != null && response.containsKey(ELECTRONIC_ACCESS_PARAM)) {
          List<ElectronicAccessRelationship> electronicAccessRelationshipList = response.mapTo(ElectronicAccessRelationships.class).getElectronicAccessRelationships();
          future.complete(electronicAccessRelationshipList);
        } else {
          future.complete(Collections.emptyList());
        }
      }
    });
    return future;
  }
}
