package org.folio.services;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.folio.dataimport.util.marc.MarcRecordAnalyzer;
import org.folio.dataimport.util.marc.MarcRecordType;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Record;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.folio.dataimport.util.marc.MarcRecordType.AUTHORITY;
import static org.folio.dataimport.util.marc.MarcRecordType.BIB;
import static org.folio.dataimport.util.marc.MarcRecordType.HOLDING;
import static org.folio.rest.jaxrs.model.EntityType.EDIFACT_INVOICE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.EntityType.MARC_HOLDINGS;

@Component
class DataImportPayloadContextBuilderImpl implements DataImportPayloadContextBuilder {

  private static final Map<MarcRecordType, EntityType> MARC_TO_ENTITY_TYPE;

  private final MarcRecordAnalyzer analyzer;

  static {
    MARC_TO_ENTITY_TYPE = new EnumMap<>(MarcRecordType.class);

    MARC_TO_ENTITY_TYPE.put(BIB, MARC_BIBLIOGRAPHIC);
    MARC_TO_ENTITY_TYPE.put(HOLDING, MARC_HOLDINGS);
    MARC_TO_ENTITY_TYPE.put(AUTHORITY, MARC_AUTHORITY);
  }

  public DataImportPayloadContextBuilderImpl(MarcRecordAnalyzer analyzer) {
    this.analyzer = analyzer;
  }

  @Override
  public HashMap<String, String> buildFrom(Record initialRecord, String profileSnapshotWrapperId) {
    EntityType entityType = detectEntityType(initialRecord);

    return createAndPopulateContext(entityType, initialRecord, profileSnapshotWrapperId);
  }

  private HashMap<String, String> createAndPopulateContext(EntityType entityType, Record initialRecord, String profileSnapshotWrapperId) {
    HashMap<String, String> context = new HashMap<>();

    context.put(entityType.value(), Json.encode(initialRecord));
    context.put("JOB_PROFILE_SNAPSHOT_ID", profileSnapshotWrapperId);
    context.put("INCOMING_RECORD_ID", initialRecord.getId());
    return context;
  }

  private EntityType detectEntityType(Record initialRecord) {
    switch (initialRecord.getRecordType()) {
      case EDIFACT:
        return EDIFACT_INVOICE;
      case MARC_BIB:
      case MARC_HOLDING:
      case MARC_AUTHORITY:
        return getEntityType(initialRecord);
      default:
        throw new IllegalStateException("Unexpected record type: " + initialRecord.getRecordType());
    }
  }

  private EntityType getEntityType(Record marcRecord) {
    requireNonNull(marcRecord.getParsedRecord(), "Parsed record is null");
    requireNonNull(marcRecord.getParsedRecord().getContent(), "Parsed record content is null");

    MarcRecordType type = analyzer.process(new JsonObject(marcRecord.getParsedRecord().getContent().toString()));

    EntityType entityType = MARC_TO_ENTITY_TYPE.get(type);
    if (entityType == null) {
      throw new IllegalStateException("Unsupported Marc record type");
    }
    return entityType;
  }
}
