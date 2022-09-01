package org.folio.services;

import io.vertx.core.json.jackson.DatabindCodec;
import org.folio.rest.jaxrs.model.ActionProfile;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingProfile;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.springframework.stereotype.Service;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.folio.rest.jaxrs.model.ActionProfile.FolioRecord.AUTHORITY;
import static org.folio.rest.jaxrs.model.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.rest.jaxrs.model.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.rest.jaxrs.model.ActionProfile.FolioRecord.INVOICE;
import static org.folio.rest.jaxrs.model.ActionProfile.FolioRecord.ITEM;
import static org.folio.rest.jaxrs.model.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ActionProfile.FolioRecord.MARC_HOLDINGS;
import static org.folio.rest.jaxrs.model.ActionProfile.FolioRecord.ORDER;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.Record.RecordType.EDIFACT;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_HOLDING;

@Service
public class JobProfileSnapshotValidationServiceImpl implements JobProfileSnapshotValidationService {

  private final Map<Record.RecordType, EnumSet<ActionProfile.FolioRecord>> recordTypeToEntityType = Map.of(
    MARC_BIB, EnumSet.of(MARC_BIBLIOGRAPHIC, INSTANCE, HOLDINGS, ITEM, ORDER),
    MARC_HOLDING, EnumSet.of(MARC_HOLDINGS, HOLDINGS),
    MARC_AUTHORITY, EnumSet.of(ActionProfile.FolioRecord.MARC_AUTHORITY, AUTHORITY),
    EDIFACT, EnumSet.of(INVOICE)
  );

  @Override
  public boolean isJobProfileCompatibleWithRecordType(ProfileSnapshotWrapper jobProfileSnapshot, Record.RecordType recordType) {
    List<ProfileSnapshotWrapper> childWrappers = jobProfileSnapshot.getChildSnapshotWrappers();
    for (ProfileSnapshotWrapper childWrapper : childWrappers) {
      if (childWrapper.getContentType() == ACTION_PROFILE) {
        ActionProfile actionProfile = DatabindCodec.mapper().convertValue(childWrapper.getContent(), ActionProfile.class);
        EnumSet<ActionProfile.FolioRecord> eligibleEntityTypes = recordTypeToEntityType.get(recordType);
        if (!eligibleEntityTypes.contains(actionProfile.getFolioRecord())) {
          return false;
        }
      } else if (childWrapper.getContentType() == MAPPING_PROFILE) {
        MappingProfile mappingProfile = DatabindCodec.mapper().convertValue(childWrapper.getContent(), MappingProfile.class);
        if (!mappingProfile.getIncomingRecordType().equals(convertToEntityType(recordType))) {
          return false;
        }
      }
      if (!isJobProfileCompatibleWithRecordType(childWrapper, recordType)) {
        return false;
      }
    }
    return true;
  }

  private EntityType convertToEntityType(Record.RecordType recordType) {
    if (recordType == MARC_BIB) {
      return EntityType.MARC_BIBLIOGRAPHIC;
    } else if (recordType == MARC_HOLDING) {
      return EntityType.MARC_HOLDINGS;
    }
    return EntityType.fromValue(recordType.value());
  }

}
