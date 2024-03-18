package org.folio.services.validation;

import io.vertx.core.json.JsonObject;
import org.folio.rest.jaxrs.model.ActionProfile;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.JobProfile;
import org.folio.rest.jaxrs.model.MappingProfile;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Test;

import java.util.List;

import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_HOLDING;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JobProfileSnapshotValidationServiceImplTest {

  private JobProfile jobProfile = new JobProfile()
    .withDataType(JobProfile.DataType.MARC);

  private ActionProfile actionProfile = new ActionProfile()
    .withName("Create Marc holdings")
    .withAction(ActionProfile.Action.CREATE)
    .withFolioRecord(ActionProfile.FolioRecord.HOLDINGS);

  private MappingProfile mappingProfile = new MappingProfile()
    .withIncomingRecordType(EntityType.MARC_HOLDINGS)
    .withExistingRecordType(EntityType.HOLDINGS);

  private ProfileSnapshotWrapper createMarcHoldingsJobProfileSnapshot = new ProfileSnapshotWrapper()
    .withContentType(JOB_PROFILE)
    .withContent(jobProfile)
    .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
      .withContentType(ACTION_PROFILE)
      .withContent(JsonObject.mapFrom(actionProfile).getMap())
      .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
        .withContentType(MAPPING_PROFILE)
        .withContent(JsonObject.mapFrom(mappingProfile).getMap())
      ))
    ));

  private JobProfileSnapshotValidationServiceImpl jobProfileValidationService = new JobProfileSnapshotValidationServiceImpl();

  @Test
  public void shouldReturnFalseIfProfileSnapshotContainsMappingProfileIncompatibleWithRecordType() {
    boolean isJobProfileCompatible =
      jobProfileValidationService.isJobProfileCompatibleWithRecordType(createMarcHoldingsJobProfileSnapshot, MARC_BIB);
    assertFalse(isJobProfileCompatible);
  }

  @Test
  public void shouldReturnTrueIfProfileSnapshotContainsMappingProfileCompatibleWithMarcHoldingRecordType() {
    boolean isJobProfileCompatible =
      jobProfileValidationService.isJobProfileCompatibleWithRecordType(createMarcHoldingsJobProfileSnapshot, MARC_HOLDING);
    assertTrue(isJobProfileCompatible);
  }

}
