package org.folio.services.validation;

import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;

public interface JobProfileSnapshotValidationService {

  /**
   * Checks whether job profile snapshot is compatible with record type.
   *
   * @param jobProfileSnapshot job profile snapshot
   * @param recordType         source record type
   * @return {@code true} if the specified job profile snapshot is compatible with specified record type, otherwise {@code false}
   */
  boolean isJobProfileCompatibleWithRecordType(ProfileSnapshotWrapper jobProfileSnapshot, Record.RecordType recordType);

}
