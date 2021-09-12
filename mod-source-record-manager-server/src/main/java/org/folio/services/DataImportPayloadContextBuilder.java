package org.folio.services;

import org.folio.rest.jaxrs.model.Record;

import java.util.HashMap;

interface DataImportPayloadContextBuilder {
  HashMap<String, String> buildFrom(Record initialRecord, String profileSnapshotWrapperId);
}
