package org.folio.services.afterprocessing;

import org.apache.commons.lang3.tuple.Pair;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.rest.jaxrs.model.Record;

import java.util.List;
import java.util.Map;

public interface HrIdFieldService {
  void moveHrIdFieldsAfterMapping(Map<Instance, Record> map);
  void fillHrIdFieldInMarcRecord(List<Pair<Record, Instance>> list);
}
