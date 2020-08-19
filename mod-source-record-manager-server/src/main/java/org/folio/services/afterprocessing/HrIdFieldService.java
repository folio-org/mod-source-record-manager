package org.folio.services.afterprocessing;

import org.apache.commons.lang3.tuple.Pair;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.rest.jaxrs.model.Record;

import java.util.List;
import java.util.Map;

public interface HrIdFieldService {

  /**
   * Method move 001 field to 035
   *
   * @param records - list of MARC records
   */
  void move001valueTo035Field(List<Record> records);

  /**
   * Method fill 001 filed of marc records with instance hrId
   *
   * @param list - list with with Instance to Record relations
   */
  void fillHrIdFieldInMarcRecord(List<Pair<Record, Instance>> list);
}
