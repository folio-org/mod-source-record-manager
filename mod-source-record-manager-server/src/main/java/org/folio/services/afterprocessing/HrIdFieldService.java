package org.folio.services.afterprocessing;

import org.folio.rest.jaxrs.model.Record;

import java.util.List;

public interface HrIdFieldService {

  /**
   * Method move 001 field to 035
   *
   * @param records - list of MARC records
   */
  void move001valueTo035Field(List<Record> records);

}
