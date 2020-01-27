package org.folio.services.afterprocessing;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.rest.jaxrs.model.Record;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

import static org.folio.services.afterprocessing.AdditionalFieldsUtil.addControlledFieldToMarcRecord;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.removeField;

@Service
public class HrIdFieldServiceImpl implements HrIdFieldService {

  private static final String HR_ID_FROM_FIELD = "001";
  private static final String HR_ID_TO_FIELD = "035";

  public void moveHrIdFieldsAfterMapping(Map<Instance, Record> map) {
    map.entrySet().stream().parallel().forEach(entry -> {
      if (entry.getKey() != null && entry.getValue() != null) {
        String hrId = entry.getKey().getHrid();
        if (StringUtils.isNotEmpty(hrId)) {
          addControlledFieldToMarcRecord(entry.getValue(), HR_ID_TO_FIELD, hrId);
          removeField(entry.getValue(), HR_ID_FROM_FIELD);
          // clearing hrId field in instance to generate new one in inventory
          entry.getKey().setHrid(null);
        }
      }
    });
  }

  @Override
  public void fillHrIdFieldInMarcRecord(List<Pair<Record, Instance>> list) {
    list.stream().parallel().forEach(recordInstancePair -> {
      String hrId = recordInstancePair.getValue().getHrid();
      if (StringUtils.isNotEmpty(hrId)) {
        addControlledFieldToMarcRecord(recordInstancePair.getKey(), HR_ID_FROM_FIELD, hrId);
      }
    });
  }
}
