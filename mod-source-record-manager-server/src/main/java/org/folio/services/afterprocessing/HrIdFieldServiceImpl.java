package org.folio.services.afterprocessing;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.rest.jaxrs.model.Record;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

import static org.folio.services.afterprocessing.AdditionalFieldsUtil.addControlledFieldToMarcRecord;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.addDataFieldToMarcRecord;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.isFieldExist;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.removeField;

@Service
public class HrIdFieldServiceImpl implements HrIdFieldService {

  private static final String HR_ID_FROM_FIELD = "001";
  private static final String HR_ID_TO_FIELD = "035";
  private static final char HR_ID_FIELD_SUB = 'a';
  private static final char HR_ID_FIELD_IND = ' ';

  public void moveHrIdFieldsAfterMapping(Map<Instance, Record> map) {
    map.entrySet().stream().parallel().forEach(entry -> {
      if (entry.getKey() != null && entry.getValue() != null) {
        String hrId = entry.getKey().getHrid();
        if (StringUtils.isNotEmpty(hrId)) {
          if (!isFieldExist(entry.getValue(), HR_ID_TO_FIELD, HR_ID_FIELD_SUB, hrId)) {
            addDataFieldToMarcRecord(entry.getValue(), HR_ID_TO_FIELD, HR_ID_FIELD_IND, HR_ID_FIELD_IND, HR_ID_FIELD_SUB, hrId);
          }
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
      if (StringUtils.isNotEmpty(hrId) && !isFieldExist(recordInstancePair.getKey(), HR_ID_FROM_FIELD, HR_ID_FIELD_SUB, hrId)) {
        addDataFieldToMarcRecord(recordInstancePair.getKey(), HR_ID_FROM_FIELD, HR_ID_FIELD_IND, HR_ID_FIELD_IND, HR_ID_FIELD_SUB, hrId);
      }
    });
  }
}
