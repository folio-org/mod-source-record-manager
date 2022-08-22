package org.folio.services.afterprocessing;

import org.folio.rest.jaxrs.model.Record;
import org.springframework.stereotype.Service;

import java.util.List;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.TAG_999;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.addDataFieldToMarcRecord;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.getValue;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.isFieldExist;

@Service
public class HrIdFieldServiceImpl implements HrIdFieldService {

  private static final String TAG_001 = "001";
  private static final String TAG_003 = "003";
  private static final String TAG_035 = "035";
  private static final char SUBFIELD_FOR_035 = 'a';
  private static final char INDICATOR_FOR_035 = ' ';

  @Override
  public void move001valueTo035Field(List<Record> records) {
    records.stream().parallel().forEach(record -> {
      String valueFrom001 = getValue(record, TAG_001, ' ');
      String valueFrom999i = getValue(record, TAG_999, 'i');
      String valueFor035 = mergeFieldsFor035(getValue(record, TAG_003, ' '), valueFrom001);
      if (valueFrom001 != null && !isFieldExist(record, TAG_035, SUBFIELD_FOR_035, valueFor035)
        && valueFrom999i == null) {
        addDataFieldToMarcRecord(record, TAG_035, INDICATOR_FOR_035, INDICATOR_FOR_035, SUBFIELD_FOR_035, valueFor035);
      }
    });
  }

  private String mergeFieldsFor035(String valueFrom003, String valueFrom001) {
    if (isBlank(valueFrom003)) {
      return valueFrom001;
    }
    return "(" + valueFrom003 + ")" + valueFrom001;
  }

}
