package org.folio.services.afterprocessing;

import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.collections4.CollectionUtils;
import org.folio.rest.jaxrs.model.Record;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamWriter;
import org.marc4j.MarcWriter;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.DataField;
import org.marc4j.marc.MarcFactory;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.VariableField;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * Util to work with additional fields
 */
public final class AdditionalFieldsUtil {

  public static final String TAG_999 = "999";

  private static final Logger LOGGER = LogManager.getLogger();
  private static final char INDICATOR = 'f';

  private AdditionalFieldsUtil() {
  }

  /**
   * Adds field if it does not exist and a subfield with a value to that field
   *
   * @param record   record that needs to be updated
   * @param field    field that should contain new subfield
   * @param subfield new subfield to add
   * @param value    value of the subfield to add
   * @return true if succeeded, false otherwise
   */
  public static boolean addFieldToMarcRecord(Record record, String field, char subfield, String value) {
    boolean result = false;
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
        MarcReader reader = buildMarcReader(record);
        MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
        MarcFactory factory = MarcFactory.newInstance();
        if (reader.hasNext()) {
          org.marc4j.marc.Record marcRecord = reader.next();
          VariableField variableField = getSingleFieldByIndicators(marcRecord.getVariableFields(field), INDICATOR, INDICATOR);
          DataField dataField;
          if (variableField != null
            && ((DataField) variableField).getIndicator1() == INDICATOR
            && ((DataField) variableField).getIndicator2() == INDICATOR
          ) {
            dataField = (DataField) variableField;
            marcRecord.removeVariableField(variableField);
            dataField.removeSubfield(dataField.getSubfield(subfield));
          } else {
            dataField = factory.newDataField(field, INDICATOR, INDICATOR);
          }
          dataField.addSubfield(factory.newSubfield(subfield, value));
          marcRecord.addVariableField(dataField);
          // use stream writer to recalculate leader
          streamWriter.write(marcRecord);
          jsonWriter.write(marcRecord);
          record.setParsedRecord(record.getParsedRecord().withContent(new JsonObject(new String(os.toByteArray())).encode()));
          result = true;
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to add additional subfield {} for field {} to record {}", subfield, field, record.getId(), e);
    }
    return result;
  }

  /**
   * Adds new controlled field to marc record
   *
   * @param record record that needs to be updated
   * @param field  tag of controlled field
   * @param value  value of the field to add
   * @return true if succeeded, false otherwise
   */
  public static boolean addControlledFieldToMarcRecord(Record record, String field, String value) {
    boolean result = false;
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
        MarcReader reader = buildMarcReader(record);
        MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
        MarcFactory factory = MarcFactory.newInstance();
        if (reader.hasNext()) {
          org.marc4j.marc.Record marcRecord = reader.next();
          ControlField dataField = factory.newControlField(field, value);
          marcRecord.addVariableField(dataField);
          // use stream writer to recalculate leader
          streamWriter.write(marcRecord);
          jsonWriter.write(marcRecord);
          record.setParsedRecord(record.getParsedRecord().withContent(new JsonObject(new String(os.toByteArray())).encode()));
          result = true;
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to add additional controlled field {} to record {}", field, record.getId(), e);
    }
    return result;
  }

  /**
   * Adds new data field to marc record
   *
   * @param record record that needs to be updated
   * @param tag    tag of data field
   * @param value  value of the field to add
   * @return true if succeeded, false otherwise
   */
  public static boolean addDataFieldToMarcRecord(Record record, String tag, char ind1, char ind2, char subfield, String value) {
    boolean result = false;
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
        MarcReader reader = buildMarcReader(record);
        MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
        MarcFactory factory = MarcFactory.newInstance();
        if (reader.hasNext()) {
          org.marc4j.marc.Record marcRecord = reader.next();
          DataField dataField = factory.newDataField(tag, ind1, ind2);
          dataField.addSubfield(factory.newSubfield(subfield, value));
          addDataFieldInNumericalOrder(dataField, marcRecord);
          // use stream writer to recalculate leader
          streamWriter.write(marcRecord);
          jsonWriter.write(marcRecord);
          record.setParsedRecord(record.getParsedRecord().withContent(new JsonObject(new String(os.toByteArray())).encode()));
          result = true;
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to add additional data field {} to record {}", tag, record.getId(), e);
    }
    return result;
  }

  private static void addDataFieldInNumericalOrder(DataField field, org.marc4j.marc.Record marcRecord) {
    String tag = field.getTag();
    List<DataField> dataFields = marcRecord.getDataFields();
    for (int i = 0; i < dataFields.size(); i++) {
      if (dataFields.get(i).getTag().compareTo(tag) > 0) {
        marcRecord.getDataFields().add(i, field);
        return;
      }
    }
    marcRecord.addVariableField(field);
  }

  /**
   * Check if data field with the same value exist
   *
   * @param record record that needs to be updated
   * @param tag    tag of data field
   * @param value  value of the field to add
   * @return true if exist
   */
  public static boolean isFieldExist(Record record, String tag, char subfield, String value) {
    if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
      MarcReader reader = buildMarcReader(record);
      try {
        if (reader.hasNext()) {
          org.marc4j.marc.Record marcRecord = reader.next();
          for (VariableField field : marcRecord.getVariableFields(tag)) {
            if (field instanceof DataField) {
              for (Subfield sub : ((DataField) field).getSubfields(subfield)) {
                if (isNotEmpty(sub.getData()) && sub.getData().equals(value.trim())) {
                  return true;
                }
              }
            } else if (field instanceof ControlField
              && isNotEmpty(((ControlField) field).getData())
              && ((ControlField) field).getData().equals(value.trim())) {
              return true;
            }
          }
        }
      } catch (Exception e) {
        LOGGER.error("Error during the search a field in the record", e);
        return false;
      }
    }
    return false;
  }

  /**
   * Extracts value from specified field
   *
   * @param record record
   * @param tag    tag of data field
   * @return value from the specified field, or null
   */
  public static String getControlFieldValue(Record record, String tag) {
    if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
      MarcReader reader = buildMarcReader(record);
      try {
        if (reader.hasNext()) {
          org.marc4j.marc.Record marcRecord = reader.next();
          return marcRecord.getControlFields().stream()
            .filter(controlField -> controlField.getTag().equals(tag))
            .findFirst()
            .map(ControlField::getData)
            .orElse(null);
        }
      } catch (Exception e) {
        LOGGER.error("Error during the search a field in the record", e);
        return null;
      }
    }
    return null;
  }

  /**
   * Extracts value from specified field
   *
   * @param record record
   * @param tag    tag of data field
   * @return value from the specified field, or null
   */
  public static String getValue(Record record, String tag, char subfield) {
    if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
      MarcReader reader = buildMarcReader(record);
      try {
        if (reader.hasNext()) {
          org.marc4j.marc.Record marcRecord = reader.next();
          for (VariableField field : marcRecord.getVariableFields(tag)) {
            if (field instanceof DataField) {
              if (CollectionUtils.isNotEmpty(((DataField) field).getSubfields(subfield))) {
                return ((DataField) field).getSubfields(subfield).get(0).getData();
              }
            } else if (field instanceof ControlField) {
              return ((ControlField) field).getData();
            }
          }
        }
      } catch (Exception e) {
        LOGGER.error("Error during the search a field in the record", e);
        return null;
      }
    }
    return null;
  }

  /**
   * remove field from marc record
   *
   * @param record record that needs to be updated
   * @param field  tag of the field
   * @return true if succeeded, false otherwise
   */
  public static boolean removeField(Record record, String field) {
    boolean result = false;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
        MarcReader reader = buildMarcReader(record);
        MarcWriter marcStreamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter marcJsonWriter = new MarcJsonWriter(baos);
        if (reader.hasNext()) {
          org.marc4j.marc.Record marcRecord = reader.next();
          VariableField variableField = marcRecord.getVariableField(field);
          if (variableField != null) {
            marcRecord.removeVariableField(variableField);
          }
          // use stream writer to recalculate leader
          marcStreamWriter.write(marcRecord);
          marcJsonWriter.write(marcRecord);
          record.setParsedRecord(record.getParsedRecord().withContent(new JsonObject(new String(baos.toByteArray())).encode()));
          result = true;
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to remove controlled field {) from record {}", e, field, record.getId());
    }
    return result;
  }

  private static MarcReader buildMarcReader(Record record) {
    return new MarcJsonReader(new ByteArrayInputStream(record.getParsedRecord().getContent().toString().getBytes(StandardCharsets.UTF_8)));
  }

  public static boolean hasIndicator(Record record, char subfield) {
    MarcReader reader = buildMarcReader(record);
    if (reader.hasNext()) {
      org.marc4j.marc.Record marcRecord = reader.next();
      VariableField variableField = getSingleFieldByIndicators(marcRecord.getVariableFields(TAG_999), INDICATOR, INDICATOR);
      return variableField != null
        && ((DataField) variableField).getSubfield(subfield) != null;
    }
    return false;
  }

  private static VariableField getSingleFieldByIndicators(List<VariableField> list, char ind1, char ind2) {
    if (list == null || list.isEmpty()) {
      return null;
    }
    return list.stream()
      .filter(f -> ((DataField) f).getIndicator1() == ind1 && ((DataField) f).getIndicator2() == ind2)
      .findFirst()
      .orElse(null);
  }
}
