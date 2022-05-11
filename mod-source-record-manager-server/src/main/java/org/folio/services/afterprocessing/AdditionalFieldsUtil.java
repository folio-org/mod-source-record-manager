package org.folio.services.afterprocessing;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.jaxrs.model.Record;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcJsonWriter;
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
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * Util to work with additional fields
 */
public final class AdditionalFieldsUtil {

  public static final String TAG_999 = "999";
  public static final char INDICATOR = 'f';
  public static final char SUBFIELD_I = 'i';
  public static final char SUBFIELD_S = 's';

  private static final Logger LOGGER = LogManager.getLogger();

  private final static CacheLoader<Object, org.marc4j.marc.Record> parsedRecordContentCacheLoader;
  private final static LoadingCache<Object, org.marc4j.marc.Record> parsedRecordContentCache;

  static {
    // this function is executed when creating a new item to be saved in the cache.
    // In this case, this is a MARC4J Record
    parsedRecordContentCacheLoader =
      parsedRecordContent -> {
        MarcJsonReader marcJsonReader =
          new MarcJsonReader(
            new ByteArrayInputStream(
              parsedRecordContent.toString().getBytes(StandardCharsets.UTF_8)));
        if (marcJsonReader.hasNext()) {
          return marcJsonReader.next();
        }
        return null;
      };

    parsedRecordContentCache =
        Caffeine.newBuilder()
            .maximumSize(2000)
            // weak keys allows parsed content strings that are used as keys to be garbage
            // collected, even it is still
            // referenced by the cache.
            .weakKeys()
            .recordStats()
            .executor(
                serviceExecutor -> {
                  // Due to the static nature and the API of this AdditionalFieldsUtil class, it is difficult to
                  // pass a vertx instance or assume whether a call to any of its static methods here is by a Vertx
                  // thread or a regular thread. The logic before is able to discern the type of thread and execute
                  // cache operations using the appropriate threading model.
                  Context context = Vertx.currentContext();
                  if (context != null) {
                    context.runOnContext(ar -> serviceExecutor.run());
                  }
                  else {
                    // The common pool below is used because it is the  default executor for caffeine
                    ForkJoinPool.commonPool().execute(serviceExecutor);
                  }
                })
          .build(parsedRecordContentCacheLoader);
  }

  private AdditionalFieldsUtil() {
  }

  /**
   * Get cache stats
   */
  static CacheStats getCacheStats() {
    return parsedRecordContentCache.stats();
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
        MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
        MarcFactory factory = MarcFactory.newInstance();
        org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
        if (marcRecord != null) {
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

          String parsedContentString = new JsonObject(os.toString()).encode();
          // save parsed content string to cache then set it on the record
          parsedRecordContentCache.put(parsedContentString, marcRecord);
          record.setParsedRecord(record.getParsedRecord().withContent(parsedContentString));
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
      org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
      if (marcRecord != null) {
        MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
        MarcFactory factory = MarcFactory.newInstance();
        ControlField dataField = factory.newControlField(field, value);
        marcRecord.addVariableField(dataField);
        // use stream writer to recalculate leader
        streamWriter.write(marcRecord);
        jsonWriter.write(marcRecord);

        String parsedContentString = new JsonObject(os.toString()).encode();
        // save parsed content string to cache then set it on the record
        parsedRecordContentCache.put(parsedContentString, marcRecord);
        record.setParsedRecord(
            record
                .getParsedRecord()
                .withContent(parsedContentString));
        result = true;
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
      org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
      if (marcRecord != null) {
        MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
        MarcFactory factory = MarcFactory.newInstance();
        DataField dataField = factory.newDataField(tag, ind1, ind2);
        dataField.addSubfield(factory.newSubfield(subfield, value));
        addDataFieldInNumericalOrder(dataField, marcRecord);
        // use stream writer to recalculate leader
        streamWriter.write(marcRecord);
        jsonWriter.write(marcRecord);

        String parsedContentString = new JsonObject(os.toString()).encode();
        // save parsed content string to cache then set it on the record
        parsedRecordContentCache.put(parsedContentString, marcRecord);
        record.setParsedRecord(
            record
                .getParsedRecord()
                .withContent(parsedContentString));
        result = true;
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
   * @param tag tag of data field
   * @param value value of the field to add
   * @return true if exist
   */
  public static boolean isFieldExist(Record record, String tag, char subfield, String value) {
    org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
    if (marcRecord != null) {
      try {
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
   * @param tag tag of data field
   * @return value from the specified field, or null
   */
  public static String getControlFieldValue(Record record, String tag) {
    org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
    if (marcRecord != null) {
      try {
        return marcRecord.getControlFields().stream()
            .filter(controlField -> controlField.getTag().equals(tag))
            .findFirst()
            .map(ControlField::getData)
            .orElse(null);
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
    org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
    if (marcRecord != null) {
      try {
        for (VariableField field : marcRecord.getVariableFields(tag)) {
          if (field instanceof DataField) {
            if (CollectionUtils.isNotEmpty(((DataField) field).getSubfields(subfield))) {
              return ((DataField) field).getSubfields(subfield).get(0).getData();
            }
          } else if (field instanceof ControlField) {
            return ((ControlField) field).getData();
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
   * @param field tag of the field
   * @return true if succeeded, false otherwise
   */
  public static boolean removeField(Record record, String field) {
    boolean result = false;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
      if (marcRecord != null) {
        MarcWriter marcStreamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter marcJsonWriter = new MarcJsonWriter(baos);
        VariableField variableField = marcRecord.getVariableField(field);
        if (variableField != null) {
          marcRecord.removeVariableField(variableField);
        }
        // use stream writer to recalculate leader
        marcStreamWriter.write(marcRecord);
        marcJsonWriter.write(marcRecord);

        String parsedContentString = new JsonObject(baos.toString()).encode();
        // save parsed content string to cache then set it on the record
        parsedRecordContentCache.put(parsedContentString, marcRecord);
        record.setParsedRecord(
          record
            .getParsedRecord()
            .withContent(parsedContentString));
        result = true;
      }
    } catch (Exception e) {
      LOGGER.error("Failed to remove controlled field {} from record {}", field, record.getId(), e);
    }
    return result;
  }

  /**
   * Generate a {@link org.marc4j.marc.Record} from {@link Record} passed in.
   * Will return null when there is no parsed content string present. Generated MARC record will be saved into cache if
   * its parsed content string is not present in the cache as a key
   */
  private static org.marc4j.marc.Record computeMarcRecord(Record record) {
    if (record != null
        && record.getParsedRecord() != null
        && !StringUtils.isBlank(record.getParsedRecord().getContent().toString())) {
      try {
        return parsedRecordContentCache.get(record.getParsedRecord().getContent());
      } catch (Exception e) {
        LOGGER.error("Error during the transformation to marc record", e);
        return null;
      }
    }
    return null;
  }

  public static boolean hasIndicator(Record record, char subfield) {
    org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
    if (marcRecord != null) {
      VariableField variableField = getSingleFieldByIndicators(marcRecord.getVariableFields(TAG_999), INDICATOR, INDICATOR);
      return Objects.nonNull(variableField)
        && Objects.nonNull(((DataField) variableField).getSubfield(subfield));
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
