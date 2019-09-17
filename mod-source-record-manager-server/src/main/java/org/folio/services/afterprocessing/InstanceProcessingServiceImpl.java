package org.folio.services.afterprocessing;

import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.HttpStatus;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.rest.client.SourceStorageBatchClient;
import org.folio.rest.client.SourceStorageClient;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.rest.jaxrs.model.Instances;
import org.folio.rest.jaxrs.model.InstancesBatchResponse;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.mappers.RecordToInstanceMapper;
import org.folio.services.mappers.RecordToInstanceMapperBuilder;
import org.folio.services.mappers.processor.parameters.MappingParameters;
import org.folio.services.mappers.processor.parameters.MappingParametersProvider;
import org.folio.services.parsers.RecordFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static org.folio.rest.jaxrs.model.JobExecutionSourceChunk.State.COMPLETED;
import static org.folio.rest.jaxrs.model.JobExecutionSourceChunk.State.ERROR;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.TAG_999;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.addFieldToMarcRecord;

@Service
public class InstanceProcessingServiceImpl implements AfterProcessingService {

  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceProcessingServiceImpl.class);
  private static final String INVENTORY_URL = "/inventory/instances/batch";

  private JobExecutionSourceChunkDao jobExecutionSourceChunkDao;
  private MappingParametersProvider mappingParametersProvider;

  public InstanceProcessingServiceImpl(@Autowired JobExecutionSourceChunkDao jobExecutionSourceChunkDao,
                                       @Autowired MappingParametersProvider mappingParametersProvider) {
    this.jobExecutionSourceChunkDao = jobExecutionSourceChunkDao;
  }

  @Override
  public Future<Void> process(List<Record> records, String sourceChunkId, OkapiConnectionParams okapiParams) {
    return Future.succeededFuture()
      .compose(ar -> getMappingParameters(records, okapiParams))
      .compose(mappingParameters -> mapRecords(records, sourceChunkId, mappingParameters, okapiParams));
  }

  /**
   * Provides external parameters for the MARC-to-Instance mapping process
   *
   * @param records list of incoming records
   * @param okapiParams okapi connection parameters
   * @return mapping parameters
   */
  private Future<MappingParameters> getMappingParameters(List<Record> records, OkapiConnectionParams okapiParams) {
    if (records.isEmpty()) {
      return Future.succeededFuture(new MappingParameters());
    } else {
      String key = records.get(0).getSnapshotId();
      return mappingParametersProvider.get(key, okapiParams);
    }
  }

  /**
   * Maps list of given Records on Instances,
   * Sends Instances to inventory,
   * Adds additional fields into parsed records and sends affected records to update,
   * Updates 'state' for source chunk,
   * Updates 'completed date' for job execution source chunk.
   *
   * @param records       - parsed records for processing
   * @param sourceChunkId - id of the JobExecutionSourceChunk
   * @param mappingParams - external parameters needed for mapping functions
   * @param okapiParams   - OkapiConnectionParams to interact with external services
   * @return future
   */
  private Future<Void> mapRecords(List<Record> records, String sourceChunkId, MappingParameters mappingParams, OkapiConnectionParams okapiParams) {
    Future future = Future.future();
    Map<Instance, Record> instanceRecordMap = mapRecords(records, mappingParams, okapiParams);
    List<Instance> instances = new ArrayList<>(instanceRecordMap.keySet());
    postInstances(instances, okapiParams).setHandler(ar -> {
      JobExecutionSourceChunk.State sourceChunkState = ERROR;
      if (ar.succeeded()) {
        List<Instance> result = Optional.ofNullable(ar.result()).orElse(new ArrayList<>());
        List<Pair<Record, Instance>> recordsToUpdate = calculateRecordsToUpdate(instanceRecordMap, result);
        addAdditionalFields(recordsToUpdate, okapiParams);
        sourceChunkState = COMPLETED;
      }
      updateSourceChunkState(sourceChunkId, sourceChunkState, okapiParams)
        .compose(updatedChunk -> jobExecutionSourceChunkDao.update(updatedChunk.withCompletedDate(new Date()), okapiParams.getTenantId()))
        // Complete future in order to continue the import process regardless of the result of creating Instances
        .setHandler(updateAr -> future.complete());
    });
    return future;
  }

  private List<Pair<Record, Instance>> calculateRecordsToUpdate(Map<Instance, Record> instanceRecordMap, List<Instance> instances) {
    //In performance perspective we creat a map Instance::id -> Record.
    //In addition, inventory can return an instance with extra fields or changed fields (metadata)
    //that influences on a hashCode of an Instance.
    Map<String, Record> instanceToRecordMap = instanceRecordMap.entrySet().stream()
      .collect(Collectors.toMap(entry -> entry.getKey().getId(), Map.Entry::getValue));

    return instances.stream()
      .map(it -> {
        Record record = instanceToRecordMap.get(it.getId());
        return nonNull(record) ? Pair.of(record, it) : null;
      })
      .filter(Objects::nonNull)
      .collect(Collectors.toList());
  }

  /**
   * Maps list of Records to Instances
   *
   * @param records given list of records
   * @param mappingParameters parameters needed for mapping rules
   * @return association between Records and corresponding Instances
   */
  private Map<Instance, Record> mapRecords(List<Record> records, MappingParameters mappingParameters, OkapiConnectionParams params) {
    if (CollectionUtils.isEmpty(records)) {
      return new HashMap<>();
    }
    final RecordToInstanceMapper mapper = RecordToInstanceMapperBuilder.buildMapper(RecordFormat.getByDataType(getRecordsType(records)));
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();
    return records.parallelStream()
      .map(record -> mapRecordToInstance(record, mapper, mappingParameters))
      .filter(Objects::nonNull)
      .filter(instanceRecordPair -> validateInstanceAndUpdateRecordIfInvalid(instanceRecordPair, validator, params))
      .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  /**
   * Maps Record to an Instance
   *
   * @param record Record
   * @param mapper Record to Instance mapper
   * @param mappingParameters parameters needed for mapping rules
   * @return either a pair of record-instance or null
   */
  private Pair<Instance, Record> mapRecordToInstance(Record record, RecordToInstanceMapper mapper, MappingParameters mappingParameters) {
    try {
      if (record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
        JsonObject parsedRecordContent = new JsonObject(record.getParsedRecord().getContent().toString());
        Instance instance = mapper.mapRecord(parsedRecordContent, mappingParameters);
        return Pair.of(instance, record);
      }
    } catch (Exception e) {
      LOGGER.error("Error mapping Record to Instance", e);
    }
    return null;
  }

  /**
   * Validates mapped Instance, updates Record if Instance is invalid
   *
   * @param instanceRecordPair pair containing Instance entity as a key that needs to be validated and Record entity as value
   * @param validator          Validator
   * @param params             OkapiConnectionParams to interact with external services
   * @return true if Instance is valid, false if invalid
   */
  private boolean validateInstanceAndUpdateRecordIfInvalid(Pair<Instance, Record> instanceRecordPair, Validator validator, OkapiConnectionParams params) {
    Set<ConstraintViolation<Instance>> violations = validator.validate(instanceRecordPair.getKey());
    if (!violations.isEmpty()) {
      Record record = instanceRecordPair.getValue().withErrorRecord(new ErrorRecord().withId(UUID.randomUUID().toString())
        .withDescription(String.format("Mapped Instance is invalid: %s", violations.toString()))
        .withContent(instanceRecordPair.getKey()));
      updateRecord(record, params);
      return false;
    }
    return true;
  }

  /**
   * Sends given collection of Instances to mod-inventory
   *
   * @param instanceList collection of Instances
   * @param params       Okapi connection params
   * @return future
   */
  private Future<List<Instance>> postInstances(List<Instance> instanceList, OkapiConnectionParams params) {
    if (CollectionUtils.isEmpty(instanceList)) {
      return Future.succeededFuture();
    }
    Future<List<Instance>> future = Future.future();
    Instances instances = new Instances().withInstances(instanceList).withTotalRecords(instanceList.size());
    RestUtil.doRequest(params, INVENTORY_URL, HttpMethod.POST, instances).setHandler(responseResult -> {
      try {
        if (RestUtil.validateAsyncResult(responseResult, future)) {
          InstancesBatchResponse response = responseResult.result().getJson().mapTo(InstancesBatchResponse.class);
          future.complete(response.getInstances());
        } else {
          LOGGER.error("Error creating a new collection of Instances", future.cause());
        }
      } catch (Exception e) {
        LOGGER.error("Error during post for new collection of Instances", e);
        future.fail(e);
      }
    });
    return future;
  }

  /**
   * Adds additional custom fields to parsed records and updates parsed records in mod-source-record-storage
   *
   * @param recordToInstanceList association between Records and corresponding Instances
   * @param params               okapi connection params
   */
  private void addAdditionalFields(List<Pair<Record, Instance>> recordToInstanceList, OkapiConnectionParams params) {
    if (CollectionUtils.isEmpty(recordToInstanceList)) {
      return;
    }

    if (Record.RecordType.MARC == recordToInstanceList.get(0).getKey().getRecordType()) {
      List<Record> records = recordToInstanceList.parallelStream()
        .peek(recordInstancePair -> addFieldToMarcRecord(recordInstancePair.getKey(), TAG_999, 'i', recordInstancePair.getValue().getId()))
        .map(Pair::getKey)
        .collect(Collectors.toList());

      updateParsedRecords(records, params).setHandler(updatedAr -> {
        if (updatedAr.failed()) {
          LOGGER.error("Couldn't update parsed records", updatedAr.cause());
        }
      });
    }
  }

  /**
   * Updates state of given source chunk
   *
   * @param sourceChunkId id of source chunk
   * @param state         state of source chunk
   * @param params        okapi connection params
   * @return future with updated JobExecutionSourceChunk entity
   */
  private Future<JobExecutionSourceChunk> updateSourceChunkState(String sourceChunkId, JobExecutionSourceChunk.State state, OkapiConnectionParams params) {
    return jobExecutionSourceChunkDao.getById(sourceChunkId, params.getTenantId())
      .compose(optional -> optional
        .map(sourceChunk -> jobExecutionSourceChunkDao.update(sourceChunk.withState(state), params.getTenantId()))
        .orElseThrow(() ->
          new NotFoundException(
            format("Couldn't update failed jobExecutionSourceChunk status to ERROR, jobExecutionSourceChunk with id %s was not found", sourceChunkId))));
  }

  /**
   * Return type of records
   *
   * @param records list of records
   * @return type of records
   */
  private Record.RecordType getRecordsType(List<Record> records) {
    return records.get(0).getRecordType();
  }

  /**
   * Updates a collection of parsedRecords
   *
   * @param records collection of records containing updated parsed records
   * @param params  okapi connection params
   * @return Future
   */
  private Future<Void> updateParsedRecords(List<Record> records, OkapiConnectionParams params) {
    if (CollectionUtils.isEmpty(records)) {
      return Future.succeededFuture();
    }
    Future<Void> future = Future.future();
    try {
      new SourceStorageBatchClient(params.getOkapiUrl(), params.getTenantId(), params.getToken())
        .putSourceStorageBatchParsedRecords(buildParsedRecordCollection(records), response -> {
          if (HttpStatus.HTTP_OK.toInt() != response.statusCode()) {
            setFail(future, response.statusCode());
          }
        });
    } catch (Exception e) {
      LOGGER.error("Failed to update parsed records collection", e);
      future.fail(e);
    }
    return future.isComplete() ? future : Future.succeededFuture();
  }

  private void setFail(Future<Void> future, int statusCode) {
    String errorMessage = format("Couldn't update parsed records collection - response status code %s, expected 200", statusCode);
    LOGGER.error(errorMessage);
    future.fail(errorMessage);
  }

  private ParsedRecordCollection buildParsedRecordCollection(List<Record> records) {
    List<ParsedRecord> parsedRecords = records.stream()
      .filter(record -> record.getParsedRecord() != null)
      .map(Record::getParsedRecord)
      .collect(Collectors.toList());

    return new ParsedRecordCollection()
      .withParsedRecords(parsedRecords)
      .withTotalRecords(parsedRecords.size())
      .withRecordType(ParsedRecordCollection.RecordType.valueOf(getRecordsType(records).value()));
  }

  /**
   * Sends request to update Record
   *
   * @param record Record to update
   * @param params OkapiConnectionParams to interact with external services
   * @return future with true if Record was updated, false otherwise
   */
  private Future<Boolean> updateRecord(Record record, OkapiConnectionParams params) {
    Future<Boolean> future = Future.future();
    try {
      SourceStorageClient client = new SourceStorageClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
      client.putSourceStorageRecordsById(record.getId(), null, record, response -> {
        if (HttpStatus.HTTP_OK.toInt() == response.statusCode()) {
          future.complete(true);
        } else {
          LOGGER.error("Record {} was not updated", record.getId());
          future.complete(false);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Error updating Record", e);
      future.fail(e);
    }
    return future;
  }
}
