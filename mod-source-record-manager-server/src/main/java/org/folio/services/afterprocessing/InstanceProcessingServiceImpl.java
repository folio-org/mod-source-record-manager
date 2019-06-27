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
import org.folio.rest.jaxrs.model.Instance;
import org.folio.rest.jaxrs.model.Instances;
import org.folio.rest.jaxrs.model.InstancesBatchResponse;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.mappers.RecordToInstanceMapper;
import org.folio.services.mappers.RecordToInstanceMapperBuilder;
import org.folio.services.parsers.RecordFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.folio.rest.jaxrs.model.JobExecutionSourceChunk.State.COMPLETED;
import static org.folio.rest.jaxrs.model.JobExecutionSourceChunk.State.ERROR;


@Service
public class InstanceProcessingServiceImpl implements AfterProcessingService {

  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceProcessingServiceImpl.class);
  private static final String INVENTORY_URL = "/inventory/instances/batch";
  public static final String CAN_NOT_MAP_RECORD_TO_INSTANCE_MSG = "Can not map a given Record to Instance. Cause: '%s'. Exception: '%s'";

  private JobExecutionSourceChunkDao jobExecutionSourceChunkDao;
  private AdditionalFieldsUtil additionalInstanceFieldsUtil;

  public InstanceProcessingServiceImpl(@Autowired JobExecutionSourceChunkDao jobExecutionSourceChunkDao,
                                       @Autowired AdditionalFieldsUtil additionalInstanceFieldsUtil) {
    this.jobExecutionSourceChunkDao = jobExecutionSourceChunkDao;
    this.additionalInstanceFieldsUtil = additionalInstanceFieldsUtil;
  }

  @Override
  public Future<Void> process(List<Record> records, String sourceChunkId, OkapiConnectionParams params) {
    Future<Void> future = Future.future();
    Map<Instance, Record> instanceRecordMap = mapInstanceToRecord(records);
    List<Instance> instances = new ArrayList<>(instanceRecordMap.keySet());
    postInstances(instances, params).setHandler(ar -> {
      JobExecutionSourceChunk.State sourceChunkState = ERROR;
      if (ar.succeeded()) {
        List<Instance> result = Optional.ofNullable(ar.result()).orElse(new ArrayList<>());
        List<Pair<Record, Instance>> recordsToUpdate = calculateRecordsToUpdate(instanceRecordMap, result);
        addAdditionalFields(recordsToUpdate, params);
        sourceChunkState = COMPLETED;
      }
      updateSourceChunkState(sourceChunkId, sourceChunkState, params)
        .compose(updatedChunk ->  jobExecutionSourceChunkDao.update(updatedChunk.withCompletedDate(new Date()), params.getTenantId()))
        // Complete future in order to continue the import process regardless of the result of creating Instances
        .setHandler(updateAr -> future.complete());
    });
    return future;
  }

  private List<Pair<Record, Instance>> calculateRecordsToUpdate(Map<Instance, Record> instanceRecordMap, List<Instance> result) {
    return result.stream()
      .map(it -> Pair.of(instanceRecordMap.get(it), it))
      .collect(Collectors.toList());
  }

  /**
   * Performs mapping a given Records to Instances.
   *
   * @param records given list of records
   * @return association between Records and corresponding Instances
   */
  private Map<Instance, Record> mapInstanceToRecord(List<Record> records) {
    if (CollectionUtils.isEmpty(records)) {
      return new HashMap<>();
    }
    final RecordToInstanceMapper mapper = RecordToInstanceMapperBuilder.buildMapper(RecordFormat.getByDataType(getRecordsType(records)));
    return records.parallelStream()
      .map(record -> mapInstanceToRecord(mapper, record))
      .filter(Objects::nonNull)
      .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  /**
   * Maps a record to an instance record.
   *
   * @param mapper a record to instance record mapper.
   * @param record a record.
   * @return either a pair of record-instance or null.
   */
  private Pair<Instance, Record> mapInstanceToRecord(RecordToInstanceMapper mapper, Record record) {
    try {
      if (record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
        Instance instance = mapper.mapRecord(new JsonObject(record.getParsedRecord().getContent().toString()));
        return Pair.of(instance, record);
      }
    } catch (Exception exception) {
      String errorMessage = String.format(CAN_NOT_MAP_RECORD_TO_INSTANCE_MSG, exception.getMessage(), exception);
      LOGGER.error(errorMessage);
    }
    return null;
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
        .peek(it -> additionalInstanceFieldsUtil.addInstanceIdToMarcRecord(it.getKey(), it.getValue().getId()))
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
   *  @param sourceChunkId id of source chunk
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
}
