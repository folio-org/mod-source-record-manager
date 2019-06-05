package org.folio.services.afterprocessing;

import javax.ws.rs.NotFoundException;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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
import org.folio.rest.client.SourceStorageClient;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.rest.jaxrs.model.Instances;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.mappers.RecordToInstanceMapper;
import org.folio.services.mappers.RecordToInstanceMapperBuilder;
import org.folio.services.parsers.RecordFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class InstanceProcessingServiceImpl implements AfterProcessingService {

  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceProcessingServiceImpl.class);
  private static final String INVENTORY_URL = "/inventory/instances/batch";

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
    List<Pair<Record, Instance>> recordToInstanceList = mapRecordsToInstances(records);
    List<Instance> instances = recordToInstanceList.parallelStream().map(Pair::getValue).collect(Collectors.toList());
    postInstances(instances, params).setHandler(ar -> {
      if (ar.failed()) {
        updateSourceChunkState(sourceChunkId, JobExecutionSourceChunk.State.ERROR, params);
      } else {
        addAdditionalFields(recordToInstanceList, params);
      }
      // Complete future in order to continue the import process regardless of the result of creating Instances
      future.complete();
    });
    return future;
  }

  /**
   * Performs mapping a given Records to Instances.
   *
   * @param records given list of records
   * @return association between Records and corresponding Instances
   */
  private List<Pair<Record, Instance>> mapRecordsToInstances(List<Record> records) {
    if (CollectionUtils.isEmpty(records)) {
      return Collections.emptyList();
    }
    final RecordToInstanceMapper mapper = RecordToInstanceMapperBuilder.buildMapper(RecordFormat.getByDataType(getRecordsType(records)));
    return records.parallelStream()
      .map(record -> mapRecordToInstance(mapper, record))
      .filter(Objects::nonNull)
      .collect(Collectors.toList());
  }

  /**
   * Maps a record to an instance record.
   *
   * @param mapper a record to instance record mapper.
   * @param record a record.
   * @return either a pair of record-instance or null.
   */
  private Pair<Record, Instance> mapRecordToInstance(RecordToInstanceMapper mapper, Record record) {
    try {
      if (record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
        Instance instance = mapper.mapRecord(new JsonObject(record.getParsedRecord().getContent().toString()));
        return Pair.of(record, instance);
      }
    } catch (Exception exception) {
      String errorMessage = String.format("Can not map a given Record to Instance. Cause: '%s'. Exception: '%s'", exception.getMessage(), exception);
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
  private Future<Void> postInstances(List<Instance> instanceList, OkapiConnectionParams params) {
    if (CollectionUtils.isEmpty(instanceList)) {
      return Future.succeededFuture();
    }
    Future<Void> future = Future.future();
    Instances instances = new Instances().withInstances(instanceList).withTotalRecords(instanceList.size());
    RestUtil.doRequest(params, INVENTORY_URL, HttpMethod.POST, instances).setHandler(responseResult -> {
      try {
        if (!RestUtil.validateAsyncResult(responseResult, future)) {
          LOGGER.error("Error creating a new collection of Instances", future.cause());
        } else {
          future.complete();
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
   *
   * @param sourceChunkId id of source chunk
   * @param state         state of source chunk
   * @param params        okapi connection params
   */
  private void updateSourceChunkState(String sourceChunkId, JobExecutionSourceChunk.State state, OkapiConnectionParams params) {
    jobExecutionSourceChunkDao.getById(sourceChunkId, params.getTenantId())
      .compose(optional -> optional
        .map(sourceChunk -> jobExecutionSourceChunkDao.update(sourceChunk.withState(state), params.getTenantId()))
        .orElseThrow(() ->
          new NotFoundException(
            String.format("Couldn't update failed jobExecutionSourceChunk status to ERROR, jobExecutionSourceChunk with id %s was not found", sourceChunkId))));
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
      new SourceStorageClient(params.getOkapiUrl(), params.getTenantId(), params.getToken())
        .putSourceStorageParsedRecordsCollection(buildParsedRecordCollection(records), response -> {
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
    String errorMessage = String.format("Couldn't update parsed records collection - response status code %s, expected 200", statusCode);
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
