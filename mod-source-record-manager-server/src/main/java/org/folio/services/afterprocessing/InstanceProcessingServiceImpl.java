package org.folio.services.afterprocessing;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.HttpStatus;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.rest.client.SourceStorageClient;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.mappers.RecordToInstanceMapper;
import org.folio.services.mappers.RecordToInstanceMapperBuilder;
import org.folio.services.parsers.RecordFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.List;

@Service
public class InstanceProcessingServiceImpl implements AfterProcessingService {

  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceProcessingServiceImpl.class);
  private static final String INVENTORY_URL = "/inventory/instances";
  private static final String INSTANCE_LOCATION_RESPONSE_HEADER = "location";

  private JobExecutionSourceChunkDao jobExecutionSourceChunkDao;
  private AdditionalFieldsUtil additionalInstanceFieldsUtil;

  public InstanceProcessingServiceImpl(@Autowired JobExecutionSourceChunkDao jobExecutionSourceChunkDao,
                                       @Autowired AdditionalFieldsUtil additionalInstanceFieldsUtil) {
    this.jobExecutionSourceChunkDao = jobExecutionSourceChunkDao;
    this.additionalInstanceFieldsUtil = additionalInstanceFieldsUtil;
  }

  @Override
  public Future<Void> process(List<Record> records, String sourceChunkId, OkapiConnectionParams params) {
    if (CollectionUtils.isEmpty(records)) {
      return Future.succeededFuture();
    }
    Future<Void> future = Future.future();
    List<Future> processRecordFutures = new ArrayList<>();
    RecordToInstanceMapper mapper = RecordToInstanceMapperBuilder.buildMapper(RecordFormat.getByDataType(getRecordsType(records)));
    SourceStorageClient sourceStorageClient = new SourceStorageClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    records
      .parallelStream()
      .forEach(record -> processRecordFutures.add(processRecordForInstance(record, mapper, sourceStorageClient, params)));
    CompositeFuture.all(processRecordFutures).setHandler(result -> {
      if (result.failed()) {
        LOGGER.error("Couldn't create Instance in mod-inventory", result.cause());
        jobExecutionSourceChunkDao.getById(sourceChunkId, params.getTenantId())
          .compose(optional -> optional
            .map(sourceChunk -> jobExecutionSourceChunkDao.update(sourceChunk.withState(JobExecutionSourceChunk.State.ERROR), params.getTenantId()))
            .orElseThrow(() -> new NotFoundException(String.format(
              "Couldn't update failed jobExecutionSourceChunk status to ERROR, jobExecutionSourceChunk with id %s was not found", sourceChunkId))));
      }
      // Immediately complete future in order to do not wait for processing of async result
      future.complete();
    });
    return future;
  }

  /**
   * Performs mapping a given Record to Instance and sends result Instance to mod-inventory.
   * If given Record is MARC record, adds additional fields into ParsedRecord.
   * If given Record is ERROR record - just successfully complete.
   *
   * @param record              Record
   * @param mapper              Instance mapper
   * @param sourceStorageClient http client for client storage
   * @param params              OKAPI connection paras
   * @return future
   */
  private Future<Void> processRecordForInstance(Record record, RecordToInstanceMapper mapper, SourceStorageClient sourceStorageClient, OkapiConnectionParams params) {
    // If given Record is not ERROR Record - try to map Record to Instance
    if (record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
      try {
        Instance instance = mapper.mapRecord(new JsonObject(record.getParsedRecord().getContent().toString()));
        return postInstance(instance, params).compose(instanceId -> {
          if (Record.RecordType.MARC.equals(record.getRecordType())) {
            boolean addedInstanceId = additionalInstanceFieldsUtil.addInstanceIdToMarcRecord(record, instanceId);
            if (addedInstanceId) {
              return updateRecord(record, sourceStorageClient);
            }
          }
          return Future.succeededFuture();
        });
      } catch (Exception exception) {
        String errorMessage =
          String.format("Can not process given Record for Instance. Cause: '%s'. Exception: '%s'", exception.getMessage(), exception);
        LOGGER.error(errorMessage);
        return Future.failedFuture(errorMessage);
      }
    }
    // IF given Record is ERROR record - return succeeded future
    return Future.succeededFuture();
  }

  /**
   * Creates Inventory Instance
   *
   * @param instance Instance entity serves as request body to POST request to Inventory
   * @param params   params enough to connect ot OKAPI
   * @return future with Instance id
   */
  private Future<String> postInstance(Instance instance, OkapiConnectionParams params) {
    Future<String> future = Future.future();
    params.getHeaders().add("Transfer-Encoding", "chunked");
    RestUtil.doRequest(params, INVENTORY_URL, HttpMethod.POST, instance).setHandler(responseResult -> {
      try {
        if (!RestUtil.validateAsyncResult(responseResult, future)) {
          LOGGER.error("Error creating new Instance record", future.cause());
        } else {
          if (StringUtils.isNotEmpty(instance.getId())) {
            String instanceId = instance.getId();
            future.complete(instanceId);
          } else {
            String location = responseResult.result().getResponse().getHeader(INSTANCE_LOCATION_RESPONSE_HEADER);
            String instanceId = location.substring(location.lastIndexOf('/') + 1);
            future.complete(instanceId);
          }
        }
      } catch (Exception e) {
        LOGGER.error("Error during post for new Instance", e);
        future.fail(e);
      }
    });
    return future;
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
   * Updates given record
   *
   * @param record given record
   * @param client http client
   * @return void
   */
  protected Future<Void> updateRecord(Record record, SourceStorageClient client) {
    Future<Void> future = Future.future();
    try {
      client.putSourceStorageRecordsById(record.getId(), null, record, response -> {
        if (response.statusCode() != HttpStatus.HTTP_OK.toInt()) {
          String errorMessage = "Error updating Record by id " + record.getId();
          LOGGER.error(errorMessage);
          future.fail(errorMessage);
        } else {
          future.complete();
        }
      });
    } catch (Exception e) {
      LOGGER.error("Couldn't send request to update Record with id {}", record.getId(), e);
      future.fail(e);
    }
    return future;
  }
}
