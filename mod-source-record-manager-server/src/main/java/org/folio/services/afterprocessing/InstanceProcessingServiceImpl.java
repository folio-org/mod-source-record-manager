package org.folio.services.afterprocessing;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
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
      Future.succeededFuture();
    }
    Future<Void> future = Future.future();
    List<Future> createRecordsFuture = new ArrayList<>();
    List<Pair<Record, Instance>> recordToInstancePairs = mapToInstance(records);
    RecordProcessingContext processingContext = new RecordProcessingContext(getRecordsType(records));
    recordToInstancePairs.forEach(recordToInstancePair -> {
      Record record = recordToInstancePair.getLeft();
      Instance instance = recordToInstancePair.getRight();
      Future<Void> postInstanceFuture = postInstance(instance, params).compose(instanceId -> {
        processingContext.addRecordContext(record.getId(), instanceId);
        return Future.succeededFuture();
      });
      createRecordsFuture.add(postInstanceFuture);
    });
    CompositeFuture.all(createRecordsFuture)
      .setHandler(result -> {
        if (result.failed()) {
          LOGGER.error("Couldn't create Instance in mod-inventory", result.cause());
          jobExecutionSourceChunkDao.getById(sourceChunkId, params.getTenantId())
            .compose(optional -> optional
              .map(sourceChunk -> jobExecutionSourceChunkDao.update(sourceChunk.withState(JobExecutionSourceChunk.State.ERROR), params.getTenantId()))
              .orElseThrow(() -> new NotFoundException(String.format(
                "Couldn't update failed jobExecutionSourceChunk status to ERROR, jobExecutionSourceChunk with id %s was not found", sourceChunkId))));
          future.fail(result.cause());
        } else {
          additionalInstanceFieldsUtil.addAdditionalFields(processingContext, params);
          future.complete();
        }
      });
    return future;
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
    RestUtil.doRequest(params, INVENTORY_URL, HttpMethod.POST, instance)
      .setHandler(responseResult -> {
        try {
          if (!RestUtil.validateAsyncResult(responseResult, future)) {
            LOGGER.error("Error creating new Instance record", future.cause());
          } else {
            String location = responseResult.result().getResponse().getHeader(INSTANCE_LOCATION_RESPONSE_HEADER);
            String id = location.substring(location.lastIndexOf('/') + 1);
            future.complete(id);
          }
        } catch (Exception e) {
          LOGGER.error("Error during post for new Instance", e);
          future.fail(e);
        }
      });
    return future;
  }

  /**
   * Creates Inventory Instance by Record in accordance with default mapping rules
   *
   * @param records records to map instances
   * @return list of key-valued objects where key is a Record, value is an Instance
   */
  private List<Pair<Record, Instance>> mapToInstance(List<Record> records) {
    RecordToInstanceMapper mapper = RecordToInstanceMapperBuilder.buildMapper(RecordFormat.getByDataType(getRecordsType(records)));
    List<Pair<Record, Instance>> pairs = new ArrayList<>();
    for (Record record : records) {
      Instance instance = mapper.mapRecord(new JsonObject(record.getParsedRecord().getContent().toString()));
      Pair<Record, Instance> recordInstancePair = new ImmutablePair<>(record, instance);
      pairs.add(recordInstancePair);
    }
    return pairs;
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
}
