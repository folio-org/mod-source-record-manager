package org.folio.services;

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
import org.folio.services.mappers.RecordToInstanceMapper;
import org.folio.services.mappers.RecordToInstanceMapperBuilder;
import org.folio.services.parsers.RecordFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class InstanceProcessingServiceImpl implements AfterProcessingService {

  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceProcessingServiceImpl.class);
  private static final String INVENTORY_URL = "/inventory/instances";
  private static final String LOCATION_HEADER = "location";

  @Autowired
  private JobExecutionSourceChunkDao jobExecutionSourceChunkDao;

  @Override
  public Future<RecordProcessingContext> process(RecordProcessingContext context, String sourceChunkId, OkapiConnectionParams params) {
    Future<RecordProcessingContext> future = Future.future();
    List<Future> createRecordsFuture = new ArrayList<>();
    List<Pair<RecordProcessingContext.RecordContext, Instance>> recordInstancesPairs = mapToInstance(context.getRecordsContext());
    recordInstancesPairs.forEach(recordInstancePair -> {
      Future<String> postInstanceFuture = postInstance(recordInstancePair.getValue(), params).compose(instanceId -> {
        RecordProcessingContext.RecordContext recordContext = recordInstancePair.getKey();
        recordContext.setInstanceId(instanceId);
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
          future.complete(context);
        }
      });
    return future;
  }

  /**
   * Sends POST request to Inventory
   *
   * @param instance Instance entity to POST
   * @param params   params enough to connect ot OKAPI
   * @return Instance id
   */
  private Future<String> postInstance(Instance instance, OkapiConnectionParams params) {
    Future<String> future = Future.future();
    RestUtil.doRequest(params, INVENTORY_URL, HttpMethod.POST, instance)
      .setHandler(responseResult -> {
        try {
          if (!RestUtil.validateAsyncResult(responseResult, future)) {
            LOGGER.error("Error creating new Instance record", future.cause());
          } else {
            String locationHeader = responseResult.result().getResponse().getHeader(LOCATION_HEADER);
            String id = locationHeader.substring(locationHeader.lastIndexOf("/"));
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
   * Creates Inventory Instance by record in accordance with default mapping rules
   *
   * @param recordsContext context object with records and properties
   * @return  list of key-valued objects where a key is Record, value is an Instance
   */
  private List<Pair<RecordProcessingContext.RecordContext, Instance>> mapToInstance(List<RecordProcessingContext.RecordContext> recordsContext) {
    if (CollectionUtils.isEmpty(recordsContext)) {
      return Collections.emptyList();
    }
    RecordToInstanceMapper mapper = RecordToInstanceMapperBuilder
      .buildMapper(RecordFormat.getByDataType(recordsContext.get(0).getRecord().getRecordType()));
    return recordsContext.stream()
      .map(recordContext -> {
        Instance instance = mapper.mapRecord(new JsonObject(recordContext.getRecord().getParsedRecord().getContent().toString()));
        return new ImmutablePair(recordContext, instance);
      })
      .collect(Collectors.toList());
  }
}
