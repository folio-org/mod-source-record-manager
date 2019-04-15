package org.folio.services;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class InstanceProcessingServiceImpl implements AfterProcessingService {

  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceProcessingServiceImpl.class);
  private static final String INVENTORY_URL = "/inventory/instances";

  @Autowired
  private JobExecutionSourceChunkDao jobExecutionSourceChunkDao;

  @Override
  public Future<List<Record>> process(List<Record> records, String sourceChunkId, OkapiConnectionParams params) {
    Future<List<Record>> future = Future.future();
    List<Future> createRecordsFuture = new ArrayList<>();
    List<Instance> instances = mapToInstance(records);
    instances.forEach(instance -> createRecordsFuture.add(postInstance(instance, params)));
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
          future.complete(records);
        }
      });
    return future;
  }

  private Future<String> postInstance(Instance instance, OkapiConnectionParams params) {
    Future<String> future = Future.future();
    RestUtil.doRequest(params, INVENTORY_URL, HttpMethod.POST, instance)
      .setHandler(responseResult -> {
        try {
          if (!RestUtil.validateAsyncResult(responseResult, future)) {
            LOGGER.error("Error creating new Instance record", future.cause());
          } else {
            String responseBody = responseResult.result().getBody();
            future.complete(responseBody);
          }
        } catch (Exception e) {
          LOGGER.error("Error during post for new Instance", e);
          future.fail(e);
        }
      });
    return future;
  }

  private List<Instance> mapToInstance(List<Record> records) {
    if (records == null || records.isEmpty()) {
      return Collections.emptyList();
    }
    RecordToInstanceMapper mapper = RecordToInstanceMapperBuilder.buildMapper(RecordFormat.getByDataType(records.get(0).getRecordType()));
    return records.stream()
      .map(record -> mapper.mapRecord(new JsonObject(record.getParsedRecord().getContent().toString()))).collect(Collectors.toList());
  }
}
