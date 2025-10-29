package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.dao.MappingParamsSnapshotDao;
import org.folio.dao.MappingRulesSnapshotDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.mappers.processor.MappingParametersProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Optional;
import java.util.UUID;

import static org.folio.rest.impl.AbstractRestTest.CACHE_EXPIRATION_TIME;
import static org.folio.rest.impl.AbstractRestTest.CACHE_MAX_SIZE;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;

@RunWith(VertxUnitRunner.class)
public class MappingMetadataServiceImplTest {

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();
  @Mock
  private MappingParametersProvider mappingParametersProvider;
  @Mock
  private MappingRuleService mappingRuleService;
  @Mock
  private MappingRulesSnapshotDao mappingRulesSnapshotDao;
  @Mock
  private MappingParamsSnapshotDao mappingParamsSnapshotDao;
  private MappingMetadataServiceImpl service;

  @Before
  public void before() {
    MockitoAnnotations.openMocks(this);
    service = new MappingMetadataServiceImpl(
      mappingParametersProvider,
      mappingRuleService,
      mappingRulesSnapshotDao,
      mappingParamsSnapshotDao,
      CACHE_EXPIRATION_TIME,
      CACHE_MAX_SIZE);
  }

  @Test
  public void getMappingMetadataDtoAndCheckCache(TestContext context) {
    Async async = context.async();
    // Arrange
    JsonObject jsonObject = new JsonObject();
    MappingParameters mappingParameters = new MappingParameters();
    String jobExecutionId = UUID.randomUUID().toString();
    String tenantId = UUID.randomUUID().toString();
    OkapiConnectionParams params = mock(OkapiConnectionParams.class);
    Mockito.when(params.getTenantId()).thenReturn(tenantId);

    // Mock DAO calls that happen on the first, cache-miss call
    Mockito.when(mappingParamsSnapshotDao.getByJobExecutionId(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(mappingParameters)));
    Mockito.when(mappingRulesSnapshotDao.getByJobExecutionId(eq(jobExecutionId), eq(tenantId)))
      .thenReturn(Future.succeededFuture(Optional.of(jsonObject)));

    // Mock DAO calls for SAVE operations
    Mockito.when(mappingParamsSnapshotDao.save(any(MappingParameters.class), anyString(), anyString()))
      .thenReturn(Future.succeededFuture(UUID.randomUUID().toString()));
    Mockito.when(mappingRulesSnapshotDao.save(any(JsonObject.class), anyString(), anyString()))
      .thenReturn(Future.succeededFuture(UUID.randomUUID().toString()));

    // Mock provider/service calls for save operations
    Mockito.when(mappingParametersProvider.get(any(), eq(params)))
      .thenReturn(Future.succeededFuture(mappingParameters));
    Mockito.when(mappingRuleService.get(eq(org.folio.Record.RecordType.MARC_BIB), eq(tenantId)))
      .thenReturn(Future.succeededFuture(Optional.of(jsonObject)));

    // Act
    service.getMappingMetadataDto(jobExecutionId, params)
      .compose(ar -> {
        context.assertNotNull(ar);
        Mockito.verify(mappingParamsSnapshotDao, Mockito.times(1)).getByJobExecutionId(eq(jobExecutionId), eq(tenantId));
        Mockito.verify(mappingRulesSnapshotDao, Mockito.times(1)).getByJobExecutionId(eq(jobExecutionId), eq(tenantId));
        return Future.succeededFuture();
      })
      .compose(ar -> service.saveMappingRulesSnapshot(jobExecutionId, Record.RecordType.MARC_BIB.toString(), tenantId))
      .compose(ar -> service.getMappingMetadataDto(jobExecutionId, params))
      .compose(ar -> {
        context.assertNotNull(ar);
        Mockito.verify(mappingParamsSnapshotDao, Mockito.times(1)).getByJobExecutionId(eq(jobExecutionId), eq(tenantId));
        Mockito.verify(mappingRulesSnapshotDao, Mockito.times(1)).getByJobExecutionId(eq(jobExecutionId), eq(tenantId));
        return Future.succeededFuture();
      })
      .compose(ar -> service.saveMappingParametersSnapshot(jobExecutionId, params))
      .compose(ar -> service.getMappingMetadataDto(jobExecutionId, params))
      .compose(ar -> {
        context.assertNotNull(ar);
        Mockito.verify(mappingParamsSnapshotDao, Mockito.times(1)).getByJobExecutionId(eq(jobExecutionId), eq(tenantId));
        Mockito.verify(mappingRulesSnapshotDao, Mockito.times(1)).getByJobExecutionId(eq(jobExecutionId), eq(tenantId));

        Mockito.verify(mappingParametersProvider, Mockito.times(1)).get(any(), eq(params));
        Mockito.verify(mappingRuleService, Mockito.times(1)).get(eq(org.folio.Record.RecordType.MARC_BIB), eq(tenantId));
        return Future.succeededFuture();
      })
      .onSuccess(ar -> async.complete())
      .onFailure(context::fail);
  }

  @Test
  public void mappingRulesNotFound(TestContext context) {
    Async async = context.async();
    String jobExecutionId = UUID.randomUUID().toString();
    String tenantId = UUID.randomUUID().toString();
    Mockito.when(mappingRuleService.get(eq(org.folio.Record.RecordType.MARC_BIB), eq(tenantId)))
      .thenReturn(Future.succeededFuture(Optional.empty()));

    service.saveMappingRulesSnapshot(jobExecutionId, Record.RecordType.MARC_BIB.toString(), tenantId)
      .onComplete(ar -> {
        if (ar.succeeded()) {
          context.fail();
        } else if (ar.failed()) {
          context.assertEquals(ar.cause().getMessage(),
            String.format("Mapping rules are not found for tenant id '%s'", tenantId));
          async.complete();
        }
      });
  }
}
