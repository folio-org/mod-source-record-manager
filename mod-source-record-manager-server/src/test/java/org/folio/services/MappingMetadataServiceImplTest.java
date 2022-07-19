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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
      mappingParamsSnapshotDao);

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

    Mockito.when(mappingParamsSnapshotDao.getByJobExecutionId(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(mappingParameters)));
    Mockito.when(mappingParamsSnapshotDao.save(any(), eq(jobExecutionId), eq(tenantId)))
      .thenReturn(Future.succeededFuture(""));
    Mockito.when(mappingRulesSnapshotDao.getByJobExecutionId(eq(jobExecutionId), eq(tenantId)))
      .thenReturn(Future.succeededFuture(Optional.of(jsonObject)));
    Mockito.when(mappingRulesSnapshotDao.save(any(), eq(jobExecutionId), eq(tenantId)))
      .thenReturn(Future.succeededFuture(""));
    Mockito.when(mappingParametersProvider.get(any(), eq(params)))
      .thenReturn(Future.succeededFuture(mappingParameters));
    Mockito.when(mappingRuleService.get(eq(org.folio.Record.RecordType.MARC_BIB), eq(tenantId)))
      .thenReturn(Future.succeededFuture(Optional.of(jsonObject)));


    // Act
    service.getMappingMetadataDto(jobExecutionId, params)
      .compose(ar -> {
        context.assertNotNull(ar);
        context.assertEquals(ar.getJobExecutionId(), jobExecutionId);
        context.assertEquals(ar.getMappingRules(), "{}");
        context.assertNotNull(ar.getMappingParams());
        // verify dependencies are called
        Mockito.verify(mappingParamsSnapshotDao, Mockito.times(1)).getByJobExecutionId(eq(jobExecutionId), eq(tenantId));
        Mockito.verify(mappingRulesSnapshotDao, Mockito.times(1)).getByJobExecutionId(eq(jobExecutionId), eq(tenantId));
        Mockito.verify(mappingParametersProvider, Mockito.never()).get(any(), eq(params));
        Mockito.verify(mappingRuleService, Mockito.never()).get(eq(org.folio.Record.RecordType.MARC_BIB), eq(tenantId));
        return Future.succeededFuture();
      })
      .compose(ar -> service.saveMappingRulesSnapshot(jobExecutionId, Record.RecordType.MARC_BIB.toString(), tenantId))
      .compose(ar -> service.getMappingMetadataDto(jobExecutionId, params))
      .compose(ar -> {
        // another call to get metadata should not return from cache
        // since only rules are saved in cache but not parameters
        context.assertNotNull(ar);
        Mockito.verify(mappingParamsSnapshotDao, Mockito.times(2)).getByJobExecutionId(eq(jobExecutionId), eq(tenantId));
        Mockito.verify(mappingRulesSnapshotDao, Mockito.times(2)).getByJobExecutionId(eq(jobExecutionId), eq(tenantId));
        Mockito.verify(mappingParametersProvider, Mockito.never()).get(any(), any());
        Mockito.verify(mappingRuleService, Mockito.times(1)).get(eq(org.folio.Record.RecordType.MARC_BIB), eq(tenantId));
        return Future.succeededFuture();
      })
      .compose(ar -> service.saveMappingParametersSnapshot(jobExecutionId, params))
      .compose(ar -> service.getMappingMetadataDto(jobExecutionId, params)
      )
      .compose(ar -> {
        // another call to get metadata should return from the cache rather than going to DAO objects
        context.assertNotNull(ar);
        Mockito.verify(mappingParamsSnapshotDao, Mockito.times(2)).getByJobExecutionId(eq(jobExecutionId), eq(tenantId));
        Mockito.verify(mappingRulesSnapshotDao, Mockito.times(2)).getByJobExecutionId(eq(jobExecutionId), eq(tenantId));
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
