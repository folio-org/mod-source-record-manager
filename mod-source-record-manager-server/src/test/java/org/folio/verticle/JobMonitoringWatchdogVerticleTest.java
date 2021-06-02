package org.folio.verticle;

import static io.vertx.core.Future.succeededFuture;
import static java.util.Collections.emptyList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;
import io.vertx.core.shareddata.LocalMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.internal.util.reflection.FieldSetter;
import org.mockito.junit.MockitoJUnitRunner;

import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobMonitoring;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.JobProfileInfo.DataType;
import org.folio.rest.jaxrs.model.RunBy;
import org.folio.services.JobExecutionService;
import org.folio.services.JobMonitoringService;

@RunWith(MockitoJUnitRunner.Silent.class)
public class JobMonitoringWatchdogVerticleTest {

  private static final String TENANT_ID1 = "testing1";
  private static final String TENANT_ID2 = "testing2";
  private static final String UUID = "5105b55a-b9a3-4f76-9402-a5243ea63c95";

  private final JobProfileInfo jobProfileInfo = new JobProfileInfo()
    .withId(UUID)
    .withDataType(DataType.MARC_BIB)
    .withName("Marc jobs profile");

  private final JobExecution jobExecution = new JobExecution()
    .withId(UUID)
    .withFileName("Job execution")
    .withRunBy(new RunBy().withFirstName("DIKU").withLastName("ADMINISTRATOR")).withJobProfileInfo(jobProfileInfo)
    .withJobProfileInfo(jobProfileInfo);

  private final JobMonitoring givenJobMonitoring = new JobMonitoring()
    .withJobExecutionId(UUID)
    .withNotificationSent(true)
    .withLastEventTimestamp(new Date());

  @Spy
  private final Vertx vertx = Vertx.vertx();

  @Spy
  @InjectMocks
  private final JobMonitoringWatchdogVerticle jobMonitoringWatchdogVerticle = new JobMonitoringWatchdogVerticle();

  @Mock
  private JobMonitoringService jobMonitoringService;

  @Mock
  private JobExecutionService jobExecutionService;

  @Before
  public void setUp() throws NoSuchFieldException {
    MockitoAnnotations.initMocks(this);
    LocalMap<String, String> tenants = vertx.sharedData().getLocalMap("tenants");
    tenants.put(TENANT_ID1, TENANT_ID1);
    tenants.put(TENANT_ID2, TENANT_ID2);

    FieldSetter.setField(jobMonitoringWatchdogVerticle,
      jobMonitoringWatchdogVerticle.getClass().getSuperclass().getDeclaredField("maxInactiveInterval"), 200L);
    doNothing().when(jobMonitoringWatchdogVerticle).declareSpringContext();
  }

  @Test
  public void shouldPrintWarnLogWhenJobIsStacked() throws InterruptedException {
    // given
    when(jobMonitoringService.getInactiveJobMonitors(anyLong(), anyString()))
      .thenReturn(succeededFuture(List.of(givenJobMonitoring)));
    when(jobExecutionService.getJobExecutionById(eq(UUID), anyString()))
      .thenReturn(succeededFuture(Optional.of(jobExecution)));
    when(jobMonitoringService.updateByJobExecutionId(eq(UUID), any(Date.class), eq(true), anyString()))
      .thenReturn(succeededFuture(true));

    // when
    jobMonitoringWatchdogVerticle.start();

    Thread.sleep(2000);

    // then
    verify(jobMonitoringService, atLeastOnce()).getInactiveJobMonitors(anyLong(), eq(TENANT_ID1));
    verify(jobMonitoringService, atLeastOnce()).getInactiveJobMonitors(anyLong(), eq(TENANT_ID2));
    verify(jobExecutionService, atLeastOnce()).getJobExecutionById(UUID, TENANT_ID1);
    verify(jobExecutionService, atLeastOnce()).getJobExecutionById(UUID, TENANT_ID2);
    verify(jobMonitoringService, atLeastOnce()).updateByJobExecutionId(eq(UUID), any(), eq(true), eq(TENANT_ID1));
    verify(jobMonitoringService, atLeastOnce()).updateByJobExecutionId(eq(UUID), any(), eq(true), eq(TENANT_ID2));
  }

  @Test
  public void shouldNotPrintWarnLogWhenJobIsNotStacked() {
    // given
    when(jobMonitoringService.getInactiveJobMonitors(anyLong(), anyString()))
      .thenReturn(succeededFuture(emptyList()));

    // when
    jobMonitoringWatchdogVerticle.start();

    // then
    verify(jobExecutionService, never()).getJobExecutionById(anyString(), anyString());
    verify(jobMonitoringService, never()).updateByJobExecutionId(anyString(), any(Date.class), anyBoolean(), anyString());
  }

}
