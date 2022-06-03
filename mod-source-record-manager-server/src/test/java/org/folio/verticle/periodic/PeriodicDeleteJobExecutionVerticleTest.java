package org.folio.verticle.periodic;

import com.google.common.collect.Lists;
import io.vertx.core.Future;
import org.folio.dao.JobExecutionDao;
import org.folio.services.TenantDataProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PeriodicDeleteJobExecutionVerticleTest {
  private static final String TENANT_ID = "diku";
  private static final int DIFF_NUMBER_OF_DAYS = 2;

  @Mock
  private TenantDataProvider tenantDataProvider;
  @Mock
  private JobExecutionDao jobExecutionDao;

  @InjectMocks
  private PeriodicDeleteJobExecutionVerticle verticle = new PeriodicDeleteJobExecutionVerticle();

  @Before
  public void setUp() {
    ReflectionTestUtils.setField(verticle,"diffNumberOfDays", DIFF_NUMBER_OF_DAYS);
  }

  @Test
  public void shouldDeleteJobExecutions() {
    when(tenantDataProvider.getModuleTenants()).thenReturn(Future.succeededFuture(Lists.newArrayList(TENANT_ID)));
    when(jobExecutionDao.hardDeleteJobExecutions(DIFF_NUMBER_OF_DAYS, TENANT_ID)).thenReturn(Future.succeededFuture(true));

    verticle.executePeriodicJob();

    verify(jobExecutionDao).hardDeleteJobExecutions(DIFF_NUMBER_OF_DAYS,TENANT_ID);
  }

  @Test
  public void shouldNotDeleteIfTenantsCallFinishesWithException() {
    when(tenantDataProvider.getModuleTenants()).thenReturn(Future.failedFuture(new RuntimeException("Something went wrong")));

    verticle.executePeriodicJob();

    verify(jobExecutionDao, never()).hardDeleteJobExecutions(eq(DIFF_NUMBER_OF_DAYS), anyString());
  }
}
