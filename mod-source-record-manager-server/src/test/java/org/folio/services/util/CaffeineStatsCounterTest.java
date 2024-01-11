package org.folio.services.util;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.micrometer.core.instrument.Tag;
import io.vertx.core.VertxOptions;
import org.folio.okapi.common.MetricsUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Order;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class CaffeineStatsCounterTest {

  private CaffeineStatsCounter statsCounter;
  private final String cacheName = "testCache";
  private final Iterable<Tag> tags = Collections.emptyList();

  @Before
  public void setUp() {
    // Setup MetricsUtil and mock counters
    System.setProperty("vertx.metrics.options.enabled", "true");
    System.setProperty("jmxMetricsOptions", "{\"domain\":\"org.folio\"}");
    if(!MetricsUtil.isEnabled()) MetricsUtil.init(new VertxOptions());

    statsCounter = new CaffeineStatsCounter(cacheName, tags);
  }

  @Test(expected = RuntimeException.class)
  @Order(1)
  public void constructor_MetricsNotEnabled_ShouldThrowException() {
    try (MockedStatic<MetricsUtil> mockedStatic = Mockito.mockStatic(MetricsUtil.class)) {
      mockedStatic.when(MetricsUtil::isEnabled).thenReturn(false);
      new CaffeineStatsCounter(cacheName, tags);
    }
  }

  @Test
  public void recordHits() {
    CacheStats beforeSnapshot = statsCounter.snapshot();
    statsCounter.recordHits(1);
    CacheStats afterSnapshot = statsCounter.snapshot();
    assertThat(afterSnapshot.minus(beforeSnapshot).hitCount()).isEqualTo(1);
  }

  @Test
  public void recordMisses() {
    CacheStats beforeSnapshot = statsCounter.snapshot();
    statsCounter.recordMisses(1);
    CacheStats afterSnapshot = statsCounter.snapshot();
    assertThat(afterSnapshot.minus(beforeSnapshot).missCount()).isEqualTo(1);
  }

  @Test
  public void recordLoadSuccess() {
    CacheStats beforeSnapshot = statsCounter.snapshot();
    statsCounter.recordLoadSuccess(10);
    CacheStats afterSnapshot = statsCounter.snapshot();
    assertThat(afterSnapshot.minus(beforeSnapshot).loadSuccessCount()).isEqualTo(1);
    assertThat(afterSnapshot.minus(beforeSnapshot).totalLoadTime()).isEqualTo(10);
  }

  @Test
  public void recordLoadFailure() {
    CacheStats beforeSnapshot = statsCounter.snapshot();
    statsCounter.recordLoadFailure(10);
    CacheStats afterSnapshot = statsCounter.snapshot();
    assertThat(afterSnapshot.minus(beforeSnapshot).loadFailureCount()).isEqualTo(1);
    assertThat(afterSnapshot.minus(beforeSnapshot).totalLoadTime()).isEqualTo(10);
  }

  @Test
  public void recordEviction() {
    CacheStats beforeSnapshot = statsCounter.snapshot();
    statsCounter.recordEviction(1, RemovalCause.EXPIRED);
    CacheStats afterSnapshot = statsCounter.snapshot();
    assertThat(afterSnapshot.minus(beforeSnapshot).evictionWeight()).isEqualTo(1);
    assertThat(afterSnapshot.minus(beforeSnapshot).evictionCount()).isEqualTo(1);
  }
}
