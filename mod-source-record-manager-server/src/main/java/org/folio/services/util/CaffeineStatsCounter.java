package org.folio.services.util;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import org.folio.okapi.common.MetricsUtil;

import static java.util.Objects.requireNonNull;

/**
 * StatsCounter that is a bridge to RMB's metrics collection system
 */
public class CaffeineStatsCounter implements StatsCounter {
  private final Counter hitCount;
  private final Counter missCount;
  private final Counter loadSuccessCount;
  private final Counter loadFailureCount;
  private final Counter totalLoadTime;
  private final Counter evictionCount;
  private final Counter evictionWeight;

  public CaffeineStatsCounter(String cacheName, Iterable<Tag> tags) {
    if (!MetricsUtil.isEnabled()) {
      throw new RuntimeException("Metrics is not enabled for RMB");
    }
    hitCount = MetricsUtil.recordCounter(cacheName+".hitCount", tags);
    missCount = MetricsUtil.recordCounter(cacheName+".missCount", tags);
    loadSuccessCount = MetricsUtil.recordCounter(cacheName+".loadSuccessCount", tags);
    loadFailureCount = MetricsUtil.recordCounter(cacheName+".loadFailureCount", tags);
    totalLoadTime = MetricsUtil.recordCounter(cacheName+".totalLoadTime", tags);
    evictionCount = MetricsUtil.recordCounter(cacheName+".evictionCount", tags);
    evictionWeight = MetricsUtil.recordCounter(cacheName+".evictionWeight", tags);
  }

  @Override
  public void recordHits(int count) {
    hitCount.increment(count);
  }

  @Override
  public void recordMisses(int count) {
    missCount.increment(count);
  }

  @Override
  public void recordLoadSuccess(long loadTime) {
    loadSuccessCount.increment();
    totalLoadTime.increment(loadTime);
  }

  @Override
  public void recordLoadFailure(long loadTime) {
    loadFailureCount.increment();
    totalLoadTime.increment(loadTime);
  }

  @Override
  public void recordEviction(int weight, RemovalCause cause) {
    requireNonNull(cause);
    evictionCount.increment();
    evictionWeight.increment(weight);
  }

  @Override
  public CacheStats snapshot() {
    return CacheStats.of(
      negativeToMaxValue((long) hitCount.count()),
      negativeToMaxValue((long) missCount.count()),
      negativeToMaxValue((long) loadSuccessCount.count()),
      negativeToMaxValue((long) loadFailureCount.count()),
      negativeToMaxValue((long) totalLoadTime.count()),
      negativeToMaxValue((long) evictionCount.count()),
      negativeToMaxValue((long) evictionWeight.count()));
  }

  /** Returns {@code value}, if non-negative. Otherwise, returns {@link Long#MAX_VALUE}. */
  private static long negativeToMaxValue(long value) {
    return (value >= 0) ? value : Long.MAX_VALUE;
  }

  /**
   * Increments all counters by the values in {@code other}.
   *
   * @param other the counter to increment from
   */
  public void incrementBy(StatsCounter other) {
    CacheStats otherStats = other.snapshot();
    hitCount.increment(otherStats.hitCount());
    missCount.increment(otherStats.missCount());
    loadSuccessCount.increment(otherStats.loadSuccessCount());
    loadFailureCount.increment(otherStats.loadFailureCount());
    totalLoadTime.increment(otherStats.totalLoadTime());
    evictionCount.increment(otherStats.evictionCount());
    evictionWeight.increment(otherStats.evictionWeight());
  }

  @Override
  public String toString() {
    return snapshot().toString();
  }
}
