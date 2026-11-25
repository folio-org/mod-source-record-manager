package org.folio.verticle.consumers.errorhandlers;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class RedeliveryPolicyTest {

  @Parameters(name = "attemptsMade={0} -> delay={1}ms, exhausted={2}")
  public static Collection<Object[]> attempts() {
    return Arrays.asList(new Object[][] {
      {0, 1000L, false}, {1, 2000L, false}, {2, 4000L, false}, {5, 32000L, false},
      {6, 60000L, false}, {14, 60000L, false}, {15, 60000L, true}, {100, 60000L, true}
    });
  }

  private final int attemptsMade;
  private final long expectedDelayMs;
  private final boolean expectedExhausted;
  private final RedeliveryPolicy policy = new RedeliveryPolicy();

  public RedeliveryPolicyTest(int attemptsMade, long expectedDelayMs, boolean expectedExhausted) {
    this.attemptsMade = attemptsMade;
    this.expectedDelayMs = expectedDelayMs;
    this.expectedExhausted = expectedExhausted;
  }

  @Test
  public void shouldDoubleDelayUpToCap_whenAttemptsMadeGrows() {
    // when
    long delay = policy.delayMs(attemptsMade);

    // then
    assertThat(delay).isEqualTo(expectedDelayMs);
  }

  @Test
  public void shouldBeExhausted_whenMaxRetriesReached() {
    // when
    boolean exhausted = policy.isExhausted(attemptsMade);

    // then
    assertThat(exhausted).isEqualTo(expectedExhausted);
  }

  @Test
  public void shouldReadAttemptsMadeFromHeader_whenHeaderPresent() {
    // given
    Map<String, String> headers = Map.of(RedeliveryPolicy.RETRY_COUNT_HEADER, String.valueOf(attemptsMade));

    // when
    int result = policy.attemptsMade(headers);

    // then
    assertThat(result).isEqualTo(attemptsMade);
  }

  @Test
  public void shouldWriteNextAttemptIntoHeaders_whenAttemptRecorded() {
    // given
    Map<String, String> headers = new HashMap<>();

    // when
    policy.recordAttempt(headers, attemptsMade);

    // then
    assertThat(headers).containsEntry(RedeliveryPolicy.RETRY_COUNT_HEADER, String.valueOf(attemptsMade + 1));
  }
}
