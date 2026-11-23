package org.folio.verticle.consumers.errorhandlers;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class RedeliveryPolicyHeaderTest {

  private final RedeliveryPolicy policy = new RedeliveryPolicy();

  @Test
  public void shouldReturnZeroAttempts_whenHeaderAbsent() {
    // when
    int attemptsMade = policy.attemptsMade(new HashMap<>());

    // then
    assertThat(attemptsMade).isZero();
  }

  @Test
  public void shouldReturnZeroAttempts_whenHeaderMalformed() {
    // given
    Map<String, String> headers = Map.of(RedeliveryPolicy.RETRY_COUNT_HEADER, "abc");

    // when
    int attemptsMade = policy.attemptsMade(headers);

    // then
    assertThat(attemptsMade).isZero();
  }
}
