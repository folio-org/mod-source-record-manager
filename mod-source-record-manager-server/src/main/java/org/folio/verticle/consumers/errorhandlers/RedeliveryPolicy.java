package org.folio.verticle.consumers.errorhandlers;

import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Retry policy for redelivered events: bounded number of attempts with exponential backoff.
 * The number of attempts made so far travels with the event in a kafka header.
 */
@Component
public class RedeliveryPolicy {

  public static final String RETRY_COUNT_HEADER = "diRetryCount";
  static final int MAX_RETRIES = 15;
  static final long BASE_DELAY_MS = 1000;
  static final long MAX_DELAY_MS = 60_000;
  private static final int MAX_SHIFT = 20;

  /**
   * @return how many times the event has already been redelivered; 0 if the header is absent or malformed
   */
  public int attemptsMade(Map<String, String> headers) {
    try {
      var value = headers.get(RETRY_COUNT_HEADER);
      return value == null ? 0 : Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  public boolean isExhausted(int attemptsMade) {
    return attemptsMade >= MAX_RETRIES;
  }

  /**
   * Exponential backoff: 1s, 2s, 4s ... capped at 60s.
   */
  public long delayMs(int attemptsMade) {
    return Math.min(MAX_DELAY_MS, BASE_DELAY_MS << Math.min(attemptsMade, MAX_SHIFT));
  }

  public void recordAttempt(Map<String, String> headers, int attemptsMade) {
    headers.put(RETRY_COUNT_HEADER, String.valueOf(attemptsMade + 1));
  }
}
