package org.folio.services.mappers.processor.publisher;

import java.util.Arrays;

public enum PublisherRole {

  PRODUCTION(0, "Production"),
  PUBLICATION(1, "Publication"),
  DISTRIBUTION(2, "Distribution"),
  MANUFACTURE(3, "Manufacture");

  private final int indicator;
  private final String caption;

  PublisherRole(int indicator, String caption) {
    this.indicator = indicator;
    this.caption = caption;
  }

  public String getCaption() {
    return caption;
  }

  public static PublisherRole getByIndicator(int indicator) {
    return Arrays.stream(values())
      .filter(publisherRole -> publisherRole.indicator == indicator)
      .findFirst()
      .orElse(null);
  }
}
