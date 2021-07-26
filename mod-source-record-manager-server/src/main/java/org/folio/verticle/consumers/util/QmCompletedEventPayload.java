package org.folio.verticle.consumers.util;

import lombok.Value;

@Value
public class QmCompletedEventPayload {

  String recordId;
  String errorMessage;
}
