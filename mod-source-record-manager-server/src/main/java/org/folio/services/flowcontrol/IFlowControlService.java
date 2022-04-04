package org.folio.services.flowcontrol;

import org.folio.rest.jaxrs.model.Event;

public interface IFlowControlService {
  void trackEvent(Event event);
}
