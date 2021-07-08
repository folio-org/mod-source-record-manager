package org.folio.verticle.consumers.util;

import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_COMPLETED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVOICE_CREATED;

@Component
public class EventTypeHandlerSelector {

  public static final String DEFAULT_HANDLER_KEY = "DEFAULT";
  private final Map<String, SpecificEventHandler> handlers;

  @Autowired
  public EventTypeHandlerSelector(MarcImportEventsHandler marcImportEventsHandler) {
    this.handlers = Map.of(DI_INVOICE_CREATED.toString(), new InvoiceImportEventHandler(),
      DEFAULT_HANDLER_KEY, marcImportEventsHandler);
  }

  public SpecificEventHandler getHandler(DataImportEventPayload eventPayload) {
    DataImportEventTypes eventType = DataImportEventTypes.valueOf(eventPayload.getEventType());
    if (DI_INVOICE_CREATED == eventType
      || (DI_ERROR == eventType && isLastOperationIs(DI_INVOICE_CREATED.value(), eventPayload.getEventsChain()))
      || (DI_COMPLETED == eventType && isLastOperationIs(DI_INVOICE_CREATED.value(), eventPayload.getEventsChain()))) {
      return handlers.get(DI_INVOICE_CREATED.value());
    }
    return handlers.get(DEFAULT_HANDLER_KEY);
  }

  private boolean isLastOperationIs(String eventType, List<String> operationsChain) {
    return operationsChain.stream().reduce((first, second) -> second)
      .map(s -> s.equals(eventType)).orElse(false);
  }

}
