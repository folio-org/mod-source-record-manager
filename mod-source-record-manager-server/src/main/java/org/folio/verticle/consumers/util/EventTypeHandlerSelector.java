package org.folio.verticle.consumers.util;

import org.folio.DataImportEventPayload;
import org.folio.DataImportEventTypes;

import java.util.List;
import java.util.Map;

import static org.folio.DataImportEventTypes.DI_ERROR;
import static org.folio.DataImportEventTypes.DI_INVOICE_CREATED;

public class EventTypeHandlerSelector {

  public static final String DEFAULT_HANDLER_KEY = "DEFAULT";
  private final Map<String, SpecificEventHandler> handlers;

  public EventTypeHandlerSelector() {
    this.handlers = Map.of(DI_INVOICE_CREATED.toString(), new InvoiceImportEventHandler(),
      DEFAULT_HANDLER_KEY, new MarcImportEventsHandler());
  }

  public SpecificEventHandler getHandler(DataImportEventPayload eventPayload) {
    DataImportEventTypes eventType = DataImportEventTypes.valueOf(eventPayload.getEventType());
    if (DI_INVOICE_CREATED == eventType ||
      (DI_ERROR == eventType && isLastOperationIs(DI_INVOICE_CREATED.value(), eventPayload.getEventsChain()))) {
      return handlers.get(DI_INVOICE_CREATED.value());
    }
    return handlers.get(DEFAULT_HANDLER_KEY);
  }

  private boolean isLastOperationIs(String eventType, List<String> operationsChain) {
    return operationsChain.stream().reduce((first, second) -> second)
      .map(s -> s.equals(eventType)).orElse(false);
  }

}
