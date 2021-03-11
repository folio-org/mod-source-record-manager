package org.folio.verticle.consumers.util;

import org.folio.DataImportEventPayload;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.util.List;

import static org.folio.DataImportEventTypes.DI_COMPLETED;
import static org.folio.DataImportEventTypes.DI_ERROR;
import static org.folio.DataImportEventTypes.DI_INVOICE_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;

@RunWith(BlockJUnit4ClassRunner.class)
public class EventTypeHandlerSelectorTest {

  EventTypeHandlerSelector eventTypeHandlerSelector = new EventTypeHandlerSelector();

  @Test
  public void shouldReturnMarcImportEventHandler() {

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value());

    Assert.assertTrue(eventTypeHandlerSelector.getHandler(eventPayload) instanceof MarcImportEventsHandler);
  }

  @Test
  public void shouldReturnMarcImportEventHandlerForError() {

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withEventsChain(List.of(DI_INVENTORY_INSTANCE_CREATED.value()));

    Assert.assertTrue(eventTypeHandlerSelector.getHandler(eventPayload) instanceof MarcImportEventsHandler);
  }

  @Test
  public void shouldReturnMarcImportEventHandlerForCompleted() {

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_COMPLETED.value())
      .withEventsChain(List.of(DI_INVENTORY_INSTANCE_UPDATED.value()));

    Assert.assertTrue(eventTypeHandlerSelector.getHandler(eventPayload) instanceof MarcImportEventsHandler);
  }

  @Test
  public void shouldReturnInvoiceImportEventHandler() {

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INVOICE_CREATED.value());

    Assert.assertTrue(eventTypeHandlerSelector.getHandler(eventPayload) instanceof InvoiceImportEventHandler);
  }

  @Test
  public void shouldReturnInvoiceImportEventHandlerForInvoiceError() {

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withEventsChain(List.of(DI_INVOICE_CREATED.value()));

    Assert.assertTrue(eventTypeHandlerSelector.getHandler(eventPayload) instanceof InvoiceImportEventHandler);
  }

}
