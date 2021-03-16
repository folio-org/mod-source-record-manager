package org.folio.verticle;

import org.folio.kafka.AsyncRecordHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.List;

import static org.folio.DataImportEventTypes.DI_INVOICE_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_COMPLETED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDING_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDING_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDING_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_ITEM_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_ITEM_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_ITEM_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_UPDATED;

public class DataImportJournalConsumersVerticle extends AbstractConsumersVerticle {

  @Autowired
  @Qualifier("DataImportJournalKafkaHandler")
  private AsyncRecordHandler<String, String> dataImportJournalKafkaHandler;

  @Override
  public List<String> getEvents() {
    return List.of(
      DI_SRS_MARC_BIB_RECORD_UPDATED.value(),
      DI_SRS_MARC_BIB_RECORD_MODIFIED.value(),
      DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.value(),
      DI_INVENTORY_INSTANCE_CREATED.value(),
      DI_INVENTORY_INSTANCE_UPDATED.value(),
      DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value(),
      DI_INVENTORY_INSTANCE_NOT_MATCHED.value(),
      DI_INVENTORY_HOLDING_CREATED.value(),
      DI_INVENTORY_HOLDING_UPDATED.value(),
      DI_INVENTORY_HOLDING_NOT_MATCHED.value(),
      DI_INVENTORY_ITEM_CREATED.value(),
      DI_INVENTORY_ITEM_UPDATED.value(),
      DI_INVENTORY_ITEM_NOT_MATCHED.value(),
      DI_INVOICE_CREATED.value(),
      DI_COMPLETED.value(),
      DI_ERROR.value()
    );
  }

  @Override
  public AsyncRecordHandler<String, String> getHandler() {
    return this.dataImportJournalKafkaHandler;
  }
}
