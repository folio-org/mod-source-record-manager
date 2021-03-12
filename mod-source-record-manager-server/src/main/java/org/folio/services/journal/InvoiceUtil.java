package org.folio.services.journal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.Record;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isAnyEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.dao.JobExecutionSourceChunkDaoImpl.LOGGER;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.EntityType.EDIFACT_INVOICE;
import static org.folio.rest.jaxrs.model.JournalRecord.*;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.INVOICE;
import static org.folio.services.journal.JournalUtil.ERROR_KEY;

public class InvoiceUtil {

  public static final String INVOICE_LINES_KEY = "INVOICE_LINES";
  public static final String INVOICE_LINES_ERRORS_KEY = "INVOICE_LINES_ERRORS";

  public static final String FIELD_DESCRIPTION = "description";
  public static final String FIELD_FOLIO_INVOICE_NO = "folioInvoiceNo";
  public static final String FIELD_ID = "id";
  public static final String FIELD_INVOICE_LINE_NUMBER = "invoiceLineNumber";
  public static final String FIELD_INVOICE_LINES = "invoiceLines";
  public static final String FIELD_INVOICE_NO = "invoiceNo";
  public static final String FIELD_SOURCE_ID = "sourceId";
  public static final String FIELD_VENDOR_INVOICE_NO = "vendorInvoiceNo";

  public static final String JOURNAL_RECORD = "journalRecord";
  private static final String EVENT_HAS_NO_DATA_MSG = "Failed to handle %s event, because event payload context does not contain %s and/or %s and/or %s data";

  public static LinkedList<JournalRecord> buildJournalRecordByEvent(DataImportEventPayload event)
    throws JournalRecordMapperException, JsonProcessingException {
    String edifactAsString = event.getContext().get(EDIFACT_INVOICE.value());
    String invoiceAsString = event.getContext().get(INVOICE.value());
    String invoiceLinesAsString = event.getContext().get(INVOICE_LINES_KEY);

    if (isAnyEmpty(edifactAsString, invoiceAsString, invoiceLinesAsString)) {
      event.getContext().keySet().forEach(key -> {
        if (isNotEmpty(event.getContext().get(key))) {
          LOGGER.error(key + ": " + event.getContext().get(key));
        }
      });
      throw new JournalRecordMapperException(String.format(EVENT_HAS_NO_DATA_MSG, event.getEventType(),
        EDIFACT_INVOICE.value(), INVOICE.value(), INVOICE_LINES_KEY));
    }

    LinkedList<JournalRecord> journalRecords = new LinkedList<>();
    Map<String, Object> journalInvoiceRecord = buildInvoiceRecord(event);
    journalRecords.add((JournalRecord) journalInvoiceRecord.get(JOURNAL_RECORD));
    journalRecords.addAll(buildInvoiceLineRecords(event, (String) journalInvoiceRecord.get(FIELD_INVOICE_NO),
      (String) journalInvoiceRecord.get(FIELD_SOURCE_ID), event.getContext().containsKey(ERROR_KEY)));

    return journalRecords;
  }

  private static Map<String, Object> buildInvoiceRecord(DataImportEventPayload eventPayload) throws JournalRecordMapperException {
    try {
      String edifactRecordAsString = eventPayload.getContext().get(EDIFACT_INVOICE.value());
      Record edifactRecord = new ObjectMapper().readValue(edifactRecordAsString, Record.class);

      String recordAsString = eventPayload.getContext().get(INVOICE.value());
      JsonObject invoiceJson = new JsonObject(recordAsString);

      JournalRecord journalRecord = new JournalRecord()
        .withJobExecutionId(eventPayload.getJobExecutionId())
        .withSourceId(edifactRecord.getId())
        .withSourceRecordOrder(0)
        .withEntityType(INVOICE)
        .withEntityId(invoiceJson.getString(FIELD_ID))
        .withTitle("Invoice")
        .withEntityHrId(invoiceJson.getString(FIELD_FOLIO_INVOICE_NO))
        .withActionType(ActionType.CREATE)
        .withActionDate(new Date())
        .withActionStatus(eventPayload.getEventType().equals(DI_ERROR.value()) ?
          ActionStatus.ERROR : ActionStatus.COMPLETED)
        .withError(eventPayload.getEventType().equals(DI_ERROR.value()) ?
          eventPayload.getContext().get(ERROR_KEY) : "");

      return Map.of(FIELD_INVOICE_NO, invoiceJson.getString(FIELD_VENDOR_INVOICE_NO),
        FIELD_SOURCE_ID, edifactRecord.getId(), JOURNAL_RECORD, journalRecord);
    } catch (Exception e) {
      throw new JournalRecordMapperException(JournalUtil.INSTANCE_OR_RECORD_MAPPING_EXCEPTION_MSG, e);
    }
  }

  private static LinkedList<JournalRecord> buildInvoiceLineRecords(DataImportEventPayload eventPayload,
                                                                   String invoiceNo, String sourceId, boolean isInvoiceIncorrect)
    throws JournalRecordMapperException, JsonProcessingException {

    HashMap<String, String> errorInvoiceLinesMap = initErrorInvoiceLinesMap(eventPayload);

    LinkedList<JournalRecord> invoiceLines = new LinkedList<>();
    try {
      String recordAsString = eventPayload.getContext().get(INVOICE_LINES_KEY);
      JsonObject jsonInvoiceLineCollection = new JsonObject(recordAsString);
      jsonInvoiceLineCollection.getJsonArray(FIELD_INVOICE_LINES).forEach(invoiceLine -> {

        String invoiceLineNumber = ((JsonObject) invoiceLine).getString(FIELD_INVOICE_LINE_NUMBER);
        String description = ((JsonObject) invoiceLine).getString(FIELD_DESCRIPTION);

        JournalRecord journalRecord = new JournalRecord()
          .withJobExecutionId(eventPayload.getJobExecutionId())
          .withSourceId(sourceId)
          .withEntityId(((JsonObject) invoiceLine).getString(FIELD_ID))
          .withEntityHrId(invoiceNo + "-" + invoiceLineNumber)
          .withSourceRecordOrder(Integer.valueOf(invoiceLineNumber))
          .withEntityType(INVOICE)
          .withTitle(description)
          .withActionType(ActionType.CREATE)
          .withActionDate(new Date())
          .withActionStatus(isInvoiceIncorrect || (errorInvoiceLinesMap.containsKey(invoiceLineNumber)) ?
            ActionStatus.ERROR : ActionStatus.COMPLETED)
          .withError(isInvoiceIncorrect ?
            eventPayload.getContext().get(ERROR_KEY) : errorInvoiceLinesMap.getOrDefault(invoiceLineNumber, ""));

        invoiceLines.add(journalRecord);
      });
      return invoiceLines;
    } catch (Exception e) {
      throw new JournalRecordMapperException(JournalUtil.INSTANCE_OR_RECORD_MAPPING_EXCEPTION_MSG, e);
    }
  }

  private static HashMap<String, String> initErrorInvoiceLinesMap(DataImportEventPayload eventPayload)
    throws JsonProcessingException {

    String errorInvoiceLines = eventPayload.getContext().get(INVOICE_LINES_ERRORS_KEY);
    if (isNotEmpty(errorInvoiceLines)) {
      return new ObjectMapper().readValue(errorInvoiceLines, HashMap.class);
    }
    return new HashMap<>();
  }
}
