package org.folio.verticle.consumers.errorhandlers.payloadbuilders;

import com.google.common.collect.Lists;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.processing.mapping.mapper.reader.record.edifact.EdifactReaderFactory;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;
import org.folio.processing.mapping.mapper.writer.common.JsonBasedWriter;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.JobExecutionService;
import org.folio.services.util.RecordConversionUtil;
import org.folio.verticle.consumers.errorhandlers.RawMarcChunksErrorHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;

import static org.folio.ActionProfile.FolioRecord.INVOICE;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVOICE_CREATED;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.Record.RecordType.EDIFACT;
import static org.folio.services.journal.InvoiceUtil.INVOICE_LINES_KEY;

@Component
public class EdifactDiErrorPayloadBuilder implements DiErrorPayloadBuilder {
  private static final String INVOICE_FIELD = "invoice";
  private static final String INVOICE_LINES_FIELD = "invoiceLines";

  private JobExecutionService jobExecutionService;

  @Autowired
  public EdifactDiErrorPayloadBuilder(JobExecutionService jobExecutionService) {
    this.jobExecutionService = jobExecutionService;

    MappingManager.registerReaderFactory(new EdifactReaderFactory());
    MappingManager.registerWriterFactory(new WriterFactory() {
      @Override
      public Writer createWriter() {
        return new JsonBasedWriter(EntityType.INVOICE);
      }

      @Override
      public boolean isEligibleForEntityType(EntityType entityType) {
        return EntityType.INVOICE == entityType;
      }
    });
  }

  @Override
  public boolean isEligible(Record.RecordType recordType) {
    return EDIFACT == recordType;
  }

  @Override
  public Future<DataImportEventPayload> buildEventPayload(Throwable throwable,
                                                  OkapiConnectionParams okapiParams,
                                                  String jobExecutionId,
                                                  String tenantId,
                                                  Record record) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, okapiParams.getTenantId())
      .compose(jobExecutionOptional -> {
        if (jobExecutionOptional.isPresent()) {
          DataImportEventPayload diErrorPayload = prepareEventPayloadForMapping(throwable, okapiParams, record, jobExecutionOptional.get());

          DataImportEventPayload payloadWithInvoiceDetails = mapPayloadWithPopulatingInvoiceDetails(diErrorPayload);
          return Future.succeededFuture(makeLightweightPayload(record, payloadWithInvoiceDetails));
        }
        DataImportEventPayload diErrorPayload = prepareDiErrorEventPayload(throwable, okapiParams, record, jobExecutionId);
        return Future.succeededFuture(makeLightweightPayload(record, diErrorPayload));
      });
  }

  private DataImportEventPayload prepareDiErrorEventPayload(Throwable throwable,
                                                            OkapiConnectionParams okapiParams,
                                                            Record record,
                                                            String jobExecutionId) {
    HashMap<String, String> context = new HashMap<>();
    context.put(RawMarcChunksErrorHandler.ERROR_KEY, throwable.getMessage());
    context.put(RecordConversionUtil.getEntityType(record).value(), Json.encode(record));

    return new DataImportEventPayload()
      .withEventType(DI_INVOICE_CREATED.value())
      .withJobExecutionId(jobExecutionId)
      .withOkapiUrl(okapiParams.getOkapiUrl())
      .withTenant(okapiParams.getTenantId())
      .withToken(okapiParams.getToken())
      .withEventsChain(Lists.newArrayList(DI_INVOICE_CREATED.value()))
      .withContext(context);
  }

  private DataImportEventPayload prepareEventPayloadForMapping(Throwable throwable,
                                                               OkapiConnectionParams okapiParams,
                                                               Record record,
                                                               JobExecution jobExecution) {
    ProfileSnapshotWrapper profileSnapshot = jobExecution.getJobProfileSnapshotWrapper();

    DataImportEventPayload eventPayload = prepareDiErrorEventPayload(throwable, okapiParams, record, jobExecution.getId());
    eventPayload.setProfileSnapshot(profileSnapshot);
    eventPayload.getEventsChain().add(eventPayload.getEventType());
    while (MAPPING_PROFILE != profileSnapshot.getContentType()) {
      profileSnapshot = profileSnapshot.getChildSnapshotWrappers().get(0);
    }
    eventPayload.setCurrentNode(profileSnapshot);
    eventPayload.getContext().put(INVOICE.value(), new JsonObject().encode());
    return eventPayload;
  }

  private DataImportEventPayload mapPayloadWithPopulatingInvoiceDetails(DataImportEventPayload dataImportEventPayload) {
    DataImportEventPayload mappedPayload = MappingManager.map(dataImportEventPayload, new MappingContext());
    mappedPayload.setEventType(DI_ERROR.value());

    JsonObject mappingResult = new JsonObject(mappedPayload.getContext().get(INVOICE.value()));
    JsonObject invoiceJson = mappingResult.getJsonObject(INVOICE_FIELD);
    JsonObject invoiceLineCollection = new JsonObject().put(INVOICE_LINES_FIELD, new JsonArray(invoiceJson.remove(INVOICE_LINES_FIELD).toString()));
    mappedPayload.getContext().put(INVOICE_LINES_KEY, Json.encode(invoiceLineCollection));
    mappedPayload.getContext().put(INVOICE.value(), invoiceJson.encode());

    return mappedPayload;
  }

  private DataImportEventPayload makeLightweightPayload(Record record, DataImportEventPayload payload) {
    record.setParsedRecord(null);
    record.setRawRecord(null);
    payload.setProfileSnapshot(null);
    payload.getContext().put(RecordConversionUtil.getEntityType(record).value(), Json.encode(record));
    return payload;
  }
}
