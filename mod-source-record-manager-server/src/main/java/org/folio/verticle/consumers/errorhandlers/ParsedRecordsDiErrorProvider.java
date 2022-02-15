package org.folio.verticle.consumers.errorhandlers;

import com.google.common.collect.Lists;
import io.vertx.core.Future;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.folio.services.ChangeEngineServiceImpl;
import org.folio.services.JobExecutionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class ParsedRecordsDiErrorProvider {

  private JobExecutionService jobExecutionService;
  private ChangeEngineServiceImpl changeEngineService;

  @Autowired
  public ParsedRecordsDiErrorProvider(JobExecutionService jobExecutionService, ChangeEngineServiceImpl changeEngineService) {
    this.jobExecutionService = jobExecutionService;
    this.changeEngineService = changeEngineService;
  }

  /**
   * Get parsed records from initial records in the particular chunk.
   *
   * @param okapiParams the okapi params
   * @param jobExecutionId the job execution id
   * @param rawRecordsDto the raw records dto
   * @return list of parsed records
   */
  public Future<List<Record>> getParsedRecordsFromInitialRecords(OkapiConnectionParams okapiParams,
                                                         String jobExecutionId,
                                                         RawRecordsDto rawRecordsDto) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, okapiParams.getTenantId())
      .compose(jobExecutionOptional -> {
        if (jobExecutionOptional.isPresent()) {
          RecordsMetadata.ContentType contentType = rawRecordsDto.getRecordsMetadata().getContentType();
          return Future.succeededFuture(changeEngineService.getParsedRecordsFromInitialRecords(rawRecordsDto.getInitialRecords(), contentType, jobExecutionOptional.get(), UUID.randomUUID().toString()));
        }
        return Future.succeededFuture(Lists.newArrayList());
      });
  }
}
