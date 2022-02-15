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

@Component
public class ParsedRecordsDiErrorProvider {

  private static final String ERROR_SOURCE_CHUNK_ID = "a5fe4e07-d6b3-47c7-9b84-3e4074f7177f";

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
          return Future.succeededFuture(changeEngineService.getParsedRecordsFromInitialRecords(rawRecordsDto.getInitialRecords(), contentType, jobExecutionOptional.get(), ERROR_SOURCE_CHUNK_ID));
        }
        return Future.succeededFuture(Lists.newArrayList());
      });
  }
}
