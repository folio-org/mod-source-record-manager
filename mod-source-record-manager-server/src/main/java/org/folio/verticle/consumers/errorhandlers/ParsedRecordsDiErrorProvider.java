package org.folio.verticle.consumers.errorhandlers;

import com.google.common.collect.Lists;
import io.vertx.core.Future;
import java.util.List;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.folio.services.ChangeEngineService;
import org.folio.services.JobExecutionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ParsedRecordsDiErrorProvider {

  private final JobExecutionService jobExecutionService;
  private final ChangeEngineService changeEngineService;

  @Autowired
  public ParsedRecordsDiErrorProvider(JobExecutionService jobExecutionService,
                                      ChangeEngineService changeEngineService) {
    this.jobExecutionService = jobExecutionService;
    this.changeEngineService = changeEngineService;
  }

  /**
   * Get parsed records from initial records in the particular chunk.
   *
   * @param okapiParams    the okapi params
   * @param jobExecutionId the job execution id
   * @param chunkId        the id of the failed chunk, used for logging only
   * @param rawRecordsDto  the raw records dto
   * @return list of parsed records
   */
  public Future<List<Record>> getParsedRecordsFromInitialRecords(ConnectionParams okapiParams,
                                                                 String jobExecutionId,
                                                                 String chunkId,
                                                                 RawRecordsDto rawRecordsDto) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, okapiParams.getTenantId())
      .compose(jobExecutionOptional -> {
        if (jobExecutionOptional.isPresent()) {
          RecordsMetadata.ContentType contentType = rawRecordsDto.getRecordsMetadata().getContentType();
          return Future.succeededFuture(
            changeEngineService.getParsedRecordsFromInitialRecords(rawRecordsDto.getInitialRecords(),
              contentType, jobExecutionOptional.get(), false, chunkId));
        }
        return Future.succeededFuture(Lists.newArrayList());
      });
  }
}
