package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.JobExecutionFilter;
import org.folio.dao.util.SortField;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.MetadataProviderJobLogEntriesJobExecutionIdGetOrder;
import org.folio.rest.jaxrs.model.MetadataProviderJournalRecordsJobExecutionIdGetOrder;
import org.folio.rest.jaxrs.model.JobExecutionDto.SubordinationType;
import org.folio.rest.jaxrs.model.MetadataProviderJobLogEntriesJobExecutionIdGetEntityType;
import org.folio.rest.jaxrs.resource.MetadataProvider;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.IncomingRecordService;
import org.folio.services.JobExecutionService;
import org.folio.services.JournalRecordService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static org.folio.rest.jaxrs.resource.MetadataProvider.GetMetadataProviderJobSummaryByJobExecutionIdResponse.respond404WithTextPlain;

public class MetadataProviderImpl implements MetadataProvider {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final String INVALID_SORT_PARAMS_MSG = "The specified parameter for sorting jobExecutions is invalid: '%s'. Valid sortable fields are: %s. Valid sorting order values are: asc, desc.";
  public static final Set<String> SORT_ORDER_VALUES = Set.of("asc", "desc");
  private static final Set<String> JOB_EXECUTION_SORTABLE_FIELDS =
    Set.of("completed_date", "started_date", "progress_total", "status", "hrid", "file_name", "job_profile_name", "job_user_first_name", "job_user_last_name");

  private static final String DESC = "desc";
  private static final String ASC = "asc";

  @Autowired
  private JobExecutionService jobExecutionService;
  @Autowired
  private JournalRecordService journalRecordService;
  @Autowired
  private IncomingRecordService incomingRecordService;
  private String tenantId;

  public MetadataProviderImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void getMetadataProviderJobExecutions(List<String> statusAny, List<String> profileIdNotAny, String statusNot,
                                               List<String> uiStatusAny, String hrId, String fileName, List<String> fileNameNotAny,
                                               List<String> profileIdAny, List<String> subordinationTypeNotAny, String userId, Date completedAfter,
                                               Date completedBefore, List<String> sortBy, String totalRecords, int offset, int limit, Map<String, String> okapiHeaders,
                                               Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        LOGGER.debug("getMetadataProviderJobExecutions:: sortBy {}", sortBy);
        List<SortField> sortFields = mapSortQueryToSortFields(sortBy);
        JobExecutionFilter filter = buildJobExecutionFilter(statusAny, profileIdNotAny, statusNot, uiStatusAny, hrId, fileName, fileNameNotAny, profileIdAny,
                                      subordinationTypeNotAny, userId, completedAfter, completedBefore);
        jobExecutionService.getJobExecutionsWithoutParentMultiple(filter, sortFields, offset, limit, tenantId)
          .map(GetMetadataProviderJobExecutionsResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getMetadataProviderJournalRecordsByJobExecutionId(String jobExecutionId, String sortBy, MetadataProviderJournalRecordsJobExecutionIdGetOrder order,
                                                                Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    vertxContext.runOnContext(v -> {
      try {
        LOGGER.debug("getMetadataProviderJournalRecordsByJobExecutionId:: jobExecutionId {}, tenantId {}", jobExecutionId, tenantId);
        jobExecutionService.getJobExecutionById(jobExecutionId, tenantId)
          .map(jobExecutionOptional -> jobExecutionOptional.orElseThrow(() ->
            new NotFoundException(format("JobExecution with id '%s' was not found", jobExecutionId))))
          .compose(jobExecution -> journalRecordService.getJobExecutionJournalRecords(jobExecutionId, sortBy, order.name(), tenantId))
          .map(GetMetadataProviderJournalRecordsByJobExecutionIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getMetadataProviderJobLogEntriesByJobExecutionId(String jobExecutionId, String sortBy, MetadataProviderJobLogEntriesJobExecutionIdGetOrder order, boolean errorsOnly,
                                                               MetadataProviderJobLogEntriesJobExecutionIdGetEntityType entityType, String totalRecords, int offset, int limit,
                                                               Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    vertxContext.runOnContext(v -> {
      try {
        LOGGER.debug("getMetadataProviderJobLogEntriesByJobExecutionId:: jobExecutionId {}, sortBy {}, errorsOnly {}, entityType {}",
          jobExecutionId, sortBy, errorsOnly, entityType.name());
        journalRecordService.getJobLogEntryDtoCollection(jobExecutionId, sortBy, order.name(), errorsOnly, entityType.name(), limit, offset, tenantId)
          .map(GetMetadataProviderJobLogEntriesByJobExecutionIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.warn("getMetadataProviderJobLogEntriesByJobExecutionId:: Failed to retrieve JobLogEntryDto entities by JobExecution id", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getMetadataProviderJobLogEntriesRecordsByJobExecutionIdAndRecordId(String jobExecutionId, String recordId,
                                                                                 Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    vertxContext.runOnContext(v -> {
      try {
        LOGGER.debug("getMetadataProviderJobLogEntriesRecordsByJobExecutionIdAndRecordId:: jobExecutionId {}, recordId {}, tenantId {}", jobExecutionId, recordId, tenantId);
        journalRecordService.getRecordProcessingLogDto(jobExecutionId, recordId, tenantId)
          .map(GetMetadataProviderJobLogEntriesRecordsByJobExecutionIdAndRecordIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.warn("getMetadataProviderJobLogEntriesRecordsByJobExecutionIdAndRecordId:: Failed to retrieve RecordProcessingLogDto entity by JobExecution id and Record id", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });

  }

  @Override
  public void getMetadataProviderJobSummaryByJobExecutionId(String jobExecutionId, Map<String, String> okapiHeaders,
                                                            Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        LOGGER.debug("getMetadataProviderJobSummaryByJobExecutionId:: jobExecutionId {}, tenantId {}", jobExecutionId, tenantId);
        journalRecordService.getJobExecutionSummaryDto(jobExecutionId, tenantId)
          .map(jobSummaryOptional -> jobSummaryOptional
            .map(GetMetadataProviderJobSummaryByJobExecutionIdResponse::respond200WithApplicationJson)
            .orElseGet(() -> respond404WithTextPlain(format("JobSummary for jobExecutionId: '%s' was not found", jobExecutionId))))
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.warn("getMetadataProviderJobSummaryByJobExecutionId:: Failed to retrieve JobExecutionSummaryDto by jobExecution id {}", jobExecutionId, e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getMetadataProviderJobExecutionsJobProfiles(String totalRecords, int offset, int limit, Map<String, String> okapiHeaders,
                                                          Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        LOGGER.debug("getMetadataProviderJobExecutionsJobProfiles:: tenantId {}", tenantId);
        jobExecutionService.getRelatedJobProfiles(offset, limit, tenantId)
          .map(GetMetadataProviderJobExecutionsJobProfilesResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getMetadataProviderJobExecutionsUsers(String totalRecords, int offset, int limit, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        LOGGER.debug("getMetadataProviderJobExecutionsUsers:: tenantId {}", tenantId);
        jobExecutionService.getRelatedUsersInfo(offset, limit, tenantId)
          .map(GetMetadataProviderJobExecutionsUsersResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.warn("getMetadataProviderJobExecutionsUsers:: Failed to retrieve unique users info for JobExecutions", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getMetadataProviderIncomingRecordsByRecordId(String recordId, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        LOGGER.debug("getMetadataProviderIncomingRecordsByRecordId:: tenantId {}", tenantId);
        incomingRecordService.getById(recordId, tenantId)
          .map(incomingRecordOptional -> incomingRecordOptional
            .map(GetMetadataProviderIncomingRecordsByRecordIdResponse::respond200WithApplicationJson)
            .orElseGet(() -> GetMetadataProviderIncomingRecordsByRecordIdResponse
              .respond404WithTextPlain(format("IncomingRecord by id: '%s' was not found", recordId))))
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.warn("getMetadataProviderIncomingRecordsByRecordId:: Failed to retrieve IncomingRecord by id", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  private JobExecutionFilter buildJobExecutionFilter(List<String> statusAny, List<String> profileIdNotAny, String statusNot,
                                                     List<String> uiStatusAny, String hrIdPattern, String fileNamePattern,
                                                     List<String> fileNameNotAny, List<String> profileIdAny, List<String> subordinationTypeNotAny,
                                                     String userId, Date completedAfter, Date completedBefore) {
    List<JobExecution.Status> statuses = statusAny.stream()
      .map(JobExecution.Status::fromValue)
      .toList();

    List<JobExecution.UiStatus> uiStatuses = uiStatusAny.stream()
      .map(JobExecution.UiStatus::fromValue)
      .toList();

    List<SubordinationType> subordinationTypes = subordinationTypeNotAny.stream()
      .map(JobExecutionDto.SubordinationType::fromValue)
      .toList();

    return new JobExecutionFilter()
      .withStatusAny(statuses)
      .withProfileIdNotAny(profileIdNotAny)
      .withStatusNot(statusNot == null ? null : JobExecution.Status.fromValue(statusNot))
      .withUiStatusAny(uiStatuses)
      .withHrIdPattern(hrIdPattern)
      .withFileNamePattern(fileNamePattern)
      .withFileNameNotAny(fileNameNotAny)
      .withProfileIdAny(profileIdAny)
      .withSubordinationTypeNotAny(subordinationTypes)
      .withUserId(userId)
      .withCompletedAfter(completedAfter)
      .withCompletedBefore(completedBefore);
  }

  private List<SortField> mapSortQueryToSortFields(List<String> sortQuery) {
    ArrayList<SortField> fields = new ArrayList<>();
    for (String sortFieldQuery : sortQuery) {
      String sortField = StringUtils.substringBefore(sortFieldQuery, ",");
      String sortOrder = StringUtils.substringAfter(sortFieldQuery, ",");

      if (!JOB_EXECUTION_SORTABLE_FIELDS.contains(sortField) || !SORT_ORDER_VALUES.contains(sortOrder)) {
        throw new BadRequestException(format(INVALID_SORT_PARAMS_MSG, sortFieldQuery, JOB_EXECUTION_SORTABLE_FIELDS));
      }
      fields.add(new SortField(sortField, sortOrder));

      if (sortField.equals("status")) {
        fields.add(new SortField("progress_current", getOppositeSortOrder(sortOrder)));
      }
    }
    return fields;
  }

  private String getOppositeSortOrder(String sortOrder) {
    if (sortOrder.equals("asc")) {
      return DESC;
    }
    return ASC;
  }
}
