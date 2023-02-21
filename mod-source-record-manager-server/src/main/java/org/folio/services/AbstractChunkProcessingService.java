package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.MappingProfile;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.exceptions.UnsupportedProfileException;

import javax.ws.rs.NotFoundException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.*;


public abstract class AbstractChunkProcessingService implements ChunkProcessingService {
  private static final Logger LOGGER = LogManager.getLogger();
  private static final String JOB_EXECUTION_MARKED_AS_ERROR_MSG = "Couldn't update JobExecution status, JobExecution already marked as ERROR";
  private static final String JOB_EXECUTION_MARKED_AS_CANCELLED_MSG = "Couldn't update JobExecution status, JobExecution already marked as CANCELLED";
  public static final String UNIQUE_CONSTRAINT_VIOLATION_CODE = "23505";

  protected JobExecutionSourceChunkDao jobExecutionSourceChunkDao;
  protected JobExecutionService jobExecutionService;

  public AbstractChunkProcessingService(JobExecutionSourceChunkDao jobExecutionSourceChunkDao,
                                        JobExecutionService jobExecutionService) {
    this.jobExecutionSourceChunkDao = jobExecutionSourceChunkDao;
    this.jobExecutionService = jobExecutionService;
  }

  @Override
  public Future<Boolean> processChunk(RawRecordsDto incomingChunk, String jobExecutionId, OkapiConnectionParams params) {
    LOGGER.debug("AbstractChunkProcessingService:: processChunk for jobExecutionId: {}", jobExecutionId);
    prepareChunk(incomingChunk);
    return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
      .compose(optionalJobExecution -> optionalJobExecution
        .map(jobExecution -> mapJobExecution(incomingChunk, jobExecution, params))
        .orElse(Future.failedFuture(new NotFoundException(String.format("Couldn't find JobExecution with id %s", jobExecutionId)))));
  }

  @Override
  public Future<Boolean> processChunk(RawRecordsDto incomingChunk, JobExecution jobExecution, OkapiConnectionParams params) {
    LOGGER.debug("AbstractChunkProcessingService:: processChunk with jobExecutionId: {}", jobExecution.getId());
    prepareChunk(incomingChunk);
    return mapJobExecution(incomingChunk, jobExecution, params);
  }
  private Future<Boolean> mapJobExecution(RawRecordsDto incomingChunk, JobExecution jobExecution, OkapiConnectionParams params) {
    if (isNotSupportedJobProfileExists(jobExecution)) {
      throw new UnsupportedProfileException("Unsupported type of Job Profile.");
    }

    JobExecutionSourceChunk sourceChunk = new JobExecutionSourceChunk()
      .withId(incomingChunk.getId())
      .withJobExecutionId(jobExecution.getId())
      .withLast(incomingChunk.getRecordsMetadata().getLast())
      .withState(JobExecutionSourceChunk.State.IN_PROGRESS)
      .withChunkSize(incomingChunk.getInitialRecords().size())
      .withCreatedDate(new Date());

    return jobExecutionSourceChunkDao.save(sourceChunk, params.getTenantId())
      .compose(ar -> processRawRecordsChunk(incomingChunk, sourceChunk, jobExecution.getId(), params))
      .map(true)
      .recover(throwable -> throwable instanceof PgException && ((PgException) throwable).getCode().equals(UNIQUE_CONSTRAINT_VIOLATION_CODE) ?
        Future.failedFuture(new DuplicateEventException(String.format("Source chunk with %s id for %s jobExecution is already exists", incomingChunk.getId(), jobExecution.getId()))) :
        Future.failedFuture(throwable));
  }

  private boolean isNotSupportedJobProfileExists(JobExecution jobExecution) {
    boolean isExists = false;
    List<ProfileSnapshotWrapper> childProfiles = jobExecution.getJobProfileSnapshotWrapper().getChildSnapshotWrappers();
    if (!CollectionUtils.isEmpty(childProfiles)) {
      isExists = isExistsMatchProfileToInstanceWithActionUpdateMarcBib(childProfiles);
    }
    return isExists;
  }

  //Disabled SONAR check "Loops with at most one iteration should be refactored" for recursive code
  private boolean isExistsMatchProfileToInstanceWithActionUpdateMarcBib(Collection<ProfileSnapshotWrapper> profileSnapshotWrapperList) { //NOSONAR
    for (ProfileSnapshotWrapper profileSnapshotWrapper : profileSnapshotWrapperList) {
      if ((profileSnapshotWrapper.getContentType() == MATCH_PROFILE) && (isMatchingMarcBibToInstanceRelation(profileSnapshotWrapper))) {
        ProfileSnapshotWrapper actionProfileSnapshotWrapper = getChildSnapshotWrapperByType(profileSnapshotWrapper, ACTION_PROFILE);
        if (actionProfileSnapshotWrapper != null) {
          ProfileSnapshotWrapper mappingProfileSnapshotWrapper = getChildSnapshotWrapperByType(actionProfileSnapshotWrapper, MAPPING_PROFILE);
          if ((mappingProfileSnapshotWrapper != null) && (isMappingMarcBibToMarcBibRelation(mappingProfileSnapshotWrapper))) {
            return true;
          }
        }
      }
      if (!CollectionUtils.isEmpty(profileSnapshotWrapper.getChildSnapshotWrappers())) {
        return isExistsMatchProfileToInstanceWithActionUpdateMarcBib(profileSnapshotWrapper.getChildSnapshotWrappers());
      }
    }
    return false;
  }

  private ProfileSnapshotWrapper getChildSnapshotWrapperByType(ProfileSnapshotWrapper profileSnapshotWrapper,
                                                                       ProfileSnapshotWrapper.ContentType contentType) {
    if (!CollectionUtils.isEmpty(profileSnapshotWrapper.getChildSnapshotWrappers())) {
      List<ProfileSnapshotWrapper> childSnapshotWrappers = profileSnapshotWrapper.getChildSnapshotWrappers();
      for(ProfileSnapshotWrapper snapshotWrapper : childSnapshotWrappers) {
        if (snapshotWrapper.getContentType() == contentType) {
          return snapshotWrapper;
        }
      }
    }
    return null;
  }

  private boolean isMatchingMarcBibToInstanceRelation(ProfileSnapshotWrapper profileSnapshotWrapper) {
    boolean isMatchingRelation = false;
    MatchProfile matchProfile = extractProfile(profileSnapshotWrapper, MatchProfile.class);
    if (!CollectionUtils.isEmpty(matchProfile.getMatchDetails())) {
      MatchDetail matchDetail = matchProfile.getMatchDetails().get(0);
      isMatchingRelation = matchDetail.getIncomingRecordType() == EntityType.MARC_BIBLIOGRAPHIC
        && matchDetail.getExistingRecordType() == EntityType.INSTANCE;
    }
    return isMatchingRelation;
  }

  private boolean isMappingMarcBibToMarcBibRelation(ProfileSnapshotWrapper profileSnapshotWrapper) {
    MappingProfile mappingProfile = extractProfile(profileSnapshotWrapper, MappingProfile.class);
    return mappingProfile.getIncomingRecordType() == EntityType.MARC_BIBLIOGRAPHIC
      && mappingProfile.getExistingRecordType() == EntityType.MARC_BIBLIOGRAPHIC;
  }

  private <T> T extractProfile(ProfileSnapshotWrapper profileSnapshotWrapper, Class clazz) {
    T profile;
    if (profileSnapshotWrapper.getContent() instanceof Map) {
      profile = (T) new JsonObject((Map) profileSnapshotWrapper.getContent()).mapTo(clazz);
    } else {
      profile = (T) profileSnapshotWrapper.getContent();
    }
    return profile;
  }

  private void prepareChunk(RawRecordsDto rawRecordsDto) {
    boolean isAnyRecordHasNoOrder = rawRecordsDto.getInitialRecords().stream()
      .anyMatch(initialRecord -> initialRecord.getOrder() == null);

    if (rawRecordsDto.getInitialRecords() != null && isAnyRecordHasNoOrder) {
      int firstRecordOrderOfCurrentChunk = rawRecordsDto.getRecordsMetadata().getCounter() - rawRecordsDto.getInitialRecords().size();

      for (InitialRecord initialRecord : rawRecordsDto.getInitialRecords()) {
        initialRecord.setOrder(firstRecordOrderOfCurrentChunk++);
      }
    }
  }

  /**
   * Process chunk of RawRecords
   *
   * @param incomingChunk  - chunk with raw records
   * @param sourceChunk    - source chunk job execution
   * @param jobExecutionId - JobExecution id
   * @param params         - okapi connection params
   * @return future with boolean
   */
  protected abstract Future<Boolean> processRawRecordsChunk(RawRecordsDto incomingChunk, JobExecutionSourceChunk sourceChunk, String jobExecutionId, OkapiConnectionParams params);

  /**
   * Checks JobExecution current status and updates it if needed
   *
   * @param jobExecutionId - JobExecution id
   * @param status         - required statusDto of JobExecution
   * @param params         - okapi connection params
   * @return future
   */
  protected Future<JobExecution> checkAndUpdateJobExecutionStatusIfNecessary(String jobExecutionId, StatusDto status, OkapiConnectionParams params) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
      .compose(optionalJobExecution -> optionalJobExecution
        .map(jobExecution -> {
          if (jobExecution.getStatus() == JobExecution.Status.ERROR) {
            LOGGER.warn(JOB_EXECUTION_MARKED_AS_ERROR_MSG);
            return Future.<JobExecution>failedFuture(JOB_EXECUTION_MARKED_AS_ERROR_MSG);
          }
          if (jobExecution.getStatus() == JobExecution.Status.CANCELLED) {
            LOGGER.warn(JOB_EXECUTION_MARKED_AS_CANCELLED_MSG);
            return Future.<JobExecution>failedFuture(JOB_EXECUTION_MARKED_AS_CANCELLED_MSG);
          }
          if (jobExecution.getStatus() == JobExecution.Status.COMMITTED) {
            return Future.succeededFuture(jobExecution);
          }
          if (!status.getStatus().value().equals(jobExecution.getStatus().value())) {
            return jobExecutionService.updateJobExecutionStatus(jobExecutionId, status, params);
          }
          return Future.succeededFuture(jobExecution);
        }).orElse(Future.failedFuture(new NotFoundException(String.format("Couldn't find JobExecution with id %s", jobExecutionId)))));
  }

}
