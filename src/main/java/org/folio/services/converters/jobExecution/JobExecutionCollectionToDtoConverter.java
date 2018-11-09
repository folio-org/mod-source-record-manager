package org.folio.services.converters.jobExecution;

import org.apache.commons.io.FilenameUtils;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.jaxrs.model.JobExecutionCollectionDto;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.Progress;
import org.folio.services.converters.Converter;

import java.util.List;

/**
 * Converts JobExecutionCollection entity to JobExecutionCollectionDto
 * @see JobExecutionCollection
 * @see JobExecutionCollectionDto
 */
public class JobExecutionCollectionToDtoConverter implements Converter<JobExecutionCollection, JobExecutionCollectionDto> {

  private static final String JOBPROFILE_NAME = "Marc jobs profile";

  @Override
  public JobExecutionCollectionDto convert(JobExecutionCollection entityCollection) {
    JobExecutionCollectionDto collectionDto = new JobExecutionCollectionDto();
    if (entityCollection == null) {
      collectionDto.setTotalRecords(0);
      return collectionDto;
    }
    List<JobExecution> entities = entityCollection.getJobExecutions();

    for (JobExecution entity : entities) {
      collectionDto.getJobExecutionDtos().add(convertEntityToDto(entity));
    }
    collectionDto.setTotalRecords(collectionDto.getJobExecutionDtos().size());
    return collectionDto;
  }

  private JobExecutionDto convertEntityToDto(JobExecution entity) {
    JobExecutionDto dto = new JobExecutionDto();
    dto.setJobExecutionId(entity.getJobExecutionId());
    dto.setJobExecutionHrId(entity.getJobExecutionHrId());
    dto.setFileName(FilenameUtils.getName(entity.getSourcePath()));
    dto.setRunBy(entity.getRunBy());
    dto.setStartedDate(entity.getStartedDate());
    dto.setCompletedDate(entity.getCompletedDate());
    dto.setStatus(JobExecutionDto.Status.valueOf(entity.getStatus().name()));

    // TODO fetch JobProfile entity by id and set profile name to the target
    dto.setJobProfileName(JOBPROFILE_NAME);
    // TODO set progress properly
    Progress progress = new Progress();
    progress.setCurrent(23);
    progress.setTotal(33);
    dto.setProgress(progress);
    return dto;
  }
}
