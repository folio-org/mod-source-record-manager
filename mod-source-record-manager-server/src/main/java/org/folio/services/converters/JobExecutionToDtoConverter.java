package org.folio.services.converters;

import org.apache.commons.io.FilenameUtils;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.springframework.stereotype.Component;

/**
 * Converts JobExecution entity to the JobExecutionDto.
 *
 * @see JobExecution
 * @see JobExecutionDto
 */
@Component
public class JobExecutionToDtoConverter extends AbstractGenericConverter<JobExecution, JobExecutionDto> {

  @Override
  public JobExecutionDto convert(JobExecution source) {
    JobExecutionDto target = new JobExecutionDto();
    target.setId(source.getId());
    target.setHrId(source.getHrId());
    target.setFileName(FilenameUtils.getName(source.getSourcePath()));
    target.setRunBy(source.getRunBy());
    target.setStartedDate(source.getStartedDate());
    target.setCompletedDate(source.getCompletedDate());
    target.setStatus(JobExecutionDto.Status.valueOf(source.getStatus().name()));
    target.setUiStatus(JobExecutionDto.UiStatus.valueOf((source.getUiStatus().name())));
    if (source.getJobProfileInfo() != null) {
      target.setJobProfileName(source.getJobProfileInfo().getName());
    }
    target.setProgress(source.getProgress());
    return target;
  }
}
