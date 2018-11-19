package org.folio.services.converters;

import org.apache.commons.io.FilenameUtils;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.Progress;

/**
 * Converts JobExecution entity to the JobExecutionDto.
 *
 * @see JobExecution
 * @see JobExecutionDto
 */
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

    // TODO fetch JobProfile entity by id and set profile name to the target
    target.setJobProfileName(source.getJobProfileName());
    // TODO set progress properly
    Progress progress = new Progress();
    progress.setCurrent(23);
    progress.setTotal(33);
    target.setProgress(progress);
    return target;
  }
}
