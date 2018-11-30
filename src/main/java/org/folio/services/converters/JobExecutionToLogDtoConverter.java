package org.folio.services.converters;

import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.LogDto;

/**
 * Converts JobExecution entity to the LogDto.
 *
 * @see JobExecution
 * @see LogDto
 */
public class JobExecutionToLogDtoConverter extends AbstractGenericConverter<JobExecution, LogDto> {

  @Override
  public LogDto convert(JobExecution source) {
    return new LogDto()
      .withJobExecutionId(source.getId())
      .withJobExecutionHrId(source.getHrId())
      .withJobProfileName(source.getJobProfileName())
      .withCompletedDate(source.getCompletedDate())
      .withFileName(source.getSourcePath())
      .withRunBy(source.getRunBy());
  }
}
