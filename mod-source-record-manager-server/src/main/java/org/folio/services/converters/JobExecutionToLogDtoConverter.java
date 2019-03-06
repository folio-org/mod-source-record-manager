package org.folio.services.converters;

import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.LogDto;
import org.springframework.stereotype.Component;

/**
 * Converts JobExecution entity to the LogDto.
 *
 * @see JobExecution
 * @see LogDto
 */
@Component
public class JobExecutionToLogDtoConverter extends AbstractGenericConverter<JobExecution, LogDto> {

  @Override
  public LogDto convert(JobExecution source) {
    return new LogDto()
      .withJobExecutionId(source.getId())
      .withJobExecutionHrId(source.getHrId())
      .withJobProfileName(source.getJobProfile() == null ? null : source.getJobProfile().getName())
      .withCompletedDate(source.getCompletedDate())
      .withFileName(source.getSourcePath())
      .withRunBy(source.getRunBy())
      .withStatus(LogDto.Status.valueOf(source.getStatus().value()))
      .withUiStatus(LogDto.UiStatus.valueOf(source.getUiStatus().value()));
  }
}
