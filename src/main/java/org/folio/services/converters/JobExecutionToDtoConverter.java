package org.folio.services.converters;

import org.apache.commons.io.FilenameUtils;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.Progress;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Converts JobExecution entity to the JobExecutionDto.
 *
 * @see JobExecution
 * @see JobExecutionDto
 */
public class JobExecutionToDtoConverter extends AbstractGenericConverter<JobExecution, JobExecutionDto> {

  // JUST for Demo session!!! Must be removed!!!
  private static final Map<String, Progress> dummyProgress = new ConcurrentHashMap<>();


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
//    Random random = new Random();
//    int nextInt;
//    int total;
//    Progress progress = new Progress();
//
//    switch (source.getStatus()) {
//      case IMPORT_IN_PROGRESS:
//      case PARSING_IN_PROGRESS:
//      case PROCESSING_IN_PROGRESS:
//        nextInt = random.nextInt(1000);
//        total = nextInt + random.nextInt(1000);
//        break;
//      case PARSING_FINISHED:
//      case PROCESSING_FINISHED:
//      case IMPORT_FINISHED:
//      case COMMITTED:
//      default:
//        nextInt = random.nextInt(1000);
//        total = nextInt;
//    }
//    progress.setCurrent(nextInt);
//    progress.setTotal(total);
    target.setProgress(getProgress(source));
    return target;
  }

  // JUST for Demo session!!! Must be removed!!!
  private Progress getProgress(JobExecution source) {
    String sourceId = source.getId();
    JobExecution.Status status = source.getStatus();
    Progress progress = dummyProgress.get(sourceId);
    if (progress == null) {
      Progress tmpProgress = dummyProgress.putIfAbsent(sourceId, progress = new Progress());
      if (tmpProgress != null) {
        progress = tmpProgress;
      }

      if (progress.getCurrent() == null) {
        synchronized (progress) {
          if (progress.getCurrent() == null) {
            return fillProgressData(progress, status);
          }
        }
      }
      return progress;

    } else {
      synchronized (progress) {
        return checkAndMoveProgressData(progress, status);
      }
    }
  }

  private Progress fillProgressData(Progress progress, JobExecution.Status status) {
    // TODO set progress properly
    Random random = new Random();
    int nextInt;
    int total;
    switch (status) {
      case IMPORT_IN_PROGRESS:
      case PARSING_IN_PROGRESS:
      case PROCESSING_IN_PROGRESS:
        nextInt = random.nextInt(1000);
        total = nextInt + random.nextInt(3000);
        break;
      case PARSING_FINISHED:
      case PROCESSING_FINISHED:
      case IMPORT_FINISHED:
      case COMMITTED:
      default:
        nextInt = random.nextInt(1000);
        total = nextInt;
    }
    progress.setCurrent(nextInt);
    progress.setTotal(total);

    return progress;
  }

  private Progress checkAndMoveProgressData(Progress progress, JobExecution.Status status) {

    switch (status) {
      case IMPORT_IN_PROGRESS:
      case PARSING_IN_PROGRESS:
      case PROCESSING_IN_PROGRESS:
        Random random = new Random();
        int current = progress.getCurrent();
        int total = progress.getTotal();
        current =  current + random.nextInt(101);
        if (current > total) {
          current -= total;
        }

        progress.setCurrent(current);

        return new Progress().withCurrent(current).withTotal(total);
    }
    return progress;
  }
}
