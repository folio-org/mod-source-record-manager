package org.folio.services.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FilenameUtils;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.jaxrs.model.LogDto;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.List;


/**
 * Testing conversion logic for the JobExecutionToLogDtoConverter
 *
 * @see JobExecutionToLogDtoConverter
 */
public class JobExecutionToLogDtoConverterUnitTest extends JobExecutionToDtoConverterUnitTest {

  private JobExecutionToLogDtoConverter converter = new JobExecutionToLogDtoConverter();

  @Test
  public void shouldReturnLogDtoWhenPassSingleEntity() throws IOException {
    // given
    JobExecutionCollection jobExecutionCollection = new ObjectMapper().readValue(UnitTestUtil.readFileFromPath(SINGLE_JOB_EXECUTION_SAMPLE_PATH), JobExecutionCollection.class);
    JobExecution jobExecutionEntity = jobExecutionCollection.getJobExecutions().get(0);
    // when
    List<LogDto> collectionDtoList = converter.convert(jobExecutionCollection.getJobExecutions());
    // then
    Assert.assertNotNull(collectionDtoList);
    Assert.assertEquals(1, collectionDtoList.size());

    LogDto logDto = collectionDtoList.get(0);
    Assert.assertEquals(logDto.getJobExecutionId(), jobExecutionEntity.getId());
    Assert.assertEquals(logDto.getJobExecutionHrId(), jobExecutionEntity.getHrId());
    Assert.assertEquals(logDto.getJobProfileName(), jobExecutionEntity.getJobProfileName());
    Assert.assertEquals(logDto.getRunBy().getFirstName(), jobExecutionEntity.getRunBy().getFirstName());
    Assert.assertEquals(logDto.getRunBy().getLastName(), jobExecutionEntity.getRunBy().getLastName());
    Assert.assertEquals(logDto.getFileName(), jobExecutionEntity.getSourcePath());
    Assert.assertEquals(logDto.getCompletedDate(), jobExecutionEntity.getCompletedDate());
  }

  @Test
  public void shouldReturnJobExecutionDtoCollectionWhenPassMultipleEntity() throws IOException {
    // given
    JobExecutionCollection jobExecutionCollection = new ObjectMapper().readValue(UnitTestUtil.readFileFromPath(MULTIPLE_JOB_EXECUTION_SAMPLE_PATH), JobExecutionCollection.class);
    // when
    List<LogDto> collectionDtoList = converter.convert(jobExecutionCollection.getJobExecutions());
    // then
    Assert.assertNotNull(collectionDtoList);
    Assert.assertEquals(collectionDtoList.size(), jobExecutionCollection.getJobExecutions().size());
  }
}
