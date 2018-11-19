package org.folio.services.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.util.List;


/**
 * Testing conversion logic for the JobExecutionToDtoConverter
 *
 * @see JobExecutionToDtoConverter
 */
@RunWith(MockitoJUnitRunner.class)
public class JobExecutionToDtoConverterUnitTest {

  private static final String SINGLE_JOB_EXECUTION_SAMPLE_PATH = "src/test/resources/org/folio/services/converters/jobExecutionCollectionSingleTest.sample";
  private static final String MULTIPLE_JOB_EXECUTION_SAMPLE_PATH = "src/test/resources/org/folio/services/converters/jobExecutionCollectionMultipleTest.sample";
  private JobExecutionToDtoConverter converter = new JobExecutionToDtoConverter();


  @Test
  public void shouldReturnJobExecutionDtoCollectionWhenPassSingleEntity() throws IOException {
    // given
    JobExecutionCollection jobExecutionCollection = new ObjectMapper().readValue(readFileFromPath(SINGLE_JOB_EXECUTION_SAMPLE_PATH), JobExecutionCollection.class);
    JobExecution jobExecutionEntity = jobExecutionCollection.getJobExecutions().get(0);
    // when
    List<JobExecutionDto> collectionDtoList = converter.convert(jobExecutionCollection.getJobExecutions());
    // then
    Assert.assertNotNull(collectionDtoList);
    Assert.assertEquals(collectionDtoList.size(), 1);

    JobExecutionDto jobExecutionDto = collectionDtoList.get(0);
    Assert.assertEquals(jobExecutionDto.getId(), jobExecutionEntity.getId());
    Assert.assertEquals(jobExecutionDto.getHrId(), jobExecutionEntity.getHrId());
    Assert.assertEquals(jobExecutionDto.getRunBy(), jobExecutionEntity.getRunBy());
    Assert.assertEquals(jobExecutionDto.getFileName(), FilenameUtils.getName(jobExecutionEntity.getSourcePath()));
    Assert.assertEquals(jobExecutionDto.getStartedDate(), jobExecutionEntity.getStartedDate());
    Assert.assertEquals(jobExecutionDto.getCompletedDate(), jobExecutionEntity.getCompletedDate());
    Assert.assertEquals(jobExecutionDto.getStatus().name(), jobExecutionEntity.getStatus().name());
    // TODO assert JobProfile name properly using JobProfile id
    Assert.assertEquals(jobExecutionDto.getJobProfileName(), jobExecutionEntity.getJobProfileName());
    // TODO assert progress properly
  }

  @Test
  public void shouldReturnJobExecutionDtoCollectionWhenPassMultipleEntity() throws IOException {
    // given
    JobExecutionCollection jobExecutionCollection = new ObjectMapper().readValue(readFileFromPath(MULTIPLE_JOB_EXECUTION_SAMPLE_PATH), JobExecutionCollection.class);
    // when
    List<JobExecutionDto> collectionDtoList = converter.convert(jobExecutionCollection.getJobExecutions());
    // then
    Assert.assertNotNull(collectionDtoList);
    Assert.assertEquals(collectionDtoList.size(), jobExecutionCollection.getJobExecutions().size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldReturnEmptyJobExecutionDtoCollectionWhenPassNull() {
    // given
    List<JobExecution> givenNullCollection = null;
    converter.convert(givenNullCollection);
  }

  private String readFileFromPath(String path) throws IOException {
    return FileUtils.readFileToString(new File(path));
  }
}
