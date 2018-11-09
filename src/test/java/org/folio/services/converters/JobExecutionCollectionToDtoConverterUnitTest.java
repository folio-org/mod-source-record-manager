package org.folio.services.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.jaxrs.model.JobExecutionCollectionDto;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.services.converters.jobExecution.JobExecutionCollectionToDtoConverter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;

/**
 * Testing conversion logic for the JobExecutionCollectionToDtoConverter
 *
 * @see JobExecutionCollectionToDtoConverter
 */
@RunWith(MockitoJUnitRunner.class)
public class JobExecutionCollectionToDtoConverterUnitTest {

  private static final String SINGLE_JOB_EXECUTION_SAMPLE_PATH = "src/test/resources/org/folio/services/converters/jobExecutionCollectionSingleTest.sample";
  private static final String MULTIPLE_JOB_EXECUTION_SAMPLE_PATH = "src/test/resources/org/folio/services/converters/jobExecutionCollectionMultipleTest.sample";
  private JobExecutionCollectionToDtoConverter converter = new JobExecutionCollectionToDtoConverter();


  @Test
  public void shouldReturnJobExecutionDtoCollectionWhenPassSingleEntity() throws IOException {
    // given
    JobExecutionCollection jobExecutionCollection = new ObjectMapper().readValue(readFileFromPath(SINGLE_JOB_EXECUTION_SAMPLE_PATH), JobExecutionCollection.class);
    JobExecution jobExecutionEntity = jobExecutionCollection.getJobExecutions().get(0);
    // when
    JobExecutionCollectionDto collectionDto = converter.convert(jobExecutionCollection);
    // then
    Assert.assertNotNull(collectionDto);
    Assert.assertNotNull(collectionDto.getJobExecutionDtos());
    Assert.assertNotNull(collectionDto.getTotalRecords());
    Assert.assertEquals(collectionDto.getJobExecutionDtos().size(), 1);
    Assert.assertEquals(collectionDto.getTotalRecords().intValue(), 1);

    JobExecutionDto jobExecutionDto = collectionDto.getJobExecutionDtos().get(0);
    Assert.assertEquals(jobExecutionDto.getJobExecutionId(), jobExecutionEntity.getJobExecutionId());
    Assert.assertEquals(jobExecutionDto.getJobExecutionHrId(), jobExecutionEntity.getJobExecutionHrId());
    Assert.assertEquals(jobExecutionDto.getRunBy(), jobExecutionEntity.getRunBy());
    Assert.assertEquals(jobExecutionDto.getFileName(), FilenameUtils.getName(jobExecutionEntity.getSourcePath()));
    Assert.assertEquals(jobExecutionDto.getStartedDate(), jobExecutionEntity.getStartedDate());
    Assert.assertEquals(jobExecutionDto.getCompletedDate(), jobExecutionEntity.getCompletedDate());
    Assert.assertEquals(jobExecutionDto.getStatus().name(), jobExecutionEntity.getStatus().name());
    // TODO assert progress properly
    // TODO assert JobProfile name properly
  }

  @Test
  public void shouldReturnJobExecutionDtoCollectionWhenPassMultipleEntity() throws IOException {
    // given
    JobExecutionCollection jobExecutionCollection = new ObjectMapper().readValue(readFileFromPath(MULTIPLE_JOB_EXECUTION_SAMPLE_PATH), JobExecutionCollection.class);
    // when
    JobExecutionCollectionDto result = converter.convert(jobExecutionCollection);
    // then
    Assert.assertNotNull(result);
    Assert.assertNotNull(result.getJobExecutionDtos());
    Assert.assertNotNull(result.getTotalRecords());
    Assert.assertEquals(result.getJobExecutionDtos().size(), jobExecutionCollection.getJobExecutions().size());
    Assert.assertEquals(result.getTotalRecords().intValue(), jobExecutionCollection.getTotalRecords().intValue());
  }

  @Test
  public void shouldReturnEmptyJobExecutionDtoCollectionWhenPassNull() {
    // when
    JobExecutionCollectionDto result = converter.convert(null);
    // then
    Assert.assertNotNull(result);
    Assert.assertNotNull(result.getJobExecutionDtos());
    Assert.assertNotNull(result.getTotalRecords());
    Assert.assertEquals(result.getJobExecutionDtos().size(), 0);
    Assert.assertEquals(result.getTotalRecords().intValue(), 0);
  }

  private String readFileFromPath(String path) throws IOException {
    return FileUtils.readFileToString(new File(path));
  }
}
