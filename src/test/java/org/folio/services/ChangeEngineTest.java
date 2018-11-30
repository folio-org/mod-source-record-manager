package org.folio.services;

import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.is;

/**
 * REST tests for MetadataProvider to manager JobExecution entities
 */
@RunWith(VertxUnitRunner.class)
public class ChangeEngineTest extends AbstractRestTest {
  private ChangeEngineService changeEngineService;
  private JobExecutionService jobExecutionService;
  private static final String POST_JOB_EXECUTIONS_PATH = "/change-manager/jobExecutions";

  @Before
  public void beforeTest() {
    changeEngineService = new ChangeEngineServiceImpl(vertx, TENANT_ID);
    jobExecutionService = new JobExecutionServiceImpl(vertx, TENANT_ID);
  }

  @Test
  public void parseWithoutErrorsInTwoChunks() {
    InitJobExecutionsRqDto rq = new InitJobExecutionsRqDto();
    rq.setFiles(Collections.singletonList(new File().withName("importMarc.mrc")));
    jobExecutionService.initializeJobExecutions(rq, params).compose(rs -> changeEngineService.parseSourceRecordsForJobExecution(rs.getJobExecutions().get(0), params));
  }

  private List<JobExecution> createJobExecutions() {
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.getFiles().add(new File().withName("importMarc.mrc"));
    InitJobExecutionsRsDto response = RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when()
      .post(POST_JOB_EXECUTIONS_PATH)
      .body().as(InitJobExecutionsRsDto.class);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    return createdJobExecutions;
  }
}
