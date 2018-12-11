package org.folio.rest.impl.changeManager;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;

/**
 * REST tests for ChangeManager to manager JobExecution entities initialization
 */
@RunWith(VertxUnitRunner.class)
public class ChangeManagerAPITest extends AbstractRestTest {

  private static final String POST_JOB_EXECUTIONS_PATH = "/change-manager/jobExecutions";
  private static final String PUT_JOB_EXECUTION_PATH = "/change-manager/jobExecution";
  private static final String GET_JOB_EXECUTIONS_PATH = "/metadata-provider/jobExecutions";
  private static final String POST_RAW_RECORDS_PATH = "/change-manager/records";

  private Set<JobExecution.SubordinationType> parentTypes = EnumSet.of(
    JobExecution.SubordinationType.PARENT_SINGLE,
    JobExecution.SubordinationType.PARENT_MULTIPLE
  );

  private JsonObject jobExecution = new JsonObject()
    .put("id", "5105b55a-b9a3-4f76-9402-a5243ea63c95")
    .put("hrId", "1000")
    .put("parentJobId", "5105b55a-b9a3-4f76-9402-a5243ea63c95")
    .put("subordinationType", "PARENT_SINGLE")
    .put("status", "NEW")
    .put("uiStatus", "INITIALIZATION")
    .put("sourcePath", "importMarc.mrc")
    .put("jobProfileName", "Marc jobs profile")
    .put("userId", UUID.randomUUID().toString());

  private JsonObject chunk = new JsonObject()
    .put("last", false)
    .put("total", 15)
    .put("records", new JsonArray());

  @Test
  public void testInitJobExecutionsWith1File() {
    // given
    File givenFile = new File().withName("importBib.bib");
    int expectedJobExecutionsNumber = 1;

    // when
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(Arrays.asList(givenFile)).body().as(InitJobExecutionsRsDto.class);

    // then
    String actualParentJobExecutionId = response.getParentJobExecutionId();
    List<JobExecution> actualJobExecutions = response.getJobExecutions();

    Assert.assertNotNull(actualParentJobExecutionId);
    Assert.assertEquals(expectedJobExecutionsNumber, actualJobExecutions.size());

    JobExecution parentSingle = actualJobExecutions.get(0);
    Assert.assertEquals(JobExecution.SubordinationType.PARENT_SINGLE, parentSingle.getSubordinationType());
    assertParent(parentSingle);
  }

  @Test
  public void testInitJobExecutionsWith2Files() {
    // given
    File file1 = new File().withName("importBib.bib");
    File file2 = new File().withName("importMarc.mrc");
    int expectedParentJobExecutions = 1;
    int expectedChildJobExecutions = 2;
    int expectedJobExecutionsNumber = expectedParentJobExecutions + expectedChildJobExecutions;

    // when
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(Arrays.asList(file1, file2)).body().as(InitJobExecutionsRsDto.class);

    // then
    String actualParentJobExecutionId = response.getParentJobExecutionId();
    List<JobExecution> actualJobExecutions = response.getJobExecutions();

    Assert.assertNotNull(actualParentJobExecutionId);
    Assert.assertEquals(expectedJobExecutionsNumber, actualJobExecutions.size());

    int actualParentJobExecutions = 0;
    int actualChildJobExecutions = 0;

    for (JobExecution actualJobExecution : actualJobExecutions) {
      if (JobExecution.SubordinationType.PARENT_MULTIPLE.equals(actualJobExecution.getSubordinationType())) {
        assertParent(actualJobExecution);
        actualParentJobExecutions++;
      } else {
        assertChild(actualJobExecution, actualParentJobExecutionId);
        actualChildJobExecutions++;
      }
    }

    Assert.assertEquals(expectedParentJobExecutions, actualParentJobExecutions);
    Assert.assertEquals(expectedChildJobExecutions, actualChildJobExecutions);
  }

  @Test
  public void testInitJobExecutionsWithNoFiles(TestContext context) {
    // given
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.setUserId(UUID.randomUUID().toString());

    // when
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().post(POST_JOB_EXECUTIONS_PATH)
      .then().statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void shouldReturnBadRequestOnPutWhenNoJobExecutionPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .put(PUT_JOB_EXECUTION_PATH + "/11dfac11-1caf-4470-9ad1-d533f6360bdd")
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldReturnNotFoundOnPutWhenRecordDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .body(jobExecution.toString())
      .when()
      .put(PUT_JOB_EXECUTION_PATH + "/" + jobExecution.getString("id"))
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldUpdateSingleParentOnPut() {
    File file = new File().withName("importBib.bib");
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(Arrays.asList(file)).body().as(InitJobExecutionsRsDto.class);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution singleParent = createdJobExecutions.get(0);
    Assert.assertThat(singleParent.getSubordinationType(), is(JobExecution.SubordinationType.PARENT_SINGLE));

    singleParent.setJobProfileName("Marc jobs profile");
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(singleParent).toString())
      .when()
      .put(PUT_JOB_EXECUTION_PATH + "/" + singleParent.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(singleParent.getId()))
      .body("jobProfileName", is(singleParent.getJobProfileName()));
  }

  @Test
  public void shouldUpdateMultipleParentOnPut() {
    File file1 = new File().withName("importBib.bib");
    File file2 = new File().withName("importMarc.mrc");
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(Arrays.asList(file1, file2)).body().as(InitJobExecutionsRsDto.class);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(3));
    JobExecution multipleParent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE)).findFirst().get();

    multipleParent.setJobProfileName("Marc jobs profile");
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(multipleParent).toString())
      .when()
      .put(PUT_JOB_EXECUTION_PATH + "/" + multipleParent.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(multipleParent.getId()))
      .body("jobProfileName", is(multipleParent.getJobProfileName()));

    RestAssured.given()
      .spec(spec)
      .when()
      .when()
      .get(GET_JOB_EXECUTIONS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      // expect collection that does not contain PARENT_MULTIPLE itself
      .body("jobExecutionDtos.size()", is(createdJobExecutions.size() - 1))
      .body("jobExecutionDtos*.jobProfileName", everyItem(is(multipleParent.getJobProfileName())));
  }

  @Test
  public void shouldReturnBadRequestOnPostWhenNoDtoPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .post(POST_RAW_RECORDS_PATH + "/11dfac11-1caf-4470-9ad1-d533f6360bdd")
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  // TODO replace stub test
  @Test
  public void shouldReturnErrorOnPost() {
    RestAssured.given()
      .spec(spec)
      .body(chunk.toString())
      .when()
      .post(POST_RAW_RECORDS_PATH + "/11dfac11-1caf-4470-9ad1-d533f6360bdd")
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }

  private void assertParent(JobExecution parent) {
    Assert.assertNotNull(parent);
    Assert.assertNotNull(parent.getId());
    Assert.assertNotNull(parent.getParentJobId());
    Assert.assertTrue(parentTypes.contains(parent.getSubordinationType()));
    Assert.assertEquals(parent.getId(), parent.getParentJobId());
    Assert.assertEquals(JobExecution.Status.NEW, parent.getStatus());
    if (JobExecution.SubordinationType.PARENT_SINGLE.equals(parent.getSubordinationType())) {
      //TODO assert source path properly
      Assert.assertNotNull(parent.getSourcePath());
    }
  }

  private void assertChild(JobExecution child, String parentJobExecutionId) {
    Assert.assertNotNull(child);
    Assert.assertNotNull(child.getId());
    Assert.assertNotNull(child.getParentJobId());
    Assert.assertEquals(child.getParentJobId(), parentJobExecutionId);
    Assert.assertEquals(JobExecution.Status.NEW, child.getStatus());
    Assert.assertEquals(JobExecution.SubordinationType.CHILD, child.getSubordinationType());
    //TODO assert source path properly
    Assert.assertNotNull(child.getSourcePath());
  }

  private Response constructAndPostInitJobExecutionRqDto(List<File> files) {
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.getFiles().addAll(files);
    requestDto.setUserId(UUID.randomUUID().toString());
    return RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().post(POST_JOB_EXECUTIONS_PATH);
  }

}
