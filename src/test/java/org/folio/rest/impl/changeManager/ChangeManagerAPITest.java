package org.folio.rest.impl.changeManager;

import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfile;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.converters.Status;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;

/**
 * REST tests for ChangeManager to manager JobExecution entities initialization
 */
@RunWith(VertxUnitRunner.class)
public class ChangeManagerAPITest extends AbstractRestTest {

  private static final String POST_RAW_RECORDS_PATH = "/change-manager/records/";
  private static final String CHILDREN_PATH = "/children";
  private static final String STATUS_PATH = "/status";
  private static final String JOB_PROFILE_PATH = "/jobProfile";

  private Set<JobExecution.SubordinationType> parentTypes = EnumSet.of(
    JobExecution.SubordinationType.PARENT_SINGLE,
    JobExecution.SubordinationType.PARENT_MULTIPLE
  );

  private JobExecution jobExecution = new JobExecution()
    .withId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
    .withHrId("1000")
    .withParentJobId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
    .withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE)
    .withStatus(JobExecution.Status.NEW)
    .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
    .withSourcePath("importMarc.mrc")
    .withJobProfile(new JobProfile().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"))
    .withUserId(UUID.randomUUID().toString());

  private RawRecordsDto rawRecordsDto = new RawRecordsDto()
    .withLast(false)
    .withTotal(15)
    .withRecords(new ArrayList<>());

  @Test
  public void testInitJobExecutionsWith1File() {
    // given
    int expectedJobExecutionsNumber = 1;

    // when
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(expectedJobExecutionsNumber);

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
    int expectedParentJobExecutions = 1;
    int expectedChildJobExecutions = 2;
    int expectedJobExecutionsNumber = expectedParentJobExecutions + expectedChildJobExecutions;

    // when
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(expectedChildJobExecutions);

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
      .put(JOB_EXECUTION_PATH + UUID.randomUUID().toString())
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldReturnNotFoundOnPutWhenRecordDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobExecution).toString())
      .when()
      .put(JOB_EXECUTION_PATH + jobExecution.getId())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldUpdateSingleParentOnPut() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution singleParent = createdJobExecutions.get(0);
    Assert.assertThat(singleParent.getSubordinationType(), is(JobExecution.SubordinationType.PARENT_SINGLE));

    singleParent.setJobProfile(new JobProfile().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"));
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(singleParent).toString())
      .when()
      .put(JOB_EXECUTION_PATH + singleParent.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(singleParent.getId()))
      .body("jobProfile.name", is(singleParent.getJobProfile().getName()));
  }

  @Test
  public void shouldReturnNotFoundOnGetByIdWhenRecordDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExecution.getId())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnJobExecutionOnGetById() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(2);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(3));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + createdJobExecutions.get(0).getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(createdJobExecutions.get(0).getId()));
  }

  @Test
  public void shouldReturnNotFoundOnGetChildrenByIdWhenJobExecutionDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + UUID.randomUUID().toString() + CHILDREN_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnChildrenOfMultipleParentOnGetChildrenById() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(3);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(4));
    JobExecution multipleParent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE)).findFirst().get();

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + multipleParent.getId() + CHILDREN_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions.size()", is(createdJobExecutions.size() - 1))
      .body("totalRecords", is(createdJobExecutions.size() - 1))
      .body("jobExecutions*.subordinationType", everyItem(is(JobExecution.SubordinationType.CHILD.name())));
  }

  @Test
  public void shouldReturnEmptyCollectionOnGetChildrenByIdInCaseOfSingleParent() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId() + CHILDREN_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions", empty())
      .body("totalRecords", is(0));
  }

  @Test
  public void shouldReturnEmptyCollectionOnGetChildrenByIdInCaseOfChild() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(3);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(4));
    JobExecution child = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.CHILD)).findFirst().get();

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + child.getId() + CHILDREN_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions", empty())
      .body("totalRecords", is(0));
  }

  @Test
  public void shouldReturnNotFoundOnStatusUpdate() {
    StatusDto status = new StatusDto().withStatus(StatusDto.Status.NEW);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(status).toString())
      .when()
      .put(JOB_EXECUTION_PATH + UUID.randomUUID().toString() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnBadRequestOnStatusUpdateWhenNoEntityPassed() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .put(JOB_EXECUTION_PATH + UUID.randomUUID().toString() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldReturnBadRequestOnStatusUpdate() {
    JsonObject status = new JsonObject().put("status", "Nonsense");
    RestAssured.given()
      .spec(spec)
      .body(status.toString())
      .when()
      .put(JOB_EXECUTION_PATH + UUID.randomUUID().toString() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void shouldUpdateStatusOfSingleParent() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    StatusDto status = new StatusDto().withStatus(StatusDto.Status.IMPORT_IN_PROGRESS);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(status).toString())
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(status.getStatus().name()))
      .body("uiStatus", is(Status.valueOf(status.getStatus().name()).getUiStatus()));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(status.getStatus().name()))
      .body("uiStatus", is(Status.valueOf(status.getStatus().name()).getUiStatus()));
  }

  @Test
  public void shouldUpdateStatusOfChild() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(3);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(4));
    JobExecution child = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.CHILD)).findFirst().get();

    StatusDto status = new StatusDto().withStatus(StatusDto.Status.IMPORT_IN_PROGRESS);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(status).toString())
      .when()
      .put(JOB_EXECUTION_PATH + child.getId() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(status.getStatus().name()))
      .body("uiStatus", is(Status.valueOf(status.getStatus().name()).getUiStatus()));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + child.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(status.getStatus().name()))
      .body("uiStatus", is(Status.valueOf(status.getStatus().name()).getUiStatus()));
  }

  @Test
  public void shouldUpdateMultipleParentOnPut() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(2);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(3));
    JobExecution multipleParent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE)).findFirst().get();

    multipleParent.setJobProfile(new JobProfile().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"));
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(multipleParent).toString())
      .when()
      .put(JOB_EXECUTION_PATH + multipleParent.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(multipleParent.getId()))
      .body("jobProfile.name", is(multipleParent.getJobProfile().getName()));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + multipleParent.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobProfile.name", is(multipleParent.getJobProfile().getName()));
  }

  @Test
  public void shouldReturnNotFoundOnSetJobProfile() {
    JobProfile jobProfile = new JobProfile().withId(UUID.randomUUID().toString()).withName("marc");
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobProfile).toString())
      .when()
      .put(JOB_EXECUTION_PATH + UUID.randomUUID().toString() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnBadRequestOnSetJobProfile() {
    JobProfile jobProfile = new JobProfile().withName("Nonsense");
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobProfile).toString())
      .when()
      .put(JOB_EXECUTION_PATH + UUID.randomUUID().toString() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldSetJobProfileForJobExecution() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    JobProfile jobProfile = new JobProfile().withId(UUID.randomUUID().toString()).withName("marc");
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobProfile).toString())
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobProfile.id", is(jobProfile.getId()))
      .body("jobProfile.name", is(jobProfile.getName()));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobProfile.id", is(jobProfile.getId()))
      .body("jobProfile.name", is(jobProfile.getName()));
  }

  @Test
  public void shouldReturnBadRequestOnPostWhenNoDtoPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .post(POST_RAW_RECORDS_PATH + UUID.randomUUID().toString())
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  // TODO replace stub test
  @Test
  public void shouldReturnErrorOnPostRawRecords() {
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(rawRecordsDto).toString())
      .when()
      .post(POST_RAW_RECORDS_PATH + UUID.randomUUID().toString())
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }

  private void assertParent(JobExecution parent) {
    Assert.assertNotNull(parent);
    Assert.assertNotNull(parent.getId());
    Assert.assertNotNull(parent.getParentJobId());
    Assert.assertTrue(parentTypes.contains(parent.getSubordinationType()));
    Assert.assertEquals(parent.getId(), parent.getParentJobId());
    if (JobExecution.SubordinationType.PARENT_SINGLE.equals(parent.getSubordinationType())) {
      Assert.assertEquals(JobExecution.Status.NEW, parent.getStatus());
    } else {
      Assert.assertEquals(JobExecution.Status.PARENT, parent.getStatus());
    }
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

}
