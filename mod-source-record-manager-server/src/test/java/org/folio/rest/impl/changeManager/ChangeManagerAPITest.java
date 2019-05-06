package org.folio.rest.impl.changeManager;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.TestUtil;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.Progress;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.RunBy;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.converters.Status;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * REST tests for ChangeManager to manager JobExecution entities initialization
 */
@RunWith(VertxUnitRunner.class)
public class ChangeManagerAPITest extends AbstractRestTest {

  private static final String POST_RAW_RECORDS_PATH = "/records";
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
    .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"))
    .withUserId(UUID.randomUUID().toString());

  private RawRecordsDto rawRecordsDto = new RawRecordsDto()
    .withLast(false)
    .withCounter(15)
    .withRecords(Collections.singletonList("01240cas a2200397   450000100070000000500170000700800410002401000170006" +
      "502200140008203500260009603500220012203500110014403500190015504000440017405000150021808200110023322200420024424500" +
      "4300286260004700329265003800376300001500414310002200429321002500451362002300476570002900499650003300528650004500561" +
      "6550042006067000045006488530018006938630023007119020016007349050021007509480037007719500034008083668322014110622" +
      "1425.0750907c19509999enkqr p       0   a0eng d  a   58020553   a0022-0469  a(CStRLIN)NYCX1604275S  a(NIC)not" +
      "isABP6388  a366832  a(OCoLC)1604275  dCtYdMBTIdCtYdMBTIdNICdCStRLINdNIC0 aBR140b.J6  a270.0504aThe Jou" +
      "rnal of ecclesiastical history04aThe Journal of ecclesiastical history.  aLondon,bCambridge University Press [e" +
      "tc.]  a32 East 57th St., New York, 10022  av.b25 cm.  aQuarterly,b1970-  aSemiannual,b1950-690 av. 1-   Ap" +
      "r. 1950-  aEditor:   C. W. Dugmore. 0aChurch historyxPeriodicals. 7aChurch history2fast0(OCoLC)fst00860740 7" +
      "aPeriodicals2fast0(OCoLC)fst014116411 aDugmore, C. W.q(Clifford William),eed.0381av.i(year)4081a1-49i1950-" +
      "1998  apfndbLintz  a19890510120000.02 a20141106bmdbatcheltsxaddfast  lOLINaBR140b.J86h01/01/01 N01542ccm" +
      " a2200361   "));

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
      .when().post(JOB_EXECUTION_PATH)
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

    singleParent.setJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"));
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(singleParent).toString())
      .when()
      .put(JOB_EXECUTION_PATH + singleParent.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(singleParent.getId()))
      .body("jobProfileInfo.name", is(singleParent.getJobProfileInfo().getName()));
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
  public void shouldReturnAllChildrenOfMultipleParentOnGetChildrenById() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(25);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(26));
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
  public void shouldReturnLimitedCollectionOnGetChildrenById() {
    int limit = 15;
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(25);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(26));
    JobExecution multipleParent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE)).findFirst().get();

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + multipleParent.getId() + CHILDREN_PATH + "?limit=" + limit)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions.size()", is(limit))
      .body("totalRecords", is(createdJobExecutions.size() - 1))
      .body("jobExecutions*.subordinationType", everyItem(is(JobExecution.SubordinationType.CHILD.name())));
  }

  @Test
  public void shouldReturnFilteredCollectionOnGetChildrenById() {
    int numberOfFiles = 25;
    int expectedNumberOfNew = 12;
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(numberOfFiles);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(numberOfFiles + 1));
    JobExecution multipleParent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE)).findFirst().get();

    List<JobExecution> children = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.CHILD)).collect(Collectors.toList());
    StatusDto parsingInProgressStatus = new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS);

    for (int i = 0; i < children.size() - expectedNumberOfNew; i++) {
      updateJobExecutionStatus(children.get(i), parsingInProgressStatus)
        .then()
        .statusCode(HttpStatus.SC_OK);
    }

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + multipleParent.getId() + CHILDREN_PATH + "?query=status=" + StatusDto.Status.PARSING_IN_PROGRESS.name())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions.size()", is(children.size() - expectedNumberOfNew))
      .body("totalRecords", is(children.size() - expectedNumberOfNew))
      .body("jobExecutions*.status", everyItem(is(JobExecution.Status.PARSING_IN_PROGRESS.name())))
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

    StatusDto status = new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS);
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

    StatusDto status = new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS);
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
  public void shouldNotUpdateStatusToParent() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    StatusDto status = new StatusDto().withStatus(StatusDto.Status.PARENT);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(status).toString())
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(jobExec.getStatus().name()))
      .body("uiStatus", is(jobExec.getUiStatus().name()));
  }

  @Test
  public void shouldNotUpdateStatusOfParentMultiple() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(3);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(4));
    JobExecution parent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE)).findFirst().get();

    StatusDto status = new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(status).toString())
      .when()
      .put(JOB_EXECUTION_PATH + parent.getId() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + parent.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.PARENT.name()))
      .body("uiStatus", is(JobExecution.UiStatus.PARENT.name()));
  }

  @Test
  public void shouldUpdateMultipleParentOnPut() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(2);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(3));
    JobExecution multipleParent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE)).findFirst().get();

    multipleParent.setJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"));
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(multipleParent).toString())
      .when()
      .put(JOB_EXECUTION_PATH + multipleParent.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(multipleParent.getId()))
      .body("jobProfileInfo.name", is(multipleParent.getJobProfileInfo().getName()));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + multipleParent.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobProfileInfo.name", is(multipleParent.getJobProfileInfo().getName()));
  }

  @Test
  public void shouldNotUpdateMultipleParentStatusOnPut() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(2);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(3));
    JobExecution multipleParent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE)).findFirst().get();

    multipleParent.setStatus(JobExecution.Status.PARSING_IN_PROGRESS);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(multipleParent).toString())
      .when()
      .put(JOB_EXECUTION_PATH + multipleParent.getId())
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + multipleParent.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.PARENT.name()))
      .body("uiStatus", is(JobExecution.UiStatus.PARENT.name()));
  }

  @Test
  public void shouldNotUpdateStatusToParentOnPut() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    jobExec.setStatus(JobExecution.Status.PARENT);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobExec).toString())
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.NEW.name()))
      .body("uiStatus", is(JobExecution.UiStatus.INITIALIZATION.name()));
  }

  @Test
  public void shouldReturnNotFoundOnSetJobProfileInfo() {
    JobProfileInfo jobProfile = new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("marc");
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobProfile).toString())
      .when()
      .put(JOB_EXECUTION_PATH + UUID.randomUUID().toString() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnBadRequestOnSetJobProfileInfo() {
    JobProfileInfo jobProfile = new JobProfileInfo().withName("Nonsense");
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobProfile).toString())
      .when()
      .put(JOB_EXECUTION_PATH + UUID.randomUUID().toString() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldSetJobProfileInfoForJobExecution() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    JobProfileInfo jobProfile = new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("marc");
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobProfile).toString())
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobProfileInfo.id", is(jobProfile.getId()))
      .body("jobProfileInfo.name", is(jobProfile.getName()));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobProfileInfo.id", is(jobProfile.getId()))
      .body("jobProfileInfo.name", is(jobProfile.getName()));
  }

  @Test
  public void shouldReturnBadRequestOnPostWhenNoDtoPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .post(JOB_EXECUTION_PATH + UUID.randomUUID().toString() + POST_RAW_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldReturnErrorOnPostRawRecordsWhenJobExecutionDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto)
      .when()
      .post(JOB_EXECUTION_PATH + UUID.randomUUID().toString() + POST_RAW_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldProcessChunkOfRawRecords(TestContext testContext) {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(UUID.randomUUID().toString())
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto)
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + POST_RAW_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.PARSING_IN_PROGRESS.name()))
      .body("runBy.firstName", is("DIKU"))
      .body("progress.total", is(1000))
      .body("startedDate", notNullValue(Date.class)).log().all();
    async.complete();
  }

  @Test
  public void shouldParseChunkOfRawRecordsIfInstancesAreNotCreated() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(WireMock.post(INVENTORY_URL)
      .willReturn(WireMock.serverError()));

    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(UUID.randomUUID().toString())
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);

    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto)
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + POST_RAW_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
  }

  @Test
  public void shouldProcessLastChunkOfRawRecords() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);
    jobExec.setRunBy(new RunBy().withFirstName("DIKU").withLastName("ADMINISTRATOR"));
    jobExec.setProgress(new Progress().withCurrent(1000).withTotal(1000));
    jobExec.setStartedDate(new Date());

    RestAssured.given()
      .spec(spec)
      .body(jobExec)
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK).log().all();

    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(UUID.randomUUID().toString())
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);

    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto.withLast(true))
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + POST_RAW_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.PARSING_FINISHED.name()));
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

  @Test
  public void shouldUpdateSingleParentOnPutWhenSnapshotUpdateFailed() {
    WireMock.stubFor(WireMock.put(new UrlPathPattern(new RegexPattern(SNAPSHOT_SERVICE_URL + "/.*"), true))
      .willReturn(WireMock.serverError()));

    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution singleParent = createdJobExecutions.get(0);
    Assert.assertThat(singleParent.getSubordinationType(), is(JobExecution.SubordinationType.PARENT_SINGLE));

    singleParent.setJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"));
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(singleParent).toString())
      .when()
      .put(JOB_EXECUTION_PATH + singleParent.getId())
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }


  @Test
  public void shouldReturnErrorOnPostChunkOfRawRecordsWhenFailedPostRecordsToRecordsStorage() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(UUID.randomUUID().toString())
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);

    WireMock.stubFor(WireMock.post(RECORDS_SERVICE_URL)
      .willReturn(WireMock.serverError()));

    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto)
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + POST_RAW_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.ERROR.name()));
  }

  @Test
  public void shouldReturnErrorOnPostJobExecutionWhenFailedPostSnapshotToStorage() throws IOException {
    WireMock.stubFor(WireMock.post(SNAPSHOT_SERVICE_URL)
      .willReturn(WireMock.serverError()));

    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    String jsonFiles = null;
    List<File> filesList = null;
    jsonFiles = TestUtil.readFileFromPath(FILES_PATH);
    filesList = new ObjectMapper().readValue(jsonFiles, new TypeReference<List<File>>() {
    });

    requestDto.getFiles().addAll(filesList.stream().limit(1).collect(Collectors.toList()));
    requestDto.setUserId(UUID.randomUUID().toString());
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when()
      .post(JOB_EXECUTION_PATH)
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }

}
