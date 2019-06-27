package org.folio.rest.impl.changeManager;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.TestUtil;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.*;
import org.folio.services.converters.Status;
import org.hamcrest.text.MatchesPattern;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static java.util.Arrays.asList;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;

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
      " a2200361   ")
    ).withContentType(RawRecordsDto.ContentType.MARC_RAW);

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
    assertEquals(expectedJobExecutionsNumber, actualJobExecutions.size());

    JobExecution parentSingle = actualJobExecutions.get(0);
    assertEquals(JobExecution.SubordinationType.PARENT_SINGLE, parentSingle.getSubordinationType());
    assertParent(parentSingle);
  }

  @Test
  public void testInitJobExecutionsWithJobProfile() {
    // given
    int expectedJobExecutionsNumber = 1;

    // when
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.setUserId(UUID.randomUUID().toString());
    requestDto.setSourceType(InitJobExecutionsRqDto.SourceType.ONLINE);
    requestDto.setJobProfileInfo(new JobProfileInfo()
      .withId(UUID.randomUUID().toString())
      .withDataType(JobProfileInfo.DataType.MARC)
      .withName("Test Profile"));

    InitJobExecutionsRsDto response = RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().log().all()
      .post(JOB_EXECUTION_PATH).body().as(InitJobExecutionsRsDto.class);

    // then
    String actualParentJobExecutionId = response.getParentJobExecutionId();
    List<JobExecution> actualJobExecutions = response.getJobExecutions();

    Assert.assertNotNull(actualParentJobExecutionId);
    assertEquals(expectedJobExecutionsNumber, actualJobExecutions.size());

    JobExecution parentSingle = actualJobExecutions.get(0);
    Assert.assertNotNull(parentSingle);
    assertEquals(JobExecution.SubordinationType.PARENT_SINGLE, parentSingle.getSubordinationType());
    Assert.assertNotNull(parentSingle.getId());
    Assert.assertNotNull(parentSingle.getParentJobId());
    Assert.assertTrue(parentTypes.contains(parentSingle.getSubordinationType()));
    assertEquals(parentSingle.getId(), parentSingle.getParentJobId());
    assertEquals(JobExecution.Status.NEW, parentSingle.getStatus());
    Assert.assertNotNull(parentSingle.getJobProfileInfo());
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
    assertEquals(expectedJobExecutionsNumber, actualJobExecutions.size());

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

    assertEquals(expectedParentJobExecutions, actualParentJobExecutions);
    assertEquals(expectedChildJobExecutions, actualChildJobExecutions);
  }

  @Test
  public void testInitJobExecutionsWithNoFiles(TestContext context) {
    // given
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.setUserId(UUID.randomUUID().toString());
    requestDto.setSourceType(InitJobExecutionsRqDto.SourceType.FILES);

    // when
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().post(JOB_EXECUTION_PATH)
      .then().statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void testInitJobExecutionsWithNoProfile(TestContext context) {
    // given
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.setUserId(UUID.randomUUID().toString());
    requestDto.setSourceType(InitJobExecutionsRqDto.SourceType.ONLINE);

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
      .body("id", is(createdJobExecutions.get(0).getId()))
      .body("hrId", MatchesPattern.matchesPattern("\\d+"));
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

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

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

    WireMock.stubFor(post(INVENTORY_URL)
      .willReturn(WireMock.serverError()));
    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

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
  public void shouldProcessChunkOfRawRecordsIfAddingAdditionalFieldsWasFail(TestContext testContext) {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(WireMock.put(PARSED_RECORDS_COLLECTION_URL)
      .willReturn(WireMock.serverError()));

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

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
  public void shouldNotParseChunkOfRawRecordsIfRecordListEmpty() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(INVENTORY_URL)
      .willReturn(WireMock.serverError()));

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

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
      .body(new RawRecordsDto().withContentType(RawRecordsDto.ContentType.MARC_RAW).withLast(false).withCounter(1))
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + POST_RAW_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
  }


  @Test
  public void shouldProcessLastChunkOfRawRecords(TestContext testContext) {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);
    jobExec.setRunBy(new RunBy().withFirstName("DIKU").withLastName("ADMINISTRATOR"));
    jobExec.setProgress(new Progress().withCurrent(1000).withTotal(1000));
    jobExec.setStartedDate(new Date());

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    WireMock.stubFor(post(INVENTORY_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(jobExec)
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK).log().all();
    async.complete();

    async = testContext.async();
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
      .body(rawRecordsDto.withLast(true))
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
      // status should be JobExecution.Status.PARSING_FINISHED but for first version we finish import in this place
      .body("status", is(JobExecution.Status.COMMITTED.name()));
    async.complete();
  }

  private void assertParent(JobExecution parent) {
    Assert.assertNotNull(parent);
    Assert.assertNotNull(parent.getId());
    Assert.assertNotNull(parent.getParentJobId());
    Assert.assertTrue(parentTypes.contains(parent.getSubordinationType()));
    assertEquals(parent.getId(), parent.getParentJobId());
    if (JobExecution.SubordinationType.PARENT_SINGLE.equals(parent.getSubordinationType())) {
      assertEquals(JobExecution.Status.NEW, parent.getStatus());
    } else {
      assertEquals(JobExecution.Status.PARENT, parent.getStatus());
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
    assertEquals(child.getParentJobId(), parentJobExecutionId);
    assertEquals(JobExecution.Status.NEW, child.getStatus());
    assertEquals(JobExecution.SubordinationType.CHILD, child.getSubordinationType());
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

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
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
  public void shouldReturnSavedParsedRecordsOnPostChunkOfRawRecordsWhenRecordsSavedPartially(TestContext testContext) {
    RawRecordsDto rawRecordsDto = new RawRecordsDto()
      .withLast(false)
      .withCounter(15)
      .withRecords(asList("01240cas a2200397   450000100070000000500170000700800410002401000170006" +
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
          " a2200361   ",
        "01240cas a2200397   450000100070000000500170000700800410002401000170006" +
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
          " a2200361   ")
      ).withContentType(RawRecordsDto.ContentType.MARC_RAW);
    Async async = testContext.async();
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    async.complete();
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(RECORDS_SERVICE_URL).willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));
    WireMock.stubFor(post(INVENTORY_URL)
      .willReturn(serverError()//simulates partial success
        .withHeader(CONTENT_TYPE, APPLICATION_JSON)
        .withTransformers(InstancesBatchResponseTransformer.NAME)
      )
    );

    async = testContext.async();
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
      .body("status", is(JobExecution.Status.PARSING_IN_PROGRESS.name()));
    async.complete();

    List<LoggedRequest> requests = findAll(putRequestedFor(urlMatching(PARSED_RECORDS_COLLECTION_URL)));

    assertEquals(1, requests.size());
    String body = requests.get(0).getBodyAsString();
    assertEquals(1, new JsonObject(body).getJsonArray("parsedRecords").size());
  }

  @Test
  public void shouldReturnErrorOnPostJobExecutionWhenFailedPostSnapshotToStorage() throws IOException {
    WireMock.stubFor(post(SNAPSHOT_SERVICE_URL)
      .willReturn(WireMock.serverError()));

    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    String jsonFiles = null;
    List<File> filesList = null;
    jsonFiles = TestUtil.readFileFromPath(FILES_PATH);
    filesList = new ObjectMapper().readValue(jsonFiles, new TypeReference<List<File>>() {
    });

    requestDto.getFiles().addAll(filesList.stream().limit(1).collect(Collectors.toList()));
    requestDto.setUserId(UUID.randomUUID().toString());
    requestDto.setSourceType(InitJobExecutionsRqDto.SourceType.FILES);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when()
      .post(JOB_EXECUTION_PATH)
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }

  @Test
  public void shouldProcessErrorRawRecords(TestContext testContext) {
    RawRecordsDto rawRecordsDtoContainingError = new RawRecordsDto()
      .withLast(true)
      .withCounter(15)
      .withRecords(asList(
        "01314nam  22003851a 4500001001100000003000800011005001700019006001800036007001500054008004100069020003200110020003500142040002100177050002000198082001500218100002000233245008900253250001200342260004900354300002300403490002400426500002400450504006200474505009200536650003200628650001400660700002500674710001400699776004000713830001800753856009400771935001500865980003400880981001400914\u001Eybp7406411\u001ENhCcYBP\u001E20120404100627.6\u001Em||||||||d|||||||\u001Ecr||n|||||||||\u001E120329s2011    sz a    ob    001 0 eng d\u001E  \u001Fa2940447241 (electronic bk.)\u001E  \u001Fa9782940447244 (electronic bk.)\u001E  \u001FaNhCcYBP\u001FcNhCcYBP\u001E 4\u001FaZ246\u001Fb.A43 2011\u001E04\u001Fa686.22\u001F222\u001E1 \u001FaAmbrose, Gavin.\u001E14\u001FaThe fundamentals of typography\u001Fh[electronic resource] /\u001FcGavin Ambrose, Paul Harris.\u001E  \u001Fa2nd ed.\u001E  \u001FaLausanne ;\u001FaWorthing :\u001FbAVA Academia,\u001Fc2011.\u001E  \u001Fa1 online resource.\u001E1 \u001FaAVA Academia series\u001E  \u001FaPrevious ed.: 2006.\u001E  \u001FaIncludes bibliographical references (p. [200]) and index.\u001E0 \u001FaType and language -- A few basics -- Letterforms -- Words and paragraphs -- Using type.\u001E 0\u001FaGraphic design (Typography)\u001E 0\u001FaPrinting.\u001E1 \u001FaHarris, Paul,\u001Fd1971-\u001E2 \u001FaEBSCOhost\u001E  \u001FcOriginal\u001Fz9782940411764\u001Fz294041176X\u001E 0\u001FaAVA academia.\u001E40\u001Fuhttp://search.ebscohost.com/login.aspx?direct=true&scope=site&db=nlebk&db=nlabk&AN=430135\u001E  \u001Fa.o13465259\u001E  \u001Fa130307\u001Fb7107\u001Fe7107\u001Ff243965\u001Fg1\u001E  \u001FbOM\u001Fcnlnet\u001E\u001D\n",
        "01247nam  2200313zu 450000100110000000300080001100500170001905\u001F222\u001E1 \u001FaAriáes, Philippe.\u001E10\u001FaWestern attitudes toward death\u001Fh[electronic resource] :\u001Fbfrom the Middle Ages to the present /\u001Fcby Philippe Ariáes ; translated by Patricia M. Ranum.\u001E  \u001FaJohn Hopkins Paperbacks ed.\u001E  \u001FaBaltimore :\u001FbJohns Hopkins University Press,\u001Fc1975.\u001E  \u001Fa1 online resource.\u001E1 \u001FaThe Johns Hopkins symposia in comparative history ;\u001Fv4th\u001E  \u001FaDescription based on online resource; title from digital title page (viewed on Mar. 7, 2013).\u001E 0\u001FaDeath.\u001E2 \u001FaEbrary.\u001E 0\u001FaJohns Hopkins symposia in comparative history ;\u001Fv4th.\u001E40\u001FzConnect to e-book on Ebrary\u001Fuhttp://gateway.library.qut.edu.au/login?url=http://site.ebrary.com/lib/qut/docDetail.action?docID=10635130\u001E  \u001Fa.o1346565x\u001E  \u001Fa130307\u001Fb2095\u001Fe2095\u001Ff243966\u001Fg1\u001E  \u001FbOM\u001Fcnlnet\u001E\u001D\n",
        "03401nam  22004091i 4500001001200000003000800012005001700020006001800037007001500055008004100070020003200111020003500143020004300178020004000221040002100261050002700282082001900309245016200328300002300490500004700513504005100560505178700611588004702398650003702445650004502482650002902527650003602556700005502592700006102647710002302708730001902731776005902750856011902809935001502928980003402943981001402977\u001Eybp10134220\u001ENhCcYBP\u001E20130220102526.4\u001Em||||||||d|||||||\u001Ecr||n|||||||||\u001E130220s2013    ncu     ob    001 0 eng d\u001E  \u001Fa1476601852 (electronic bk.)\u001E  \u001Fa9781476601854 (electronic bk.)\u001E  \u001Fz9780786471140 (softcover : alk. paper)\u001E  \u001Fz078647114X (softcover : alk. paper)\u001E  \u001FaNhCcYBP\u001FcNhCcYBP\u001E 4\u001FaPN1995.9.V46\u001FbG37 2013\u001E04\u001Fa791.43/656\u001F223\u001E00\u001FaGame on, Hollywood!\u001Fh[electronic resource] :\u001Fbessays on the intersection of video games and cinema /\u001Fcedited by Gretchen Papazian and Joseph Michael Sommers.\u001E  \u001Fa1 online resource.\u001E  \u001FaDescription based on print version record.\u001E  \u001FaIncludes bibliographical references and index.\u001E0 \u001FaIntroduction: manifest narrativity-video games, movies, and art and adaptation / Gretchen Papazian and Joseph Michael Sommers -- The rules of engagement: watching, playing and other narrative processes. Playing the Buffyverse, playing the gothic: genre, gender and cross-media interactivity in Buffy the vampire slayer: chaos bleeds / Katrin Althans -- Dead eye: the spectacle of torture porn in Dead rising / Deborah Mellamphy -- Playing (with) the western: classical Hollywood genres in modern video games / Jason W. Buel -- Game-to-film adaptation and how Prince of Persia: the sands of time negotiates the difference between player and audience / Ben S. Bunting, Jr -- Translation between forms of interactivity: how to build the better adaptation / Marcus Schulzke -- The terms of the tale: time, place and other ideologically constructed conditions. -- Playing (in) the city: the warriors and images of urban disorder / Aubrey Anable -- When did Dante become a scythe-wielding badass? modeling adaption and shifting gender convention in Dante's Inferno / Denise A. Ayo -- \"Some of this happened to the other fellow\": remaking Goldeneye with Daniel Craig / David McGowan -- Zombie stripper geishas in the new global economy: racism and sexism in video games / Stewart Chang -- Stories, stories everywhere (and nowhere just the same): transmedia texts. \"My name is Alan Wake. I'm a writer.\": crafting narrative complexity in the age of transmedia storytelling / Michael Fuchs -- Millions of voices: Star wars, digital games, fictional worlds and franchise canon / Felan Parker -- The hype man as racial stereotype, parody and ghost in Afro samurai / Treaandrea M. Russworm -- Epic nostalgia: narrative play and transmedia storytelling in Disney epic Mickey / Lisa K. Dusenberry.\u001E  \u001FaDescription based on print version record.\u001E 0\u001FaMotion pictures and video games.\u001E 0\u001FaFilm adaptations\u001FxHistory and criticism.\u001E 0\u001FaVideo games\u001FxAuthorship.\u001E 0\u001FaConvergence (Telecommunication)\u001E1 \u001FaPapazian, Gretchen,\u001Fd1968-\u001Feeditor of compilation.\u001E1 \u001FaSommers, Joseph Michael,\u001Fd1976-\u001Feeditor of  compilation.\u001E2 \u001FaEbooks Corporation\u001E0 \u001FaEbook Library.\u001E08\u001FcOriginal\u001Fz9780786471140\u001Fz078647114X\u001Fw(DLC)  2012051432\u001E40\u001FzConnect to e-book on Ebook Library\u001Fuhttp://qut.eblib.com.au.AU/EBLWeb/patron/?target=patron&extendedid=P_1126326_0\u001E  \u001Fa.o13465405\u001E  \u001Fa130307\u001Fb6000\u001Fe6000\u001Ff243967\u001Fg1\u001E  \u001FbOM\u001Fcnlnet\u001E\u001D"
        )
      ).withContentType(RawRecordsDto.ContentType.MARC_RAW);

    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);
    jobExec.setRunBy(new RunBy().withFirstName("DIKU").withLastName("ADMINISTRATOR"));
    jobExec.setProgress(new Progress().withCurrent(1000).withTotal(1000));
    jobExec.setStartedDate(new Date());

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    WireMock.stubFor(post(INVENTORY_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(jobExec)
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK).log().all();
    async.complete();

    async = testContext.async();
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
      .body(rawRecordsDtoContainingError)
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
      // status should be JobExecution.Status.PARSING_FINISHED but for first version we finish import in this place
      .body("status", is(JobExecution.Status.COMMITTED.name()));
    async.complete();
  }

  //todo some times it's failed. When this test is run alone.
  @Test
  public void shouldMarkJobExecutionAsErrorIfInstancesWereNotCreated(TestContext testContext) {
    RawRecordsDto rawRecordsDtoContainingError = new RawRecordsDto()
      .withLast(true)
      .withCounter(15)
      .withRecords(Arrays.asList(
        "01314nam  22003851a 4500001001100000003000800011005001700019006001800036007001500054008004100069020003200110020003500142040002100177050002000198082001500218100002000233245008900253250001200342260004900354300002300403490002400426500002400450504006200474505009200536650003200628650001400660700002500674710001400699776004000713830001800753856009400771935001500865980003400880981001400914\u001Eybp7406411\u001ENhCcYBP\u001E20120404100627.6\u001Em||||||||d|||||||\u001Ecr||n|||||||||\u001E120329s2011    sz a    ob    001 0 eng d\u001E  \u001Fa2940447241 (electronic bk.)\u001E  \u001Fa9782940447244 (electronic bk.)\u001E  \u001FaNhCcYBP\u001FcNhCcYBP\u001E 4\u001FaZ246\u001Fb.A43 2011\u001E04\u001Fa686.22\u001F222\u001E1 \u001FaAmbrose, Gavin.\u001E14\u001FaThe fundamentals of typography\u001Fh[electronic resource] /\u001FcGavin Ambrose, Paul Harris.\u001E  \u001Fa2nd ed.\u001E  \u001FaLausanne ;\u001FaWorthing :\u001FbAVA Academia,\u001Fc2011.\u001E  \u001Fa1 online resource.\u001E1 \u001FaAVA Academia series\u001E  \u001FaPrevious ed.: 2006.\u001E  \u001FaIncludes bibliographical references (p. [200]) and index.\u001E0 \u001FaType and language -- A few basics -- Letterforms -- Words and paragraphs -- Using type.\u001E 0\u001FaGraphic design (Typography)\u001E 0\u001FaPrinting.\u001E1 \u001FaHarris, Paul,\u001Fd1971-\u001E2 \u001FaEBSCOhost\u001E  \u001FcOriginal\u001Fz9782940411764\u001Fz294041176X\u001E 0\u001FaAVA academia.\u001E40\u001Fuhttp://search.ebscohost.com/login.aspx?direct=true&scope=site&db=nlebk&db=nlabk&AN=430135\u001E  \u001Fa.o13465259\u001E  \u001Fa130307\u001Fb7107\u001Fe7107\u001Ff243965\u001Fg1\u001E  \u001FbOM\u001Fcnlnet\u001E\u001D\n",
        "01247nam  2200313zu 450000100110000000300080001100500170001905\u001F222\u001E1 \u001FaAriáes, Philippe.\u001E10\u001FaWestern attitudes toward death\u001Fh[electronic resource] :\u001Fbfrom the Middle Ages to the present /\u001Fcby Philippe Ariáes ; translated by Patricia M. Ranum.\u001E  \u001FaJohn Hopkins Paperbacks ed.\u001E  \u001FaBaltimore :\u001FbJohns Hopkins University Press,\u001Fc1975.\u001E  \u001Fa1 online resource.\u001E1 \u001FaThe Johns Hopkins symposia in comparative history ;\u001Fv4th\u001E  \u001FaDescription based on online resource; title from digital title page (viewed on Mar. 7, 2013).\u001E 0\u001FaDeath.\u001E2 \u001FaEbrary.\u001E 0\u001FaJohns Hopkins symposia in comparative history ;\u001Fv4th.\u001E40\u001FzConnect to e-book on Ebrary\u001Fuhttp://gateway.library.qut.edu.au/login?url=http://site.ebrary.com/lib/qut/docDetail.action?docID=10635130\u001E  \u001Fa.o1346565x\u001E  \u001Fa130307\u001Fb2095\u001Fe2095\u001Ff243966\u001Fg1\u001E  \u001FbOM\u001Fcnlnet\u001E\u001D\n",
        "03401nam  22004091i 4500001001200000003000800012005001700020006001800037007001500055008004100070020003200111020003500143020004300178020004000221040002100261050002700282082001900309245016200328300002300490500004700513504005100560505178700611588004702398650003702445650004502482650002902527650003602556700005502592700006102647710002302708730001902731776005902750856011902809935001502928980003402943981001402977\u001Eybp10134220\u001ENhCcYBP\u001E20130220102526.4\u001Em||||||||d|||||||\u001Ecr||n|||||||||\u001E130220s2013    ncu     ob    001 0 eng d\u001E  \u001Fa1476601852 (electronic bk.)\u001E  \u001Fa9781476601854 (electronic bk.)\u001E  \u001Fz9780786471140 (softcover : alk. paper)\u001E  \u001Fz078647114X (softcover : alk. paper)\u001E  \u001FaNhCcYBP\u001FcNhCcYBP\u001E 4\u001FaPN1995.9.V46\u001FbG37 2013\u001E04\u001Fa791.43/656\u001F223\u001E00\u001FaGame on, Hollywood!\u001Fh[electronic resource] :\u001Fbessays on the intersection of video games and cinema /\u001Fcedited by Gretchen Papazian and Joseph Michael Sommers.\u001E  \u001Fa1 online resource.\u001E  \u001FaDescription based on print version record.\u001E  \u001FaIncludes bibliographical references and index.\u001E0 \u001FaIntroduction: manifest narrativity-video games, movies, and art and adaptation / Gretchen Papazian and Joseph Michael Sommers -- The rules of engagement: watching, playing and other narrative processes. Playing the Buffyverse, playing the gothic: genre, gender and cross-media interactivity in Buffy the vampire slayer: chaos bleeds / Katrin Althans -- Dead eye: the spectacle of torture porn in Dead rising / Deborah Mellamphy -- Playing (with) the western: classical Hollywood genres in modern video games / Jason W. Buel -- Game-to-film adaptation and how Prince of Persia: the sands of time negotiates the difference between player and audience / Ben S. Bunting, Jr -- Translation between forms of interactivity: how to build the better adaptation / Marcus Schulzke -- The terms of the tale: time, place and other ideologically constructed conditions. -- Playing (in) the city: the warriors and images of urban disorder / Aubrey Anable -- When did Dante become a scythe-wielding badass? modeling adaption and shifting gender convention in Dante's Inferno / Denise A. Ayo -- \"Some of this happened to the other fellow\": remaking Goldeneye with Daniel Craig / David McGowan -- Zombie stripper geishas in the new global economy: racism and sexism in video games / Stewart Chang -- Stories, stories everywhere (and nowhere just the same): transmedia texts. \"My name is Alan Wake. I'm a writer.\": crafting narrative complexity in the age of transmedia storytelling / Michael Fuchs -- Millions of voices: Star wars, digital games, fictional worlds and franchise canon / Felan Parker -- The hype man as racial stereotype, parody and ghost in Afro samurai / Treaandrea M. Russworm -- Epic nostalgia: narrative play and transmedia storytelling in Disney epic Mickey / Lisa K. Dusenberry.\u001E  \u001FaDescription based on print version record.\u001E 0\u001FaMotion pictures and video games.\u001E 0\u001FaFilm adaptations\u001FxHistory and criticism.\u001E 0\u001FaVideo games\u001FxAuthorship.\u001E 0\u001FaConvergence (Telecommunication)\u001E1 \u001FaPapazian, Gretchen,\u001Fd1968-\u001Feeditor of compilation.\u001E1 \u001FaSommers, Joseph Michael,\u001Fd1976-\u001Feeditor of  compilation.\u001E2 \u001FaEbooks Corporation\u001E0 \u001FaEbook Library.\u001E08\u001FcOriginal\u001Fz9780786471140\u001Fz078647114X\u001Fw(DLC)  2012051432\u001E40\u001FzConnect to e-book on Ebook Library\u001Fuhttp://qut.eblib.com.au.AU/EBLWeb/patron/?target=patron&extendedid=P_1126326_0\u001E  \u001Fa.o13465405\u001E  \u001Fa130307\u001Fb6000\u001Fe6000\u001Ff243967\u001Fg1\u001E  \u001FbOM\u001Fcnlnet\u001E\u001D"
        )
      ).withContentType(RawRecordsDto.ContentType.MARC_RAW);

    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);
    jobExec.setRunBy(new RunBy().withFirstName("DIKU").withLastName("ADMINISTRATOR"));
    jobExec.setProgress(new Progress().withCurrent(1000).withTotal(1000));
    jobExec.setStartedDate(new Date());

    WireMock.stubFor(post(INVENTORY_URL)
      .willReturn(WireMock.serverError()));

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(jobExec)
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK).log().all();
    async.complete();

    async = testContext.async();
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
      .body(rawRecordsDtoContainingError)
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
      .body("status", is(JobExecution.Status.ERROR.name()))
      .body("completedDate", notNullValue(Date.class));
    async.complete();
  }

  @Test
  public void shouldMarkJobExecutionAsErrorAndSetCompletedDateWhenFailedPostRecordsToRecordsStorage() {
    RawRecordsDto lastRawRecordsDto = new RawRecordsDto()
      .withLast(true)
      .withCounter(rawRecordsDto.getCounter())
      .withRecords(rawRecordsDto.getRecords())
      .withContentType(rawRecordsDto.getContentType());

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
      .body(lastRawRecordsDto)
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
      .body("status", is(JobExecution.Status.ERROR.name()))
      .body("completedDate", notNullValue(Date.class));
  }

}
