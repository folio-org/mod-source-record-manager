package org.folio.rest.impl.metadataProvider;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.LogCollectionDto;
import org.folio.rest.jaxrs.model.LogDto;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.created;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static org.folio.rest.jaxrs.model.JobExecution.Status.COMMITTED;
import static org.folio.rest.jaxrs.model.JobExecution.Status.ERROR;
import static org.folio.rest.jaxrs.model.JobExecution.SubordinationType.CHILD;
import static org.folio.rest.jaxrs.model.JobExecution.SubordinationType.PARENT_SINGLE;
import static org.folio.rest.jaxrs.model.JobProfileInfo.DataType.MARC;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;

/**
 * REST tests for MetadataProvider to manager Log entities
 */
@RunWith(VertxUnitRunner.class)
public class MetadataProviderLogAPITest extends AbstractRestTest {

  private static final String GET_LOGS_PATH_LANDING_PAGE_FALSE = "/metadata-provider/logs?landingPage=false";
  private static final String GET_LOGS_PATH_LANDING_PAGE_TRUE = "/metadata-provider/logs?landingPage=true";
  private static final String PUT_JOB_EXECUTIONS_PATH = "/change-manager/jobExecutions/";
  private static final String PROFILE_NAME = "Parse Marc files profile";
  private static final int LANDING_PAGE_LOGS_LIMIT = 25;
  private static final String RAW_RECORD = "01240cas a2200397   450000100070000000500170000700800410002401000170006502200140008203500260009603500220012203500110014403500190015504000440017405000150021808200110023322200420024424500430028626000470032926500380037630000150041431000220042932100250045136200230047657000290049965000330052865000450056165500420060670000450064885300180069386300230071190200160073490500210075094800370077195000340080836683220141106221425.0750907c19509999enkqr p       0   a0eng d  a   58020553   a0022-0469  a(CStRLIN)NYCX1604275S  a(NIC)notisABP6388  a366832  a(OCoLC)1604275  dCtYdMBTIdCtYdMBTIdNICdCStRLINdNIC0 aBR140b.J6  a270.0504aThe Journal of ecclesiastical history04aThe Journal of ecclesiastical history.  aLondon,bCambridge University Press [etc.]  a32 East 57th St., New York, 10022  av.b25 cm.  aQuarterly,b1970-  aSemiannual,b1950-690 av. 1-   Apr. 1950-  aEditor:   C. W. Dugmore. 0aChurch historyxPeriodicals. 7aChurch history2fast0(OCoLC)fst00860740 7aPeriodicals2fast0(OCoLC)fst014116411 aDugmore, C. W.q(Clifford William),eed.0381av.i(year)4081a1-49i1950-1998  apfndbLintz  a19890510120000.02 a20141106bmdbatcheltsxaddfast  lOLINaBR140b.J86h01/01/01 N01542ccm a2200361   ";

  @Test
  public void shouldReturnEmptyListOnGetIfNoLogsExist() {
    int expectedLogNumber = 0;
    int expectedTotalRecords = 0;

    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_FALSE)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("logDtos.size()", is(expectedLogNumber))
      .body("totalRecords", is(expectedTotalRecords));
  }

  @Test
  public void shouldReturnEmptyListOnGetIfCreated1ParentMultiple2Child() {
    int actualFilesNumber = 2;
    int expectedLogNumber = 0;
    int expectedTotalRecords = 0;

    constructAndPostInitJobExecutionRqDto(actualFilesNumber);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_FALSE)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("logDtos.size()", is(expectedLogNumber))
      .body("totalRecords", is(expectedTotalRecords));
  }

  @Test
  public void shouldReturnEmptyListOnGetIfCreated1ParentSingle() {
    int actualFilesNumber = 1;
    int expectedLogNumber = 0;
    int expectedTotalRecords = 0;

    constructAndPostInitJobExecutionRqDto(actualFilesNumber);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_FALSE)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("logDtos.size()", is(expectedLogNumber))
      .body("totalRecords", is(expectedTotalRecords));
  }

  @Test
  public void should1LogOnGetIfCreated1ParentSingleCommitted() {
    int actualFilesNumber = 1;
    int expectedLogNumber = 1;
    int expectedTotalRecords = 1;

    List<JobExecution> createdJobExecutions = constructAndPostInitJobExecutionRqDto(actualFilesNumber).getJobExecutions();

    JobExecution expectedCommittedChild = createdJobExecutions.get(0);
    Assert.assertEquals(PARENT_SINGLE, expectedCommittedChild.getSubordinationType());
    expectedCommittedChild.setStatus(COMMITTED);
    expectedCommittedChild.setJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName(PROFILE_NAME));

    putJobExecution(expectedCommittedChild)
      .then()
      .statusCode(HttpStatus.SC_OK);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_FALSE)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("logDtos.size()", is(expectedLogNumber))
      .body("totalRecords", is(expectedTotalRecords));
  }

  @Test
  public void shouldReturn1LogOnGetIfCreated1ParentMultiple1ChildNew1ChildCommitted() {
    int actualFilesNumber = 2;
    int expectedLogNumber = 1;
    int expectedTotalRecords = 1;

    List<JobExecution> createdJobExecutions = constructAndPostInitJobExecutionRqDto(actualFilesNumber).getJobExecutions();

    JobExecution expectedCommittedChild = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(CHILD)).findFirst().get();

    expectedCommittedChild.setStatus(COMMITTED);
    expectedCommittedChild.setJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName(PROFILE_NAME));

    putJobExecution(expectedCommittedChild)
      .then()
      .statusCode(HttpStatus.SC_OK);

    LogCollectionDto logs = RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_FALSE)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(LogCollectionDto.class);

    Assert.assertEquals(expectedLogNumber, logs.getLogDtos().size());
    Assert.assertEquals(expectedCommittedChild.getId(), logs.getLogDtos().get(0).getJobExecutionId());
    Assert.assertEquals(expectedTotalRecords, logs.getTotalRecords().intValue());
  }

  @Test
  public void shouldReturn2LogsOnGetIfCreated1ParentMultiple2ChildCommitted() {
    int actualFilesNumber = 2;
    int expectedLogNumber = 2;
    int expectedTotalRecords = 2;

    List<JobExecution> createdJobExecutions = constructAndPostInitJobExecutionRqDto(actualFilesNumber).getJobExecutions();

    List<JobExecution> expectedCommittedChildren = new ArrayList<>();
    for (JobExecution createdJobExecution : createdJobExecutions) {
      if (CHILD.equals(createdJobExecution.getSubordinationType())) {
        createdJobExecution.setStatus(COMMITTED);
        createdJobExecution.setJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName(PROFILE_NAME));
        expectedCommittedChildren.add(createdJobExecution);
      }
    }

    for (JobExecution child : expectedCommittedChildren) {
      putJobExecution(child)
        .then()
        .statusCode(HttpStatus.SC_OK);
    }

    LogCollectionDto logCollectionDto = RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_FALSE)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(LogCollectionDto.class);

    Assert.assertEquals(expectedLogNumber, logCollectionDto.getLogDtos().size());
    Assert.assertEquals(expectedTotalRecords, logCollectionDto.getTotalRecords().intValue());

    for (JobExecution childExpectedCommittedJoExec : expectedCommittedChildren) {
      LogDto log = logCollectionDto.getLogDtos().stream()
        .filter(logDto -> childExpectedCommittedJoExec.getId().equals(logDto.getJobExecutionId()))
        .findAny()
        .orElse(null);
      Assert.assertNotNull(log);
    }
  }

  @Test
  public void shouldReturnLimitedListOnGetIfCreated1ParentMultiple3ChildCommittedWithLimit() {
    int actualLimit = 2;
    int expectedLogNumber = 2;
    // We do not expect PARENT entity in total records
    int expectedTotalRecords = 3;
    int actualFilesNumber = 3;

    List<JobExecution> createdJobExecutions = constructAndPostInitJobExecutionRqDto(actualFilesNumber).getJobExecutions();

    List<JobExecution> expectedCommittedChildren = new ArrayList<>();
    for (JobExecution createdJobExecution : createdJobExecutions) {
      if (CHILD.equals(createdJobExecution.getSubordinationType())) {
        createdJobExecution.setStatus(COMMITTED);
        createdJobExecution.setJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName(PROFILE_NAME));
        expectedCommittedChildren.add(createdJobExecution);
      }
    }

    for (JobExecution child : expectedCommittedChildren) {
      putJobExecution(child)
        .then()
        .statusCode(HttpStatus.SC_OK);
    }

    LogCollectionDto logCollectionDto = RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_FALSE + "&limit=" + actualLimit)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(LogCollectionDto.class);

    Assert.assertEquals(expectedLogNumber, logCollectionDto.getLogDtos().size());
    Assert.assertEquals(expectedTotalRecords, logCollectionDto.getTotalRecords().intValue());
  }

  @Test
  public void shouldReturnSortedListOnGetIfCreated1ParentMultiple3ChildCommitted() {
    int actualFilesNumber = 4;

    List<JobExecution> createdJobExecutions = constructAndPostInitJobExecutionRqDto(actualFilesNumber).getJobExecutions();

    List<JobExecution> expectedCommittedChildren = new ArrayList<>();
    for (int i = 0; i < createdJobExecutions.size(); i++) {
      JobExecution createdJobExecution = createdJobExecutions.get(i);
      if (CHILD.equals(createdJobExecution.getSubordinationType())) {
        createdJobExecution.setStatus(COMMITTED);
        createdJobExecution.setJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName(PROFILE_NAME));
        createdJobExecution.setCompletedDate(new Date(1542714612000L + i));
        expectedCommittedChildren.add(createdJobExecution);
      }
    }

    for (JobExecution child : expectedCommittedChildren) {
      putJobExecution(child)
        .then()
        .statusCode(HttpStatus.SC_OK);
    }

    LogCollectionDto logCollectionDto = RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_FALSE + "&query=cql.allRecords=1 sortBy completedDate/sort.descending")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(LogCollectionDto.class);

    List<LogDto> logsList = logCollectionDto.getLogDtos();
    Assert.assertEquals(createdJobExecutions.size() - 1, logsList.size());
    Assert.assertTrue(logsList.get(0).getCompletedDate().after(logsList.get(1).getCompletedDate()));
    Assert.assertTrue(logsList.get(1).getCompletedDate().after(logsList.get(2).getCompletedDate()));
    Assert.assertTrue(logsList.get(2).getCompletedDate().after(logsList.get(3).getCompletedDate()));
  }

  @Test
  public void shouldReturnLimitedListOnGetIfLandingPageIsTrue() {
    List<JobExecution> createdJobExecutions = constructAndPostInitJobExecutionRqDto(Integer.MAX_VALUE).getJobExecutions();

    Assert.assertTrue(LANDING_PAGE_LOGS_LIMIT < createdJobExecutions.size());

    List<JobExecution> expectedCommittedChildren = new ArrayList<>();
    for (int i = 0; i < createdJobExecutions.size(); i++) {
      JobExecution createdJobExecution = createdJobExecutions.get(i);
      if (CHILD.equals(createdJobExecution.getSubordinationType())) {
        createdJobExecution.setStatus(COMMITTED);
        createdJobExecution.setJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName(PROFILE_NAME));
        expectedCommittedChildren.add(createdJobExecution);
      }
    }

    for (JobExecution child : expectedCommittedChildren) {
      putJobExecution(child)
        .then()
        .statusCode(HttpStatus.SC_OK);
    }

    LogCollectionDto logCollectionDto = RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_TRUE)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(LogCollectionDto.class);

    Assert.assertEquals(LANDING_PAGE_LOGS_LIMIT, logCollectionDto.getLogDtos().size());
    Assert.assertEquals(createdJobExecutions.size() - 1, logCollectionDto.getTotalRecords().intValue());
  }

  @Test
  public void shouldReturnCommittedAndErrorJobExecutionsAsLogs() {
    int actualFilesNumber = 10;

    List<JobExecution> createdJobExecutions = constructAndPostInitJobExecutionRqDto(actualFilesNumber).getJobExecutions();

    List<JobExecution> expectedLogs = new ArrayList<>();
    for (int i = 0; i < createdJobExecutions.size(); i++) {
      JobExecution createdJobExecution = createdJobExecutions.get(i);
      if (CHILD.equals(createdJobExecution.getSubordinationType())) {
        if (i % 2 == 0) {
          createdJobExecution.setStatus(COMMITTED);
        } else {
          createdJobExecution.setStatus(ERROR);
        }
        createdJobExecution.setJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName(PROFILE_NAME));
        createdJobExecution.setCompletedDate(new Date(1542714612000L + i));
        expectedLogs.add(createdJobExecution);
      }
    }

    for (JobExecution child : expectedLogs) {
      putJobExecution(child)
        .then()
        .statusCode(HttpStatus.SC_OK);
    }

    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_FALSE)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(expectedLogs.size()))
      .body("logDtos*.status", everyItem(anyOf(is(LogDto.Status.COMMITTED.name()), is(LogDto.Status.ERROR.name()))));
  }

  @Test
  public void shouldReturnLogWithTotalRecordsFromParentJobExecution() {
    int expectedTotalRecords = 15;

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));
    WireMock.stubFor(post(INVENTORY_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    RawRecordsDto rawRecordsDto = new RawRecordsDto()
      .withRecordsMetadata(new RecordsMetadata()
        .withLast(true)
        .withCounter(1)
        .withTotal(expectedTotalRecords)
        .withContentType(RecordsMetadata.ContentType.MARC_RAW))
      .withInitialRecords(Collections.singletonList(new InitialRecord().withRecord(RAW_RECORD)));

    List<JobExecution> createdJobExecutions = constructAndPostInitJobExecutionRqDto(2).getJobExecutions();
    JobExecution jobExecution = createdJobExecutions.get(0);

    jobExecution.setJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName(PROFILE_NAME).withDataType(MARC));
    putJobExecution(jobExecution)
      .then()
      .statusCode(HttpStatus.SC_OK);

    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto)
      .when()
      .post(JOB_EXECUTION_PATH + jobExecution.getId() + "/records")
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(GET_LOGS_PATH_LANDING_PAGE_FALSE)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("logDtos.size()", is(1))
      .body("totalRecords", is(1))
      .body("logDtos.get(0).totalRecords", is(expectedTotalRecords));
  }

  @Test
  public void shouldReturnLogsWithTotalRecordsFromChildJobExecutions() {
    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));
    WireMock.stubFor(post(INVENTORY_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    JobProfileInfo jobProfileInfo = new JobProfileInfo()
      .withId(UUID.randomUUID().toString())
      .withName(PROFILE_NAME)
      .withDataType(MARC);

    List<JobExecution> createdJobExecutions = constructAndPostInitJobExecutionRqDto(2).getJobExecutions();
    List<JobExecution> expectedChildJobs = createdJobExecutions.stream()
      .filter(createdJob -> CHILD.equals(createdJob.getSubordinationType()))
      .peek(childJob -> childJob.setJobProfileInfo(jobProfileInfo))
      .collect(Collectors.toList());

    for (JobExecution jobExecution : expectedChildJobs) {
      putJobExecution(jobExecution)
        .then()
        .statusCode(HttpStatus.SC_OK);
    }

    RawRecordsDto rawRecordsDto = new RawRecordsDto()
      .withInitialRecords(Collections.singletonList(new InitialRecord().withRecord(RAW_RECORD)));

    RecordsMetadata recordsMetadata = new RecordsMetadata()
      .withLast(true)
      .withCounter(1)
      .withContentType(RecordsMetadata.ContentType.MARC_RAW);

    ArrayList<RawRecordsDto> rawRecords = new ArrayList<>();
    rawRecords.add(rawRecordsDto.withRecordsMetadata(recordsMetadata.withTotal(15)));
    rawRecords.add(rawRecordsDto.withRecordsMetadata(recordsMetadata.withTotal(1)));

    for (int i = 0; i < expectedChildJobs.size(); i++) {
      RestAssured.given()
        .spec(spec)
        .body(rawRecords.get(i))
        .when()
        .post(JOB_EXECUTION_PATH + expectedChildJobs.get(i).getId() + "/records")
        .then()
        .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    for (int i = 0; i < expectedChildJobs.size(); i++) {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(GET_LOGS_PATH_LANDING_PAGE_FALSE + "&query=id=" + expectedChildJobs.get(0).getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("logDtos.size()", is(1))
        .body("totalRecords", is(1))
        .body("logDtos.get(0).totalRecords", is(rawRecords.get(i).getRecordsMetadata().getTotal()));
    }
  }

  private Response putJobExecution(JobExecution jobExecution) {
    return RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobExecution).toString())
      .when()
      .put(PUT_JOB_EXECUTIONS_PATH + jobExecution.getId());
  }
}
