package org.folio.rest.impl.changeManager;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.dao.JobExecutionDaoImpl;
import org.folio.rest.RestVerticle;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.services.JobExecutionServiceImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.folio.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.util.RestUtil.OKAPI_URL_HEADER;

/**
 * REST tests for ChangeManager to manager JobExecution entities initialization
 */

@RunWith(VertxUnitRunner.class)
public class ChangeManagerAPITest extends AbstractRestTest {

  private static final String CHANGE_MANAGER_PATH = "/change-manager";
  private static final String POST_JOB_EXECUTIONS_PATH = CHANGE_MANAGER_PATH + "/jobExecutions";

  private static RequestSpecification spec;
  private static String postedSnapshotResponseBody = UUID.randomUUID().toString();

  @Rule
  public WireMockRule snapshotMockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new ConsoleNotifier(true)));

  @Before
  public void setUp(TestContext context) {
    spec = new RequestSpecBuilder()
      .setContentType(ContentType.JSON)
      .addHeader(OKAPI_URL_HEADER, "http://localhost:" + snapshotMockServer.port())
      .addHeader(OKAPI_TENANT_HEADER, TENANT_ID)
      .addHeader(RestVerticle.OKAPI_USERID_HEADER, UUID.randomUUID().toString())
      .addHeader("Accept", "text/plain, application/json")
      .setBaseUri("http://localhost:" + port)
      .build();

    WireMock.stubFor(WireMock.post(JobExecutionServiceImpl.SNAPSHOT_SERVICE_URL)
      .willReturn(WireMock.created().withBody(postedSnapshotResponseBody)));

    clearTable(context);
  }

  @Test
  public void testInitJobExecutionsWith1File() {
    // given
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    File givenFile = new File().withName("importBib.bib");
    requestDto.getFiles().add(givenFile);
    int expectedJobExecutionsNumber = 1;

    // when
    String body = RestAssured.given()
      .spec(spec)
      .header(TENANT_HEADER)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().post(POST_JOB_EXECUTIONS_PATH)
      .then().statusCode(HttpStatus.SC_CREATED)
      .extract().body().jsonPath().prettify();

    // then
    InitJobExecutionsRsDto response = new JsonObject(body).mapTo(InitJobExecutionsRsDto.class);
    String actualParentJobExecutionId = response.getParentJobExecutionId();
    List<JobExecution> actualJobExecutions = response.getJobExecutions();

    Assert.assertNotNull(actualParentJobExecutionId);
    Assert.assertEquals(expectedJobExecutionsNumber, actualJobExecutions.size());

    JobExecution parentSingle = actualJobExecutions.get(0);
    assertParent(parentSingle);
  }

  @Test
  public void testInitJobExecutionsWith2Files() {
    // given
    String servicePath = "/jobExecutions";
    String testUrl = CHANGE_MANAGER_PATH + servicePath;
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.getFiles().add(new File().withName("importBib.bib"));
    requestDto.getFiles().add(new File().withName("importMarc.mrc"));
    int expectedParentJobExecutions = 1;
    int expectedChildJobExecutions = 2;
    int expectedJobExecutionsNumber = expectedParentJobExecutions + expectedChildJobExecutions;

    // when
    String body = RestAssured.given()
      .spec(spec)
      .header(TENANT_HEADER)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().post(testUrl)
      .then().statusCode(HttpStatus.SC_CREATED)
      .extract().body().jsonPath().prettify();

    // then
    InitJobExecutionsRsDto response = new JsonObject(body).mapTo(InitJobExecutionsRsDto.class);
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

    // when
    RestAssured.given()
      .spec(spec)
      .header(TENANT_HEADER)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().post(POST_JOB_EXECUTIONS_PATH)
      .then().statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  private void assertParent(JobExecution parent) {
    Assert.assertNotNull(parent);
    Assert.assertNotNull(parent.getId());
    Assert.assertNotNull(parent.getParentJobId());
    Set<JobExecution.SubordinationType> parentTypes = EnumSet.of(
      JobExecution.SubordinationType.PARENT_SINGLE,
      JobExecution.SubordinationType.PARENT_MULTIPLE
    );
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

  private void clearTable(TestContext context) {
    PostgresClient.getInstance(vertx, TENANT_ID).delete(JobExecutionDaoImpl.TABLE_NAME, new Criterion(), event -> {
      if (event.failed()) {
        context.fail(event.cause());
      }
    });
  }
}
