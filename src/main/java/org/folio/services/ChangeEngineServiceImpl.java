package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dao.JobExecutionDao;
import org.folio.dao.JobExecutionDaoImpl;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.util.OkapiConnectionParams;
import org.folio.util.RestUtil;

import java.net.URLEncoder;

public class ChangeEngineServiceImpl implements ChangeEngineService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEngineServiceImpl.class);
  public static final String RECORD_SERVICE_URL = "/source-storage/record";

  private Vertx vertx;
  private String tenantId;
  private JobExecutionDao jobExecutionDao;

  public ChangeEngineServiceImpl(Vertx vertx, String tenantId) {
    this.vertx = vertx;
    this.tenantId = tenantId;
    this.jobExecutionDao = new JobExecutionDaoImpl(vertx, tenantId);
  }

  @Override
  public Future<JobExecution> parseSourceRecordsForJobExecution(JobExecution job, OkapiConnectionParams params) {
    Future<JobExecution> future = Future.future();
    RestUtil.doRequest(params, buildQueryForRecordsCount(job.getId()), HttpMethod.GET, null)
      .setHandler(countRecordsResult -> {
        if (countRecordsResult.failed()) {
          LOGGER.error("Error during requesting number of records for jobExecution with id: " + job.getId());
          future.fail(countRecordsResult.cause());
        } else {
          JsonObject recordCollection = countRecordsResult.result().getJson();
          if (recordCollection == null) {
            String errorMessage = "Error during getting records for parse";
            LOGGER.error(errorMessage);
            future.fail(errorMessage);
          } else {
            Integer totalRecords = recordCollection.getInteger("totalRecords");
          }
        }
      });
    return future;
  }

  private String buildQueryForRecordsCount(String jobId) {
    String query = "snapshotId==" + jobId;
    StringBuilder queryParams = new StringBuilder(RECORD_SERVICE_URL);
    try {
      queryParams.append("?");
      queryParams.append("query=");
      queryParams.append(URLEncoder.encode(query, "UTF-8"));
      queryParams.append("&");
      queryParams.append("offset=0");
      queryParams.append("&");
      queryParams.append("limit=1");
    } catch (Exception e) {
      LOGGER.error("Error during build query for records count", e);
    }
//    sb.append("?query=snapshotId==").append(jobId).append("&limit=1");
    return queryParams.toString();
  }
}
