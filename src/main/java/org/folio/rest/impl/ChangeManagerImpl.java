package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.resource.ChangeManager;

import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.UUID;

public class ChangeManagerImpl implements ChangeManager {

  @Override
  public void postChangeManagerJobExecutions(InitJobExecutionsRqDto initJobExecutionsRqDto, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        // TODO stub implementation
        InitJobExecutionsRsDto rsDto = new InitJobExecutionsRsDto();
        String parentJobExecutionId = UUID.randomUUID().toString();

        rsDto.setParentJobExecutionId(parentJobExecutionId);
        for (File file : initJobExecutionsRqDto.getFiles()) {
          rsDto.getJobExecutions().add(
            new JobExecution()
              .withId(UUID.randomUUID().toString())
              .withParentJobId(parentJobExecutionId)
              .withSourcePath(file.getName())
          );
        }
        asyncResultHandler.handle(Future.succeededFuture(PostChangeManagerJobExecutionsResponse.respond201WithApplicationJson(rsDto)));
      } catch (Exception e) {
        asyncResultHandler.handle(Future.failedFuture(e));
      }
    });

  }
}
