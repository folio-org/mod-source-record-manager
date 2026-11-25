package org.folio.services;

import static java.lang.String.format;

import io.vertx.core.Future;
import java.util.function.UnaryOperator;
import javax.ws.rs.NotFoundException;
import lombok.extern.log4j.Log4j2;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;

/**
 * Read-modify-write updates of a {@link JobExecutionSourceChunk}.
 */
@Log4j2
final class SourceChunkStateUpdater {

  private SourceChunkStateUpdater() {
  }

  /**
   * Moves the source chunk to {@link JobExecutionSourceChunk.State#ERROR}. Never fails: a failure to update is logged.
   */
  static Future<Void> markAsError(JobExecutionSourceChunkDao dao, String sourceChunkId, String jobExecutionId,
                                  String tenantId) {
    return update(dao, sourceChunkId, tenantId, "set state ERROR for", jobExecutionId,
      chunk -> chunk.withState(JobExecutionSourceChunk.State.ERROR))
      .recover(e -> {
        log.warn("markAsError:: Failed to mark source chunk {} as ERROR, jobExecutionId: {}",
          sourceChunkId, jobExecutionId, e);
        return Future.succeededFuture();
      });
  }

  static Future<Void> update(JobExecutionSourceChunkDao dao, String sourceChunkId, String tenantId, String action,
                             String jobExecutionId, UnaryOperator<JobExecutionSourceChunk> change) {
    return dao.getById(sourceChunkId, tenantId)
      .compose(optional -> optional
        .map(chunk -> dao.update(change.apply(chunk), tenantId))
        .orElseThrow(() -> new NotFoundException(format(
          "Couldn't %s jobExecutionSourceChunk, it was not found: id %s, jobExecutionId: %s",
          action, sourceChunkId, jobExecutionId))))
      .mapEmpty();
  }
}
