package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import org.apache.commons.io.IOUtils;
import org.folio.rest.jaxrs.model.FileExtension;
import org.folio.rest.jaxrs.model.FileExtensionCollection;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;

import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.folio.dao.util.DaoUtil.constructCriteria;
import static org.folio.dao.util.DaoUtil.getCQLWrapper;
import static org.folio.rest.impl.ModTenantAPI.MODULE_PLACEHOLDER;
import static org.folio.rest.impl.ModTenantAPI.TENANT_PLACEHOLDER;

public class FileExtensionDaoImpl implements FileExtensionDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileExtensionDaoImpl.class);

  private static final String FILE_EXTENSIONS_TABLE = "file_extensions";
  private static final String DEFAULT_FILE_EXTENSIONS_TABLE = "default_file_extensions";
  private static final String DEFAULT_FILE_EXTENSIONS_SQL = "templates/db_scripts/defaultFileExtensions.sql";
  private static final String ID_FIELD = "'id'";

  private PostgresClient pgClient;
  private String tenantId;

  public FileExtensionDaoImpl(Vertx vertx, String tenantId) {
    pgClient = PostgresClient.getInstance(vertx, tenantId);
    this.tenantId = tenantId;
  }

  @Override
  public Future<FileExtensionCollection> getFileExtensions(String query, int offset, int limit) {
    Future<Results<FileExtension>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = getCQLWrapper(FILE_EXTENSIONS_TABLE, query, limit, offset);
      pgClient.get(FILE_EXTENSIONS_TABLE, FileExtension.class, fieldList, cql, true, false, future.completer());
    } catch (Exception e) {
      LOGGER.error("Error while searching for FileExtensions", e);
      future.fail(e);
    }
    return future.map(results -> new FileExtensionCollection()
      .withFileExtensions(results.getResults())
      .withTotalRecords(results.getResultInfo().getTotalRecords()));
  }

  @Override
  public Future<FileExtensionCollection> getAllFileExtensions() {
    Future<Results<FileExtension>> future = Future.future();
    try {
      pgClient.get(FILE_EXTENSIONS_TABLE, FileExtension.class, new Criterion(), true, false, future.completer());
    } catch (Exception e) {
      LOGGER.error("Error while searching for FileExtensions", e);
      future.fail(e);
    }
    return future.map(results -> new FileExtensionCollection()
      .withFileExtensions(results.getResults())
      .withTotalRecords(results.getResultInfo().getTotalRecords()));
  }

  @Override
  public Future<Optional<FileExtension>> getFileExtensionById(String id) {
    Future<Results<FileExtension>> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, id);
      pgClient.get(FILE_EXTENSIONS_TABLE, FileExtension.class, new Criterion(idCrit), true, false, future.completer());
    } catch (Exception e) {
      LOGGER.error("Error querying FileExtensions by id", e);
      future.fail(e);
    }
    return future
      .map(Results::getResults)
      .map(fileExtensions -> fileExtensions.isEmpty() ? Optional.empty() : Optional.of(fileExtensions.get(0)));
  }

  @Override
  public Future<String> addFileExtension(FileExtension fileExtension) {
    Future<String> future = Future.future();
    pgClient.save(FILE_EXTENSIONS_TABLE, fileExtension.getId(), fileExtension, future.completer());
    return future;
  }

  @Override
  public Future<FileExtension> updateFileExtension(FileExtension fileExtension) {
    Future<FileExtension> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, fileExtension.getId());
      pgClient.update(FILE_EXTENSIONS_TABLE, fileExtension, new Criterion(idCrit), true, updateResult -> {
        if (updateResult.failed()) {
          LOGGER.error(String.format("Could not update fileExtension with id '%s'", fileExtension.getId()), updateResult.cause());
          future.fail(updateResult.cause());
        } else if (updateResult.result().getUpdated() != 1) {
          String errorMessage = String.format("FileExtension with id '%s' was not found", fileExtension.getId());
          LOGGER.error(errorMessage);
          future.fail(new NotFoundException(errorMessage));
        } else {
          future.complete(fileExtension);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Error updating fileExtension", e);
      future.fail(e);
    }
    return future;
  }

  @Override
  public Future<Boolean> deleteFileExtension(String id) {
    Future<UpdateResult> future = Future.future();
    pgClient.delete(FILE_EXTENSIONS_TABLE, id, future.completer());
    return future.map(updateResult -> updateResult.getUpdated() == 1);
  }

  @Override
  public Future<FileExtensionCollection> restoreFileExtensions() {
    Future<FileExtensionCollection> future = Future.future();
    Future<SQLConnection> tx = Future.future(); //NOSONAR
    String moduleName = PostgresClient.getModuleName();
    Future.succeededFuture()
      .compose(v -> {
        pgClient.startTx(tx.completer());
        return tx;
      }).compose(v -> {
      Future<UpdateResult> deleteFuture = Future.future(); //NOSONAR
      pgClient.delete(tx, FILE_EXTENSIONS_TABLE, new Criterion(), deleteFuture);
      return deleteFuture;
    }).compose(v -> {
      Future<UpdateResult> resultFuture = Future.future(); //NOSONAR
      try {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(DEFAULT_FILE_EXTENSIONS_SQL);
        String sqlScript = IOUtils.toString(inputStream, StandardCharsets.UTF_8.name());
        sqlScript = sqlScript.replace(TENANT_PLACEHOLDER, tenantId)
          .replace(MODULE_PLACEHOLDER, moduleName)
          .replace(DEFAULT_FILE_EXTENSIONS_TABLE, FILE_EXTENSIONS_TABLE);
        pgClient.execute(tx, sqlScript, resultFuture);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return resultFuture;
    }).compose(updateHandler -> {
      if (updateHandler.getUpdated() < 1) {
        throw new InternalServerErrorException();
      }
      Future<Void> endTxFuture = Future.future(); //NOSONAR
      pgClient.endTx(tx, endTxFuture);
      return endTxFuture;
    }).setHandler(result -> {
      if (result.failed()) {
        pgClient.rollbackTx(tx, rollback -> future.fail(result.cause()));
      } else {
        future.complete();
      }
    });
    return future.compose(v->getAllFileExtensions());
  }
}
