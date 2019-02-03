package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.UpdateResult;
import org.folio.dao.FileExtensionDao;
import org.folio.dao.FileExtensionDaoImpl;
import org.folio.dataImport.util.OkapiConnectionParams;
import org.folio.dataImport.util.RestUtil;
import org.folio.rest.jaxrs.model.DataType;
import org.folio.rest.jaxrs.model.FileExtension;
import org.folio.rest.jaxrs.model.FileExtensionCollection;
import org.folio.rest.jaxrs.model.UserInfo;

import javax.ws.rs.NotFoundException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.folio.rest.RestVerticle.OKAPI_USERID_HEADER;

public class FileExtensionServiceImpl implements FileExtensionService {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileExtensionServiceImpl.class);
  private static final String GET_USER_URL = "/users?query=id==";
  private FileExtensionDao fileExtensionDao;

  public FileExtensionServiceImpl(Vertx vertx, String tenantId) {
    this.fileExtensionDao = new FileExtensionDaoImpl(vertx, tenantId);
  }

  @Override
  public Future<FileExtensionCollection> getFileExtensions(String query, int offset, int limit) {
    return fileExtensionDao.getFileExtensions(query, offset, limit);
  }

  @Override
  public Future<Optional<FileExtension>> getFileExtensionById(String id) {
    return fileExtensionDao.getFileExtensionById(id);
  }

  @Override
  public Future<FileExtension> addFileExtension(FileExtension fileExtension, OkapiConnectionParams params) {
    fileExtension.setId(UUID.randomUUID().toString());
    fileExtension.setDataTypes(sortDataTypes(fileExtension.getDataTypes()));
    return lookupUser(params).compose(userInfo -> {
      fileExtension.setUserInfo(userInfo);
      return fileExtensionDao.addFileExtension(fileExtension).map(fileExtension);
    });
  }

  @Override
  public Future<FileExtension> updateFileExtension(FileExtension fileExtension, OkapiConnectionParams params) {
    return getFileExtensionById(fileExtension.getId())
      .compose(optionalFileExtension -> optionalFileExtension.map(fileExt ->lookupUser(params).compose(userInfo -> {
        fileExtension.setUserInfo(userInfo);
        return fileExtensionDao.updateFileExtension(fileExtension.withDataTypes(sortDataTypes(fileExtension.getDataTypes())));
      })
    ).orElse(Future.failedFuture(new NotFoundException(String.format("FileExtension with id '%s' was not found", fileExtension.getId())))));
  }

  @Override
  public Future<Boolean> deleteFileExtension(String id) {
    return fileExtensionDao.deleteFileExtension(id);
  }

  @Override
  public Future<FileExtensionCollection> restoreFileExtensions() {
    return fileExtensionDao.restoreFileExtensions();
  }

  @Override
  public Future<UpdateResult> copyExtensionsFromDefault() {
    return fileExtensionDao.copyExtensionsFromDefault();
  }

  private List<DataType> sortDataTypes(List<DataType> list) {
    if (list == null) {
      return Collections.emptyList();
    }
    Collections.sort(list);
    return list;
  }

  /**
   * Finds user by user id and returns UserInfo
   *
   * @param params Okapi connection params
   * @return Future with found UserInfo
   */
  private Future<UserInfo> lookupUser(OkapiConnectionParams params) {
    Future<UserInfo> future = Future.future();
    String userId = params.getHeaders().get(OKAPI_USERID_HEADER);
    RestUtil.doRequest(params, GET_USER_URL + userId, HttpMethod.GET, null)
      .setHandler(getUserResult -> {
        if (RestUtil.validateAsyncResult(getUserResult, future)) {
          JsonObject response = getUserResult.result().getJson();
          if (!response.containsKey("totalRecords") || !response.containsKey("users")) {
            future.fail("Error, missing field(s) 'totalRecords' and/or 'users' in user response object");
          } else {
            int recordCount = response.getInteger("totalRecords");
            if (recordCount > 1) {
              String errorMessage = "There are more then one user by requested user id : " + userId;
              LOGGER.error(errorMessage);
              future.fail(errorMessage);
            } else if (recordCount == 0) {
              String errorMessage = "No user found by user id :" + userId;
              LOGGER.error(errorMessage);
              future.fail(errorMessage);
            } else {
              JsonObject jsonUser = response.getJsonArray("users").getJsonObject(0);
              JsonObject userPersonalInfo = jsonUser.getJsonObject("personal");
              UserInfo userInfo = new UserInfo()
                .withFirstName(userPersonalInfo.getString("firstName"))
                .withLastName(userPersonalInfo.getString("lastName"))
                .withUserName(jsonUser.getString("username"));
              future.complete(userInfo);
            }
          }
        }
      });
    return future;
  }
}
