package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.folio.dao.FileExtensionDao;
import org.folio.dao.FileExtensionDaoImpl;
import org.folio.rest.jaxrs.model.FileExtension;
import org.folio.rest.jaxrs.model.FileExtensionCollection;

import javax.ws.rs.NotFoundException;
import java.util.Optional;
import java.util.UUID;

public class FileExtensionServiceImpl implements FileExtensionService {

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
  public Future<FileExtension> addFileExtension(FileExtension fileExtension) {
    fileExtension.setId(UUID.randomUUID().toString());
    return fileExtensionDao.addFileExtension(fileExtension).map(fileExtension);
  }

  @Override
  public Future<FileExtension> updateFileExtension(FileExtension fileExtension) {
    return getFileExtensionById(fileExtension.getId())
      .compose(optionalFileExtension -> optionalFileExtension
        .map(fileExt -> fileExtensionDao.updateFileExtension(fileExtension))
        .orElse(Future.failedFuture(new NotFoundException(
          String.format("FileExtension with id '%s' was not found", fileExtension.getId()))))
      );
  }

  @Override
  public Future<Boolean> deleteFileExtension(String id) {
    return fileExtensionDao.deleteFileExtension(id);
  }
}
