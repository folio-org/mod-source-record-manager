package org.folio.services;

import io.vertx.core.Future;
import io.vertx.ext.sql.UpdateResult;
import org.folio.rest.jaxrs.model.FileExtension;
import org.folio.rest.jaxrs.model.FileExtensionCollection;

import java.util.Optional;

/**
 * FileExtension Service
 */
public interface FileExtensionService {

  /**
   * Searches for {@link FileExtension}
   *
   * @param query  query from URL
   * @param offset starting index in a list of results
   * @param limit  limit of records for pagination
   * @return future with {@link FileExtensionCollection}
   */
  Future<FileExtensionCollection> getFileExtensions(String query, int offset, int limit);

  /**
   * Searches for {@link FileExtension} by id
   *
   * @param id FileExtension id
   * @return future with optional {@link FileExtension}
   */
  Future<Optional<FileExtension>> getFileExtensionById(String id);

  /**
   * Saves {@link FileExtension}
   *
   * @param fileExtension FileExtension to save
   * @return future with {@link FileExtension}
   */
  Future<FileExtension> addFileExtension(FileExtension fileExtension);

  /**
   * Updates {@link FileExtension} with given id
   *
   * @param fileExtension FileExtension to update
   * @return future with {@link FileExtension}
   */
  Future<FileExtension> updateFileExtension(FileExtension fileExtension);

  /**
   * Deletes {@link FileExtension} by id
   *
   * @param id FileExtension id
   * @return future with true if succeeded
   */
  Future<Boolean> deleteFileExtension(String id);

  /**
   * Restore default values for {@link FileExtension}
   *
   * @return future with {@link FileExtensionCollection} that contains default values
   */
  Future<FileExtensionCollection> restoreFileExtensions();

  /**
   * Copy values from default_file_extensions into the file_extensions table
   *
   * @return - Update Result of coping execution
   */
  Future<UpdateResult> copyExtensionsFromDefault();
}
