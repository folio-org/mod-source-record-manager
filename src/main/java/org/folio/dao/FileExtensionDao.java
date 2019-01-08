package org.folio.dao;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.FileExtension;
import org.folio.rest.jaxrs.model.FileExtensionCollection;

import java.util.List;
import java.util.Optional;

/**
 * Data access object for {@link FileExtension}
 */
public interface FileExtensionDao {

  /**
   * Searches for {@link FileExtension} in database
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
   * Searches for all {@link FileExtension} in database
   *
   * @return future with {@link FileExtensionCollection}
   */
  Future<FileExtensionCollection> getAllFileExtensions();

  /**
   * Saves {@link FileExtension} to database
   *
   * @param fileExtension FileExtension to save
   * @return future
   */
  Future<String> addFileExtension(FileExtension fileExtension);

  /**
   * Updates {@link FileExtension} in database
   *
   * @param fileExtension FileExtension to update
   * @return future with {@link FileExtension}
   */
  Future<FileExtension> updateFileExtension(FileExtension fileExtension);

  /**
   * Deletes {@link FileExtension} from database
   *
   * @param id FileExtension id
   * @return future with true if succeeded
   */
  Future<Boolean> deleteFileExtension(String id);

  /**
   * Restore default values for {@link FileExtension}
   *
   * @return - future with restored file extensions
   */
  Future<FileExtensionCollection> restoreFileExtensions();
}
