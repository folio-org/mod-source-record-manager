package org.folio.dao;

import io.vertx.core.Future;

import java.util.List;
import java.util.Optional;

/**
 * The root interface for Data Access Objects, contains CRUD methods to work with <ENTITY>.
 *
 * @param <ENTITY> target entity type
 */
public interface GenericDao<ENTITY> {

  /**
   * Returns List of entities by the input query
   *
   * @param query  query string to filter entities
   * @param offset starting index in a list of results
   * @param limit  maximum number of results to return
   */
  Future<List<ENTITY>> getByQuery(String query, int offset, int limit);

  /**
   * Retrieves Entity if it exists
   *
   * @param id UUID value
   * @return optional entity
   */
  Future<Optional<ENTITY>> getById(String id);

  /**
   * Saves entity in the storage and returns
   *
   * @param id     UUID value
   * @param entity entity to save
   * @return entity
   */
  Future<ENTITY> save(String id, ENTITY entity);

  /**
   * Updates entity in the storage and returns
   *
   * @param id     UUID value
   * @param entity entity to update
   * @return entity
   */
  Future<ENTITY> update(String id, ENTITY entity);

  /**
   * Removes entity from the storage
   *
   * @param id UUID value
   * @return boolean result, true if the entity has been successfully removed
   */
  Future<Boolean> delete(String id);
}
