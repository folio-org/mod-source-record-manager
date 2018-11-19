package org.folio.dao;

import io.vertx.core.Future;

import java.util.List;
import java.util.Optional;

/**
 * The root interface for Data Access Objects, contains CRUD methods to work with <ENTITY>.
 *
 * @param <E> dedicated entity
 */
public interface GenericDao<E> {

  /**
   * Returns List of entities by the input query
   *
   * @param query  query string to filter entities
   * @param offset starting index in a list of results
   * @param limit  maximum number of results to return
   */
  Future<List<E>> getByQuery(String query, int offset, int limit);

  /**
   * Retrieves Entity if it exists
   *
   * @param id UUID value
   * @return optional entity
   */
  Future<Optional<E>> getById(String id);

  /**
   * Saves entity in the storage and returns
   *
   * @param id     UUID value
   * @param entity entity to save
   * @return entity
   */
  Future<E> save(String id, E entity);

  /**
   * Updates entity in the storage and returns
   *
   * @param id     UUID value
   * @param entity entity to update
   * @return entity
   */
  Future<E> update(String id, E entity);

  /**
   * Removes entity from the storage
   *
   * @param id UUID value
   * @return boolean result, true if the entity has been successfully removed
   */
  Future<Boolean> delete(String id);
}
