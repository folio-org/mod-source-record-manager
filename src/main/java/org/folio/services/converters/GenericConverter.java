package org.folio.services.converters;

import java.util.Collection;

/**
 * The root interface for converting Source object to the Target object
 *
 * @param <SOURCE> source object
 * @param <TARGET> result target object
 */
public interface GenericConverter<SOURCE, TARGET> {

  /**
   * Converts source object to target object.
   *
   * @param source - object to convert
   * @return converted object of target class
   */
  TARGET convert(SOURCE source);

  /**
   * Converts Collection of source objects to Collection target objects.
   *
   * @param source - object to convert
   * @return converted object of target class
   */
  Collection<TARGET> convert(Collection<SOURCE> source);
}
