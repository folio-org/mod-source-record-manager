package org.folio.services.converters;

import java.util.Collection;

/**
 * The root interface for converting Source object to the Target object
 *
 * @param <S> source object
 * @param <T> result target object
 */
public interface GenericConverter<S, T> {

  /**
   * Converts source object to target object.
   *
   * @param source - object to convert
   * @return converted object of target class
   */
  T convert(S source);

  /**
   * Converts Collection of source objects to Collection target objects.
   *
   * @param source - object to convert
   * @return converted object of target class
   */
  Collection<T> convert(Collection<S> source);
}
