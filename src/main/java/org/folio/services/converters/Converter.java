package org.folio.services.converters;

/**
 * The root interface for converting Source object to the Target object
 * @param <SOURCE> source object
 * @param <TARGET> result target object
 */
public interface Converter<SOURCE, TARGET> {

  /**
   * Converts source object to target object.
   *
   * @param source - object to convert
   * @return converted object of target class
   */
  TARGET convert(SOURCE source);
}
