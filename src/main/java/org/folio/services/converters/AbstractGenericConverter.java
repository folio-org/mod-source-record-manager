package org.folio.services.converters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Abstract converter, makes inheritors be able to convert list of the SOURCE objects.
 *
 * @param <SOURCE> - object with source data
 * @param <TARGET> - target object
 */
public abstract class AbstractGenericConverter<SOURCE, TARGET> implements GenericConverter<SOURCE, TARGET> {

  @Override
  public List<TARGET> convert(Collection<SOURCE> sources) {
    if (sources == null) {
      throw new IllegalArgumentException("Source object can not be null");
    }
    List<TARGET> result = new ArrayList<>(sources.size());
    for (SOURCE source : sources) {
      result.add(convert(source));
    }
    return result;
  }
}
