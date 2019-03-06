package org.folio.services.converters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Abstract converter, makes inheritors be able to convert list of the SOURCE objects.
 *
 * @param <S> - object with source data
 * @param <T> - target object
 */
public abstract class AbstractGenericConverter<S, T> implements GenericConverter<S, T> {

  @Override
  public List<T> convert(Collection<S> sources) {
    if (sources == null) {
      throw new IllegalArgumentException("Source object can not be null");
    }
    List<T> result = new ArrayList<>(sources.size());
    for (S source : sources) {
      result.add(convert(source));
    }
    return result;
  }
}
