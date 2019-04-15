package org.folio.services.mappers.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;

public class LoaderHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoaderHelper.class);
  private LoaderHelper() {}

  public static boolean isMappingValid(Object object, String[] path)
    throws InstantiationException, IllegalAccessException {
    Class<?> type = null;
    for (int i = 0; i < path.length; i++) {
      Field field;
      try {
        field = object.getClass().getDeclaredField(path[i]);
      } catch (NoSuchFieldException e) {
        return false;
      }
      type = field.getType();

      // this is a configuration error, the type is an object, but no fields are indicated
      // to be populated on that object. if you map a marc field to an object, it must be
      // something like - marc.identifier -> identifierObject.idField
      if (type.isAssignableFrom(java.util.List.class)
        || type.isAssignableFrom(java.util.Set.class)) {
        ParameterizedType listType = (ParameterizedType) field.getGenericType();
        Class<?> listTypeClass = (Class<?>) listType.getActualTypeArguments()[0];
        object = listTypeClass.newInstance();
        if (isPrimitiveOrPrimitiveWrapperOrString(listTypeClass) && i == path.length - 1) {
          // we are here if the last entry in the path is an array / set of primitives, that is ok
          return true;
        }
      }
    }
    return type != null && isPrimitiveOrPrimitiveWrapperOrString(type);
  }

  public static boolean isPrimitiveOrPrimitiveWrapperOrString(Class<?> type) {
    return (type.isPrimitive() && type != void.class) || type == Double.class || type == Float.class
      || type == Long.class || type == Integer.class || type == Short.class
      || type == Character.class || type == Byte.class || type == Boolean.class
      || type == String.class;
  }

  public static void closeInputStream(InputStream inputStream) {
    if (inputStream == null) {
      return;
    }
    try {
      inputStream.close();
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }
}
