package org.folio.util;

import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.http.HttpStatus;
import org.folio.rest.tools.utils.ValidationHelper;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public final class ExceptionHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionHelper.class);

  private ExceptionHelper() {
  }

  public static Response mapExceptionToResponse(Throwable throwable) {
    if (throwable instanceof BadRequestException) {
      return Response.status(HttpStatus.SC_BAD_REQUEST)
        .type(MediaType.TEXT_PLAIN)
        .entity(throwable.getMessage())
        .build();
    }
    if (throwable instanceof NotFoundException) {
      return Response.status(HttpStatus.SC_NOT_FOUND)
        .type(MediaType.TEXT_PLAIN)
        .entity(throwable.getMessage())
        .build();
    }
    Future<Response> validationFuture = Future.future();
    ValidationHelper.handleError(throwable, validationFuture.completer());
    if (validationFuture.isComplete()) {
      Response response = validationFuture.result();
      if (response.getStatus() == HttpStatus.SC_INTERNAL_SERVER_ERROR) {
        LOGGER.error(throwable.getMessage(), throwable);
      }
      return response;
    }
    LOGGER.error(throwable.getMessage(), throwable);
    return Response.status(HttpStatus.SC_INTERNAL_SERVER_ERROR)
      .type(MediaType.TEXT_PLAIN)
      .entity(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())
      .build();
  }
}
