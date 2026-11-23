package org.folio.services.exceptions;

/**
 * Marker for failures that the chunk error handlers already report to the import log as DI_ERROR events
 * (one per failed record), so they must not additionally move the source chunk to ERROR.
 */
public interface ReportedToImportLog {
}
