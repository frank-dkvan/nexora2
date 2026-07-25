/**
 * Error hierarchy for the Nexora TypeScript SDK.
 *
 * All errors thrown by the SDK extend {@link NexoraError}, so callers can
 * catch every SDK-specific error with a single `catch (e)` clause.
 */

/** Base error for all Nexora SDK errors. */
export class NexoraError extends Error {
  /** HTTP status code from the server response, if applicable. */
  readonly statusCode?: number;
  /** Raw response body from the server, if available. */
  readonly responseBody?: unknown;

  constructor(
    message: string,
    options?: {
      statusCode?: number;
      responseBody?: unknown;
    },
  ) {
    super(message);
    this.name = 'NexoraError';
    if (options) {
      this.statusCode = options.statusCode;
      this.responseBody = options.responseBody;
    }
  }
}

/** Raised when the SDK cannot connect to the Nexora server. */
export class ConnectionError extends NexoraError {
  constructor(message: string) {
    super(message);
    this.name = 'ConnectionError';
  }
}

/** Raised when a Cypher or SQL query fails on the server side (HTTP 400). */
export class QueryError extends NexoraError {
  constructor(
    message: string,
    options?: { statusCode?: number; responseBody?: unknown },
  ) {
    super(message, options);
    this.name = 'QueryError';
  }
}

/** Raised when a node or resource is not found (HTTP 404). */
export class NodeNotFoundError extends NexoraError {
  constructor(
    message: string,
    options?: { statusCode?: number; responseBody?: unknown },
  ) {
    super(message, options);
    this.name = 'NodeNotFoundError';
  }
}

/** Raised when authentication fails (HTTP 401 / 403). */
export class AuthenticationError extends NexoraError {
  constructor(
    message: string,
    options?: { statusCode?: number; responseBody?: unknown },
  ) {
    super(message, options);
    this.name = 'AuthenticationError';
  }
}

/** Raised when a request exceeds the configured timeout. */
export class TimeoutError extends NexoraError {
  constructor(message: string) {
    super(message);
    this.name = 'TimeoutError';
  }
}
