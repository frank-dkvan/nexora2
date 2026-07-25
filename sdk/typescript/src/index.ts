/**
 * Nexora TypeScript Driver SDK
 * =================================
 *
 * A lightweight, fully-typed client for the Nexora streaming graph
 * database. Uses the native `fetch` API — no external dependencies.
 * Works in Node 18+ and modern browsers.
 *
 * @example
 * ```ts
 * import { NexoraClient } from 'nexora';
 *
 * const client = new NexoraClient('http://localhost:8080');
 * await client.setProperty('alice', 'name', 'Alice');
 * const rows = await client.query('MATCH (n) RETURN n LIMIT 10');
 * ```
 */

export { NexoraClient } from './client';
export {
  StandingQuerySubscription,
  CypherStreamSubscription,
  type ResultCallback,
} from './websocket';
export {
  NexoraError,
  ConnectionError,
  QueryError,
  NodeNotFoundError,
  AuthenticationError,
  TimeoutError,
} from './errors';

// Re-export all types
export type * from './types';

export const VERSION = '1.0.0';
