/**
 * WebSocket streaming clients for Nexora.
 *
 * Provides two subscription classes:
 *
 * - {@link StandingQuerySubscription} — receive real-time notifications
 *   when a Standing Query matches.
 * - {@link CypherStreamSubscription} — stream Cypher query results over
 *   a WebSocket connection.
 *
 * Both classes use the native `WebSocket` API (available in Node 18+ and
 * all modern browsers).
 *
 * @example Standing Query
 * ```ts
 * import { NexoraClient, StandingQuerySubscription } from 'nexora';
 *
 * const client = new NexoraClient('http://localhost:8080');
 * const sqId = await client.createStandingQuery(
 *   { type: 'PropertyFilter', key: 'speed',
 *     condition: { type: 'GreaterThan', value: 100 } },
 *   'fast_alert',
 * );
 *
 * const sub = new StandingQuerySubscription('ws://localhost:8080', sqId);
 * await sub.subscribe((data) => console.log('Match!', data));
 * // ... later
 * await sub.unsubscribe();
 * ```
 *
 * @example Cypher Stream
 * ```ts
 * const stream = new CypherStreamSubscription('ws://localhost:8080');
 * for await (const row of stream.stream('MATCH (n) RETURN n LIMIT 10')) {
 *   console.log(row);
 * }
 * ```
 */

import { ConnectionError } from './errors';

/** Callback type for receiving WebSocket messages. */
export type ResultCallback = (data: Record<string, unknown>) => void;

/**
 * Subscribe to Standing Query results via WebSocket.
 *
 * Connects to `/api/v2/ws/sq/{queryId}` and receives real-time
 * notifications whenever the Standing Query matches new data.
 */
export class StandingQuerySubscription {
  private readonly wsUrl: string;
  private readonly queryId: string;
  private readonly apiKey?: string;
  private ws: WebSocket | null = null;
  private running = false;

  constructor(wsUrl: string, queryId: string, apiKey?: string) {
    this.wsUrl = wsUrl.replace(/\/$/, '');
    this.queryId = queryId;
    this.apiKey = apiKey;
  }

  private buildUrl(): string {
    const base = `${this.wsUrl}/api/v2/ws/sq/${this.queryId}`;
    if (this.apiKey) {
      return `${base}?token=${encodeURIComponent(this.apiKey)}`;
    }
    return base;
  }

  /** Whether the subscription is currently active. */
  get isRunning(): boolean {
    return this.running;
  }

  /**
   * Start receiving Standing Query results.
   *
   * Calls `callback` for each message received from the server.
   * Resolves when the connection closes or an error occurs.
   */
  subscribe(callback: ResultCallback): Promise<void> {
    return new Promise((resolve, reject) => {
      this.running = true;
      const url = this.buildUrl();

      try {
        this.ws = new WebSocket(url);
      } catch (err) {
        this.running = false;
        reject(
          new ConnectionError(
            `Cannot create WebSocket: ${(err as Error).message}`,
          ),
        );
        return;
      }

      this.ws.onopen = () => {
        // Connection established
      };

      this.ws.onmessage = (event: MessageEvent) => {
        if (!this.running) return;
        const data = this.parseMessage(event.data);
        if (data) {
          try {
            callback(data);
          } catch (err) {
            console.error('Error in subscription callback:', err);
          }
        }
      };

      this.ws.onerror = () => {
        if (this.running) {
          this.running = false;
          reject(
            new ConnectionError(
              `WebSocket error for standing query ${this.queryId}`,
            ),
          );
        }
      };

      this.ws.onclose = () => {
        if (this.running) {
          this.running = false;
        }
        resolve();
      };
    });
  }

  /** Stop receiving results and close the WebSocket connection. */
  async unsubscribe(): Promise<void> {
    this.running = false;
    if (this.ws) {
      if (
        this.ws.readyState === WebSocket.OPEN ||
        this.ws.readyState === WebSocket.CONNECTING
      ) {
        this.ws.close();
      }
      this.ws = null;
    }
  }

  /**
   * Async iterator that yields each message from the server.
   *
   * @example
   * ```ts
   * const sub = new StandingQuerySubscription(url, id);
   * for await (const data of sub) {
   *   console.log(data);
   * }
   * ```
   */
  async *[Symbol.asyncIterator](): AsyncGenerator<Record<string, unknown>> {
    this.running = true;
    const url = this.buildUrl();

    const queue: Record<string, unknown>[] = [];
    let resolveWait: (() => void) | null = null;
    let rejectWait: ((err: Error) => void) | null = null;
    let done = false;

    try {
      this.ws = new WebSocket(url);
    } catch (err) {
      this.running = false;
      throw new ConnectionError(
        `Cannot create WebSocket: ${(err as Error).message}`,
      );
    }

    this.ws.onmessage = (event: MessageEvent) => {
      if (!this.running) return;
      const data = this.parseMessage(event.data);
      if (data) {
        queue.push(data);
        resolveWait?.();
      }
    };

    this.ws.onerror = () => {
      if (this.running) {
        done = true;
        this.running = false;
        rejectWait?.(
          new ConnectionError(
            `WebSocket error for standing query ${this.queryId}`,
          ),
        );
      }
    };

    this.ws.onclose = () => {
      done = true;
      this.running = false;
      resolveWait?.();
    };

    while (!done) {
      if (queue.length > 0) {
        yield queue.shift()!;
        continue;
      }

      await new Promise<void>((resolve, reject) => {
        resolveWait = resolve;
        rejectWait = reject;
      });
      resolveWait = null;
      rejectWait = null;
    }

    // Drain remaining
    while (queue.length > 0) {
      yield queue.shift()!;
    }
  }

  private parseMessage(data: unknown): Record<string, unknown> | null {
    if (typeof data !== 'string') return null;
    try {
      const parsed = JSON.parse(data);
      if (parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed)) {
        return parsed as Record<string, unknown>;
      }
      return null;
    } catch {
      return null;
    }
  }
}

/**
 * Stream Cypher query results via WebSocket.
 *
 * Connects to `/api/v2/ws/query` and sends Cypher queries, receiving
 * results as streaming messages.
 */
export class CypherStreamSubscription {
  private readonly wsUrl: string;
  private readonly apiKey?: string;
  private ws: WebSocket | null = null;
  private running = false;

  constructor(wsUrl: string, apiKey?: string) {
    this.wsUrl = wsUrl.replace(/\/$/, '');
    this.apiKey = apiKey;
  }

  private buildUrl(): string {
    const base = `${this.wsUrl}/api/v2/ws/query`;
    if (this.apiKey) {
      return `${base}?token=${encodeURIComponent(this.apiKey)}`;
    }
    return base;
  }

  /** Whether the stream is currently active. */
  get isRunning(): boolean {
    return this.running;
  }

  /**
   * Start streaming Cypher query results.
   *
   * @param cypher   Cypher query string to execute.
   * @param callback Function invoked with each result message.
   * @param queryId  Optional client-side query identifier.
   */
  subscribe(
    cypher: string,
    callback: ResultCallback,
    queryId = 'q',
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      this.running = true;
      const url = this.buildUrl();

      try {
        this.ws = new WebSocket(url);
      } catch (err) {
        this.running = false;
        reject(
          new ConnectionError(
            `Cannot create WebSocket: ${(err as Error).message}`,
          ),
        );
        return;
      }

      this.ws.onopen = () => {
        const request = JSON.stringify({ query: cypher, queryId });
        this.ws?.send(request);
      };

      this.ws.onmessage = (event: MessageEvent) => {
        if (!this.running) return;
        const data = this.parseMessage(event.data);
        if (!data) return;

        try {
          callback(data);
        } catch (err) {
          console.error('Error in stream callback:', err);
        }

        // Stop on QueryFinished
        if (data.type === 'QueryFinished') {
          this.running = false;
        }
      };

      this.ws.onerror = () => {
        if (this.running) {
          this.running = false;
          reject(new ConnectionError('WebSocket error for Cypher stream'));
        }
      };

      this.ws.onclose = () => {
        if (this.running) {
          this.running = false;
        }
        resolve();
      };
    });
  }

  /** Stop the stream and close the WebSocket connection. */
  async stop(): Promise<void> {
    this.running = false;
    if (this.ws) {
      if (
        this.ws.readyState === WebSocket.OPEN ||
        this.ws.readyState === WebSocket.CONNECTING
      ) {
        this.ws.close();
      }
      this.ws = null;
    }
  }

  /**
   * Stream Cypher results as an async iterator.
   *
   * @param cypher  Cypher query string to execute.
   * @param queryId Optional client-side query identifier.
   * @yields Each result message from the server.
   */
  async *stream(
    cypher: string,
    queryId = 'q',
  ): AsyncGenerator<Record<string, unknown>> {
    this.running = true;
    const url = this.buildUrl();

    const queue: Record<string, unknown>[] = [];
    let resolveWait: (() => void) | null = null;
    let rejectWait: ((err: Error) => void) | null = null;
    let done = false;

    try {
      this.ws = new WebSocket(url);
    } catch (err) {
      this.running = false;
      throw new ConnectionError(
        `Cannot create WebSocket: ${(err as Error).message}`,
      );
    }

    this.ws.onopen = () => {
      const request = JSON.stringify({ query: cypher, queryId });
      this.ws?.send(request);
    };

    this.ws.onmessage = (event: MessageEvent) => {
      if (!this.running) return;
      const data = this.parseMessage(event.data);
      if (data) {
        queue.push(data);
        resolveWait?.();

        if (data.type === 'QueryFinished') {
          this.running = false;
        }
      }
    };

    this.ws.onerror = () => {
      if (this.running) {
        done = true;
        this.running = false;
        rejectWait?.(new ConnectionError('WebSocket error for Cypher stream'));
      }
    };

    this.ws.onclose = () => {
      done = true;
      this.running = false;
      resolveWait?.();
    };

    while (!done) {
      if (queue.length > 0) {
        const msg = queue.shift()!;
        yield msg;
        if (msg.type === 'QueryFinished') break;
        continue;
      }

      await new Promise<void>((resolve, reject) => {
        resolveWait = resolve;
        rejectWait = reject;
      });
      resolveWait = null;
      rejectWait = null;
    }

    // Drain remaining
    while (queue.length > 0) {
      yield queue.shift()!;
    }
  }

  private parseMessage(data: unknown): Record<string, unknown> | null {
    if (typeof data !== 'string') return null;
    try {
      const parsed = JSON.parse(data);
      if (parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed)) {
        return parsed as Record<string, unknown>;
      }
      return null;
    } catch {
      return null;
    }
  }
}
