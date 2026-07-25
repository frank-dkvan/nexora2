/**
 * Minimal mock server for testing — intercepts global `fetch`.
 *
 * Usage:
 *   const server = new MockServer();
 *   server.mock('GET', '/api/v2/health', { status: 'ok' });
 *   // ... run test ...
 *   server.restore();
 */

interface MockEntry {
  method: string;
  path: string;
  status: number;
  body: unknown;
}

interface RecordedCall {
  method: string;
  path: string;
  body: unknown;
  headers: Record<string, string>;
}

export class MockServer {
  private entries: MockEntry[] = [];
  private originalFetch: typeof globalThis.fetch;
  readonly calls: RecordedCall[] = [];

  constructor() {
    this.originalFetch = globalThis.fetch;
    globalThis.fetch = this.handleFetch.bind(this) as typeof globalThis.fetch;
  }

  /**
   * Register a mock response.
   * @param method HTTP method.
   * @param path   API path (e.g. `/api/v2/health`).
   * @param body   Response body (will be JSON-stringified).
   * @param status HTTP status code (default 200).
   */
  mock(method: string, path: string, body: unknown, status = 200): void {
    this.entries.push({ method, path, status, body });
  }

  restore(): void {
    globalThis.fetch = this.originalFetch;
  }

  private handleFetch(
    input: string | URL | Request,
    init?: RequestInit,
  ): Promise<Response> {
    const url = typeof input === 'string' ? input : input.toString();
    const method = (init?.method ?? 'GET').toUpperCase();

    // Extract path from URL
    let path: string;
    try {
      path = new URL(url).pathname + new URL(url).search;
    } catch {
      path = url;
    }

    // Parse request body
    let reqBody: unknown = undefined;
    if (init?.body) {
      try {
        reqBody = JSON.parse(init.body as string);
      } catch {
        reqBody = init.body;
      }
    }

    // Parse request headers
    const headers: Record<string, string> = {};
    if (init?.headers) {
      const h = init.headers;
      if (h instanceof Headers) {
        h.forEach((v, k) => { headers[k.toLowerCase()] = v; });
      } else if (Array.isArray(h)) {
        for (const [k, v] of h) {
          headers[k.toLowerCase()] = v;
        }
      } else {
        for (const [k, v] of Object.entries(h)) {
          headers[k.toLowerCase()] = v;
        }
      }
    }

    this.calls.push({ method, path, body: reqBody, headers });

    // Find matching mock entry
    const entry = this.entries.find(
      (e) => e.method.toUpperCase() === method && e.path === path,
    );

    if (!entry) {
      return Promise.resolve(
        new Response(JSON.stringify({ error: 'No mock for this route' }), {
          status: 404,
          headers: { 'Content-Type': 'application/json' },
        }),
      );
    }

    return Promise.resolve(
      new Response(JSON.stringify(entry.body), {
        status: entry.status,
        headers: { 'Content-Type': 'application/json' },
      }),
    );
  }
}
