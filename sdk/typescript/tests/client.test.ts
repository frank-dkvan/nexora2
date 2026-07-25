/**
 * Tests for the Nexora TypeScript SDK.
 *
 * These tests use Node's built-in `node:test` runner with mocked `fetch`.
 * To run: `npx tsx --test tests/client.test.ts` (requires tsx for TS support).
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strictEqual, deepStrictEqual, ok, throws } from 'node:assert';
import { MockServer } from './mock-server';

import { NexoraClient } from '../src/client';
import {
  NexoraError,
  ConnectionError,
  QueryError,
  NodeNotFoundError,
  AuthenticationError,
  TimeoutError,
} from '../src/errors';

// ---------------------------------------------------------------------------
// hexId static method
// ---------------------------------------------------------------------------

describe('NexoraClient.hexId', () => {
  it('should encode ASCII strings to hex', () => {
    strictEqual(NexoraClient.hexId('abc'), '616263');
  });

  it('should encode common node IDs', () => {
    strictEqual(NexoraClient.hexId('alice'), '616c696365');
    strictEqual(NexoraClient.hexId('1'), '31');
  });

  it('should handle empty strings', () => {
    strictEqual(NexoraClient.hexId(''), '');
  });

  it('should encode non-ASCII characters', () => {
    strictEqual(NexoraClient.hexId('héllo'), '68c3a96c6c6f');
  });
});

// ---------------------------------------------------------------------------
// Error hierarchy
// ---------------------------------------------------------------------------

describe('Error classes', () => {
  it('NexoraError should be an Error subclass', () => {
    const err = new NexoraError('test');
    ok(err instanceof Error);
    ok(err instanceof NexoraError);
    strictEqual(err.message, 'test');
  });

  it('NexoraError should carry statusCode and responseBody', () => {
    const err = new NexoraError('fail', {
      statusCode: 500,
      responseBody: { detail: 'oops' },
    });
    strictEqual(err.statusCode, 500);
    deepStrictEqual(err.responseBody, { detail: 'oops' });
  });

  it('All SDK errors should extend NexoraError', () => {
    ok(new ConnectionError('x') instanceof NexoraError);
    ok(new QueryError('x') instanceof NexoraError);
    ok(new NodeNotFoundError('x') instanceof NexoraError);
    ok(new AuthenticationError('x') instanceof NexoraError);
    ok(new TimeoutError('x') instanceof NexoraError);
  });
});

// ---------------------------------------------------------------------------
// Client construction
// ---------------------------------------------------------------------------

describe('NexoraClient construction', () => {
  it('should use default base URL', () => {
    const c = new NexoraClient();
    strictEqual(c.baseUrl, 'http://localhost:8080');
  });

  it('should accept string base URL', () => {
    const c = new NexoraClient('http://example.com:9090');
    strictEqual(c.baseUrl, 'http://example.com:9090');
  });

  it('should strip trailing slash from base URL', () => {
    const c = new NexoraClient('http://example.com/');
    strictEqual(c.baseUrl, 'http://example.com');
  });

  it('should accept config object', () => {
    const c = new NexoraClient({
      baseUrl: 'http://graph.local',
      apiKey: 'secret',
      timeout: 5000,
      maxRetries: 1,
    });
    strictEqual(c.baseUrl, 'http://graph.local');
    strictEqual(c.apiKey, 'secret');
    strictEqual(c.timeout, 5000);
    strictEqual(c.maxRetries, 1);
  });

  it('should accept string URL + apiKey', () => {
    const c = new NexoraClient('http://localhost:8080', 'my-token');
    strictEqual(c.apiKey, 'my-token');
  });
});

// ---------------------------------------------------------------------------
// API methods (with mock server)
// ---------------------------------------------------------------------------

describe('NexoraClient API methods', () => {
  let server: MockServer;

  beforeEach(() => {
    server = new MockServer();
  });

  afterEach(() => {
    server.restore();
  });

  // -- Query APIs --------------------------------------------------------

  describe('query / cypher', () => {
    it('should execute a Cypher query and normalise rows', async () => {
      server.mock('POST', '/api/v2/query/cypher', {
        columns: ['name', 'age'],
        rows: [['Alice', 30], ['Bob', 25]],
      });

      const client = new NexoraClient('http://localhost:8080');
      const result = await client.query('MATCH (n) RETURN n.name, n.age');

      deepStrictEqual(result.columns, ['name', 'age']);
      deepStrictEqual(result.rows, [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
      ]);
    });

    it('cypher() should be an alias for query()', async () => {
      server.mock('POST', '/api/v2/query/cypher', {
        columns: ['n'],
        rows: [[{ id: 'x' }]],
      });

      const client = new NexoraClient();
      const result = await client.cypher('MATCH (n) RETURN n');
      strictEqual(result.rows.length, 1);
    });

    it('queryOne should return first row or undefined', async () => {
      server.mock('POST', '/api/v2/query/cypher', {
        columns: ['name'],
        rows: [['Alice']],
      });

      const client = new NexoraClient();
      const row = await client.queryOne('MATCH (n) RETURN n.name LIMIT 1');
      deepStrictEqual(row, { name: 'Alice' });
    });
  });

  describe('sql', () => {
    it('should execute a SQL query', async () => {
      server.mock('POST', '/api/v2/query/sql', {
        columns: ['id', 'name'],
        rows: [[1, 'Alice']],
        row_count: 1,
      });

      const client = new NexoraClient();
      const result = await client.sql('SELECT * FROM nodes LIMIT 1');
      strictEqual(result.row_count, 1);
      deepStrictEqual(result.columns, ['id', 'name']);
    });
  });

  describe('explain', () => {
    it('should return an execution plan', async () => {
      server.mock('POST', '/api/v2/query/explain', {
        query: 'MATCH (n) RETURN n',
        plan: 'Scan AllNodes',
        estimated_cost: 10,
      });

      const client = new NexoraClient();
      const result = await client.explain('MATCH (n) RETURN n');
      strictEqual(result.plan, 'Scan AllNodes');
    });
  });

  // -- Graph APIs --------------------------------------------------------

  describe('property operations', () => {
    it('should set a property', async () => {
      server.mock('PUT', '/api/v2/graph/node/616c696365/property/name', {
        status: 'ok',
      });

      const client = new NexoraClient();
      await client.setProperty('alice', 'name', 'Alice');

      const call = server.calls[0];
      strictEqual(call.method, 'PUT');
      strictEqual(call.path, '/api/v2/graph/node/616c696365/property/name');
      deepStrictEqual(call.body, { value: 'Alice' });
    });

    it('should get a property', async () => {
      server.mock('GET', '/api/v2/graph/node/616c696365/property/name', {
        value: 'Alice',
      });

      const client = new NexoraClient();
      const val = await client.getProperty('alice', 'name');
      strictEqual(val, 'Alice');
    });

    it('should return undefined for missing property', async () => {
      server.mock('GET', '/api/v2/graph/node/616c696365/property/missing', {
        not_found: true,
      });

      const client = new NexoraClient();
      const val = await client.getProperty('alice', 'missing');
      strictEqual(val, undefined);
    });
  });

  describe('edge operations', () => {
    it('should add an edge with hex-encoded target', async () => {
      server.mock('POST', '/api/v2/graph/node/616c696365/edges', {});

      const client = new NexoraClient();
      await client.addEdge('alice', 'KNOWS', 'bob');

      const call = server.calls[0];
      strictEqual(call.path, '/api/v2/graph/node/616c696365/edges');
      deepStrictEqual(call.body, {
        label: 'KNOWS',
        target: '626f62',
      });
    });

    it('should add an edge with properties', async () => {
      server.mock('POST', '/api/v2/graph/node/616c696365/edges', {});

      const client = new NexoraClient();
      await client.addEdge('alice', 'KNOWS', 'bob', { since: 2020 });

      deepStrictEqual(server.calls[0].body, {
        label: 'KNOWS',
        target: '626f62',
        properties: { since: 2020 },
      });
    });

    it('should get edges', async () => {
      server.mock('GET', '/api/v2/graph/node/616c696365/edges', {
        edges: [
          { edge_type: 'KNOWS', direction: 'out', other: '626f62' },
          { edge_type: 'LIKES', direction: 'in', other: '63617231' },
        ],
      });

      const client = new NexoraClient();
      const edges = await client.getEdges('alice');
      strictEqual(edges.length, 2);
    });

    it('should filter edges by direction', async () => {
      server.mock('GET', '/api/v2/graph/node/616c696365/edges', {
        edges: [
          { edge_type: 'KNOWS', direction: 'out', other: '626f62' },
          { edge_type: 'LIKES', direction: 'in', other: '63617231' },
        ],
      });

      const client = new NexoraClient();
      const outEdges = await client.getEdges('alice', 'out');
      strictEqual(outEdges.length, 1);
      strictEqual(outEdges[0].edge_type, 'KNOWS');
    });
  });

  // -- Standing Queries --------------------------------------------------

  describe('standing queries', () => {
    it('should create a standing query and return ID', async () => {
      server.mock('POST', '/api/v2/standing-query', { id: 'sq-123' });

      const client = new NexoraClient();
      const id = await client.createStandingQuery(
        { type: 'PropertyFilter', key: 'speed', condition: { type: 'GreaterThan', value: 100 } },
        'fast_alert',
      );
      strictEqual(id, 'sq-123');
    });

    it('should list standing queries', async () => {
      server.mock('GET', '/api/v2/standing-query', {
        standing_queries: [
          { id: 'sq-1', name: 'a', match_count: 5 },
          { id: 'sq-2', name: 'b', match_count: 0 },
        ],
      });

      const client = new NexoraClient();
      const list = await client.listStandingQueries();
      strictEqual(list.length, 2);
    });

    it('should delete a standing query', async () => {
      server.mock('DELETE', '/api/v2/standing-query/sq-1', {});

      const client = new NexoraClient();
      await client.deleteStandingQuery('sq-1');
      strictEqual(server.calls[0].path, '/api/v2/standing-query/sq-1');
    });
  });

  // -- Vector ------------------------------------------------------------

  describe('vector operations', () => {
    it('should index a vector with hex-encoded qid', async () => {
      server.mock('POST', '/api/v2/vector/index', {
        status: 'indexed',
        qid: '616c696365',
        index_size: 1,
      });

      const client = new NexoraClient();
      const resp = await client.vectorIndex('alice', [1, 2, 3]);

      deepStrictEqual(server.calls[0].body, {
        qid: '616c696365',
        vector: [1, 2, 3],
      });
      strictEqual(resp.index_size, 1);
    });

    it('should search vectors', async () => {
      server.mock('POST', '/api/v2/vector/search', {
        query: null,
        k: 5,
        neighbors: [{ qid: '616c696365', distance: 0.1 }],
      });

      const client = new NexoraClient();
      const resp = await client.vectorSearch([1, 2, 3], 5);
      strictEqual(resp.neighbors.length, 1);
    });
  });

  // -- Health & System ---------------------------------------------------

  describe('health & system', () => {
    it('should check health', async () => {
      server.mock('GET', '/api/v2/health', { status: 'ok', active_nodes: 42 });

      const client = new NexoraClient();
      const h = await client.health();
      strictEqual(h.status, 'ok');
      strictEqual(h.active_nodes, 42);
    });

    it('should get system info', async () => {
      server.mock('GET', '/api/v2/system/info', {
        version: '1.0.0',
        num_shards: 4,
      });

      const client = new NexoraClient();
      const info = await client.systemInfo();
      strictEqual(info.version, '1.0.0');
    });
  });

  // -- Error handling ----------------------------------------------------

  describe('error handling', () => {
    it('should throw AuthenticationError on 401', async () => {
      server.mock('GET', '/api/v2/health', { error: 'Unauthorized' }, 401);

      const client = new NexoraClient();
      await throws(
        () => client.health(),
        (err: unknown) => err instanceof AuthenticationError,
      );
    });

    it('should throw NodeNotFoundError on 404', async () => {
      server.mock('GET', '/api/v2/standing-query/sq-x', { error: 'Not found' }, 404);

      const client = new NexoraClient();
      await throws(
        () => client.getStandingQuery('sq-x'),
        (err: unknown) => err instanceof NodeNotFoundError,
      );
    });

    it('should throw QueryError on 400', async () => {
      server.mock('POST', '/api/v2/query/cypher', { error: 'Bad query' }, 400);

      const client = new NexoraClient();
      await throws(
        () => client.query('INVALID CYPHER'),
        (err: unknown) => err instanceof QueryError,
      );
    });
  });

  // -- Auth --------------------------------------------------------------

  describe('auth', () => {
    it('should generate a token', async () => {
      server.mock('POST', '/api/v2/auth/token', {
        token: 'tok-abc',
        role: 'admin',
      });

      const client = new NexoraClient();
      const resp = await client.generateToken('admin', 3600);
      strictEqual(resp.token, 'tok-abc');

      deepStrictEqual(server.calls[0].body, {
        role: 'admin',
        expires_in: 3600,
      });
    });

    it('should send Authorization header when apiKey is set', async () => {
      server.mock('GET', '/api/v2/health', { status: 'ok' });

      const client = new NexoraClient('http://localhost:8080', 'my-secret');
      await client.health();
      strictEqual(server.calls[0].headers['authorization'], 'Bearer my-secret');
    });
  });
});
