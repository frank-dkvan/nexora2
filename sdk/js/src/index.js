/**
 * Nexora JavaScript/TypeScript Driver SDK
 * ==========================================
 *
 * A lightweight client for the Nexora streaming graph database.
 *
 * Usage (Node.js):
 *   const { NexoraClient } = require('./src/index.js');
 *
 *   const client = new NexoraClient('http://localhost:8080');
 *
 *   // Create a node
 *   await client.setProperty('person-1', 'name', 'Alice');
 *
 *   // Read properties
 *   const name = await client.getProperty('person-1', 'name');
 *
 *   // Run Cypher
 *   const results = await client.cypher('MATCH (n) RETURN n LIMIT 10');
 *
 * Usage (browser/ESM):
 *   import { NexoraClient } from './src/index.js';
 */

'use strict';

/**
 * @typedef {Object} NexoraClientOptions
 * @property {string} [authSecret] - Optional HMAC-SHA256 auth secret.
 */

/**
 * @typedef {Object} GraphResult
 * @property {string[]} columns - Column names.
 * @property {any[][]} rows - Result rows.
 */

/**
 * @typedef {Object} StandingQuery
 * @property {string} id - Standing query ID.
 * @property {string} name - Standing query name.
 * @property {number} match_count - Number of matches.
 */

/**
 * @typedef {Object} HealthStatus
 * @property {string} status - Server status.
 * @property {number} active_nodes - Active node count.
 * @property {number} shards - Shard count.
 * @property {number} standing_queries - Standing query count.
 * @property {string} mode - Server mode.
 */

/**
 * Error class for Nexora client errors.
 */
class NexoraError extends Error {
  constructor(message, statusCode) {
    super(message);
    this.name = 'NexoraError';
    this.statusCode = statusCode;
  }
}

/**
 * Client for the Nexora streaming graph database.
 */
class NexoraClient {
  /**
   * Create a new Nexora client.
   * @param {string} baseUrl - Base URL of the Nexora server.
   * @param {NexoraClientOptions} [options] - Client options.
   */
  constructor(baseUrl = 'http://localhost:8080', options = {}) {
    this.baseUrl = baseUrl.replace(/\/$/, '');
    this.authSecret = options.authSecret || null;
  }

  /**
   * Convert a string node ID to hex encoding.
   * @param {string} nodeId - The node ID string.
   * @returns {string} Hex-encoded node ID.
   * @private
   */
  _hexId(nodeId) {
    let hex = '';
    for (let i = 0; i < nodeId.length; i++) {
      hex += nodeId.charCodeAt(i).toString(16).padStart(2, '0');
    }
    return hex;
  }

  /**
   * Make an HTTP request to the Nexora API.
   * @param {string} method - HTTP method.
   * @param {string} path - API path.
   * @param {Object} [body] - Request body.
   * @returns {Promise<Object>} Response JSON.
   * @private
   */
  async _request(method, path, body) {
    const url = this.baseUrl + path;
    const headers = { 'Content-Type': 'application/json' };

    const opts = { method, headers };
    if (body !== undefined) {
      opts.body = JSON.stringify(body);
    }

    let resp;
    try {
      resp = await fetch(url, opts);
    } catch (e) {
      throw new NexoraError('Connection failed: ' + e.message);
    }

    if (!resp.ok) {
      const text = await resp.text().catch(() => '');
      throw new NexoraError(`HTTP ${resp.status}: ${text}`, resp.status);
    }

    return resp.json();
  }

  // ==================== Node Operations ====================

  /**
   * Set a property on a node.
   * @param {string} nodeId - Node identifier.
   * @param {string} key - Property key.
   * @param {any} value - Property value.
   * @returns {Promise<Object>} Response from server.
   */
  async setProperty(nodeId, key, value) {
    const qid = this._hexId(nodeId);
    return this._request('PUT', `/api/v2/graph/node/${qid}/property/${key}`, { value });
  }

  /**
   * Get a property from a node.
   * @param {string} nodeId - Node identifier.
   * @param {string} key - Property key.
   * @returns {Promise<any>} Property value, or null if not found.
   */
  async getProperty(nodeId, key) {
    const qid = this._hexId(nodeId);
    const result = await this._request('GET', `/api/v2/graph/node/${qid}/property/${key}`);
    return result.value !== undefined ? result.value : null;
  }

  /**
   * Get all edges of a node.
   * @param {string} nodeId - Node identifier.
   * @returns {Promise<Array>} Array of edge objects.
   */
  async getEdges(nodeId) {
    const qid = this._hexId(nodeId);
    const result = await this._request('GET', `/api/v2/graph/node/${qid}/edges`);
    return result.edges || [];
  }

  /**
   * Create an edge between two nodes.
   * @param {string} source - Source node ID.
   * @param {string} edgeType - Edge type (e.g., "KNOWS").
   * @param {string} target - Target node ID.
   * @param {string} [direction='out'] - Edge direction.
   * @returns {Promise<Object>} Response from server.
   */
  async addEdge(source, edgeType, target, direction = 'out') {
    const qid = this._hexId(source);
    return this._request('POST', `/api/v2/graph/node/${qid}/edges`, {
      edge_type: edgeType,
      target: this._hexId(target),
      direction,
    });
  }

  // ==================== Cypher Query ====================

  /**
   * Execute a Cypher query.
   * @param {string} query - Cypher query string.
   * @returns {Promise<GraphResult>} Query result with columns and rows.
   */
  async cypher(query) {
    return this._request('POST', '/api/v2/query/cypher', { query });
  }

  // ==================== Standing Queries ====================

  /**
   * List all standing queries.
   * @returns {Promise<StandingQuery[]>} Array of standing queries.
   */
  async listStandingQueries() {
    const result = await this._request('GET', '/api/v2/standing-query');
    return result.standing_queries || [];
  }

  /**
   * Register a standing query.
   * @param {string} name - Standing query name.
   * @param {string} key - Property key to monitor.
   * @param {string} condition - "GreaterThan", "LessThan", "Equals", or "Contains".
   * @param {any} [value] - Threshold value.
   * @returns {Promise<Object>} Response with standing query ID.
   */
  async registerStandingQuery(name, key, condition, value) {
    const body = {
      name,
      pattern: {
        type: 'PropertyFilter',
        key,
        condition: { type: condition },
      },
    };
    if (value !== undefined) {
      body.pattern.condition.value = value;
    }
    return this._request('POST', '/api/v2/standing-query', body);
  }

  /**
   * Delete a standing query.
   * @param {string} sqId - Standing query ID.
   * @returns {Promise<Object>} Response from server.
   */
  async deleteStandingQuery(sqId) {
    return this._request('DELETE', `/api/v2/standing-query/${sqId}`);
  }

  // ==================== Health & Ingest ====================

  /**
   * Get server health status.
   * @returns {Promise<HealthStatus>} Health status.
   */
  async health() {
    return this._request('GET', '/api/v2/health');
  }

  /**
   * Ingest a JSONL file into the graph.
   * @param {string} path - Path to the JSONL file.
   * @param {string} [idField='id'] - Field name to use as node ID.
   * @returns {Promise<Object>} Ingest result.
   */
  async ingestFile(path, idField = 'id') {
    return this._request('POST', '/api/v2/ingest/file', { path, id_field: idField });
  }
}

// CommonJS export
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { NexoraClient, NexoraError };
}

// ES module export
if (typeof exports !== 'undefined') {
  exports.NexoraClient = NexoraClient;
  exports.NexoraError = NexoraError;
}
