/**
 * TypeScript type definitions for Nexora Driver SDK.
 */

export interface NexoraClientOptions {
  authSecret?: string;
}

export interface GraphResult {
  columns: string[];
  rows: any[][];
}

export interface StandingQuery {
  id: string;
  name: string;
  match_count: number;
}

export interface HealthStatus {
  status: string;
  active_nodes: number;
  shards: number;
  standing_queries: number;
  mode: string;
}

export interface EdgeInfo {
  edge_type: string;
  direction: string;
  other: string;
}

export declare class NexoraError extends Error {
  statusCode?: number;
}

export declare class NexoraClient {
  constructor(baseUrl?: string, options?: NexoraClientOptions);

  setProperty(nodeId: string, key: string, value: any): Promise<object>;
  getProperty(nodeId: string, key: string): Promise<any>;
  getEdges(nodeId: string): Promise<EdgeInfo[]>;
  addEdge(source: string, edgeType: string, target: string, direction?: string): Promise<object>;

  cypher(query: string): Promise<GraphResult>;

  listStandingQueries(): Promise<StandingQuery[]>;
  registerStandingQuery(name: string, key: string, condition: string, value?: any): Promise<object>;
  deleteStandingQuery(sqId: string): Promise<object>;

  health(): Promise<HealthStatus>;
  ingestFile(path: string, idField?: string): Promise<object>;
}
