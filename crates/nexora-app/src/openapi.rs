//! OpenAPI/Swagger documentation generation.
//!
//! Provides complete API documentation with:
//! - All endpoints with request/response schemas
//! - Authentication schemes (Bearer token)
//! - Example requests and responses
//! - Interactive Swagger UI

use serde_json::json;

/// Complete OpenAPI 3.0 specification for Nexora-RS API
pub fn openapi_spec() -> serde_json::Value {
    json!({
        "openapi": "3.0.3",
        "info": {
            "title": "Nexora-RS API",
            "description": "Streaming graph database with Cypher query support",
            "version": env!("CARGO_PKG_VERSION"),
            "contact": {
                "name": "Nexora-RS",
                "url": "https://github.com/frank-dkvan/deepstreaming"
            },
            "license": {
                "name": "Apache 2.0",
                "url": "https://www.apache.org/licenses/LICENSE-2.0.html"
            }
        },
        "servers": [
            {
                "url": "http://localhost:8080",
                "description": "Local development server"
            }
        ],
        "tags": [
            {
                "name": "health",
                "description": "Health check and readiness endpoints"
            },
            {
                "name": "query",
                "description": "Cypher and SQL query execution"
            },
            {
                "name": "graph",
                "description": "Graph node and edge operations"
            },
            {
                "name": "ingest",
                "description": "Data ingestion endpoints"
            },
            {
                "name": "streams",
                "description": "Stream source management (Kafka)"
            },
            {
                "name": "standing-query",
                "description": "Standing query (continuous query) management"
            },
            {
                "name": "vector",
                "description": "Vector similarity search (HNSW)"
            },
            {
                "name": "recipes",
                "description": "Recipe management and execution"
            },
            {
                "name": "storage",
                "description": "Tiered storage management"
            },
            {
                "name": "udf",
                "description": "User-defined functions"
            },
            {
                "name": "websocket",
                "description": "WebSocket real-time endpoints"
            },
            {
                "name": "system",
                "description": "System information and configuration"
            },
            {
                "name": "cluster",
                "description": "Cluster management"
            },
            {
                "name": "auth",
                "description": "Authentication and authorization"
            },
            {
                "name": "metrics",
                "description": "Prometheus metrics"
            }
        ],
        "paths": {
            "/api/health": {
                "get": {
                    "tags": ["health"],
                    "summary": "Health check",
                    "description": "Returns overall system health status",
                    "operationId": "getHealth",
                    "responses": {
                        "200": {
                            "description": "System is healthy",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/HealthResponse"
                                    },
                                    "example": {
                                        "status": "healthy",
                                        "mode": "single-node",
                                        "active_nodes": 1234,
                                        "shards": 256,
                                        "standing_queries": 5,
                                        "readiness": "ready",
                                        "liveness": "alive",
                                        "durability": "ephemeral",
                                        "version": "0.1.0",
                                        "uptime_seconds": 3600
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/api/health/ready": {
                "get": {
                    "tags": ["health"],
                    "summary": "Readiness check",
                    "description": "Kubernetes-style readiness probe",
                    "operationId": "getReadiness",
                    "responses": {
                        "200": {
                            "description": "Service is ready to accept traffic",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "ready": {"type": "boolean"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/api/health/live": {
                "get": {
                    "tags": ["health"],
                    "summary": "Liveness check",
                    "description": "Kubernetes-style liveness probe",
                    "operationId": "getLiveness",
                    "responses": {
                        "200": {
                            "description": "Service is alive",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "alive": {"type": "boolean"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/api/query/cypher": {
                "post": {
                    "tags": ["query"],
                    "summary": "Execute Cypher query",
                    "description": "Execute a Cypher graph query and return results",
                    "operationId": "executeCypher",
                    "security": [{"bearerAuth": []}],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "$ref": "#/components/schemas/CypherRequest"
                                },
                                "examples": {
                                    "simple": {
                                        "summary": "Simple MATCH query",
                                        "value": {
                                            "query": "MATCH (n:Person) RETURN n.name LIMIT 10"
                                        }
                                    },
                                    "create": {
                                        "summary": "CREATE query",
                                        "value": {
                                            "query": "CREATE (n:Person {name: 'Alice', age: 30}) RETURN n"
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Query executed successfully",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/CypherResponse"
                                    }
                                }
                            }
                        },
                        "400": {
                            "description": "Invalid query syntax"
                        },
                        "401": {
                            "description": "Unauthorized"
                        }
                    }
                }
            },
            "/api/query/sql": {
                "post": {
                    "tags": ["query"],
                    "summary": "Execute SQL query",
                    "description": "Execute a SQL query over graph data",
                    "operationId": "executeSql",
                    "security": [{"bearerAuth": []}],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "query": {"type": "string"}
                                    },
                                    "required": ["query"]
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Query executed successfully"
                        },
                        "400": {
                            "description": "Invalid SQL syntax"
                        }
                    }
                }
            },
            "/api/graph/node/{qid}": {
                "get": {
                    "tags": ["graph"],
                    "summary": "Get node by ID",
                    "description": "Retrieve a single node with all its properties",
                    "operationId": "getNode",
                    "security": [{"bearerAuth": []}],
                    "parameters": [
                        {
                            "name": "qid",
                            "in": "path",
                            "required": true,
                            "schema": {"type": "string"},
                            "description": "Node ID (NexoraId)",
                            "example": "6e6f6465-3132-3300-0000-000000000000"
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Node found",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/NodeResponse"
                                    }
                                }
                            }
                        },
                        "404": {
                            "description": "Node not found"
                        }
                    }
                },
                "post": {
                    "tags": ["graph"],
                    "summary": "Set node property",
                    "description": "Set a property on a node (creates node if doesn't exist)",
                    "operationId": "setNodeProperty",
                    "security": [{"bearerAuth": []}],
                    "parameters": [
                        {
                            "name": "qid",
                            "in": "path",
                            "required": true,
                            "schema": {"type": "string"}
                        }
                    ],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "key": {"type": "string"},
                                        "value": {"type": "object"}
                                    }
                                },
                                "example": {
                                    "key": "age",
                                    "value": 30
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Property set successfully"
                        }
                    }
                }
            },
            "/api/graph/node/{qid}/edges": {
                "get": {
                    "tags": ["graph"],
                    "summary": "Get node edges",
                    "description": "Get all edges connected to a node",
                    "operationId": "getEdges",
                    "security": [{"bearerAuth": []}],
                    "parameters": [
                        {
                            "name": "qid",
                            "in": "path",
                            "required": true,
                            "schema": {"type": "string"}
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Edges retrieved",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {
                                            "$ref": "#/components/schemas/Edge"
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "post": {
                    "tags": ["graph"],
                    "summary": "Add edge",
                    "description": "Create an edge between two nodes",
                    "operationId": "addEdge",
                    "security": [{"bearerAuth": []}],
                    "parameters": [
                        {
                            "name": "qid",
                            "in": "path",
                            "required": true,
                            "schema": {"type": "string"},
                            "description": "Source node ID"
                        }
                    ],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "$ref": "#/components/schemas/AddEdgeRequest"
                                },
                                "example": {
                                    "label": "KNOWS",
                                    "target": "6e6f6465-3233-0000-0000-000000000000",
                                    "properties": {
                                        "since": 2020
                                    }
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Edge created successfully"
                        }
                    }
                }
            },
            "/api/standing-query": {
                "get": {
                    "tags": ["standing-query"],
                    "summary": "List standing queries",
                    "description": "Get all registered standing queries",
                    "operationId": "listStandingQueries",
                    "security": [{"bearerAuth": []}],
                    "responses": {
                        "200": {
                            "description": "List of standing queries",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {
                                            "$ref": "#/components/schemas/StandingQuery"
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "post": {
                    "tags": ["standing-query"],
                    "summary": "Create standing query",
                    "description": "Register a new standing query for continuous pattern matching",
                    "operationId": "createStandingQuery",
                    "security": [{"bearerAuth": []}],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "$ref": "#/components/schemas/CreateStandingQueryRequest"
                                }
                            }
                        }
                    },
                    "responses": {
                        "201": {
                            "description": "Standing query created",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "string"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/api/ingest/file": {
                "post": {
                    "tags": ["ingest"],
                    "summary": "Start file ingest",
                    "description": "Ingest data from a file (JSON, CSV, etc.)",
                    "operationId": "startFileIngest",
                    "security": [{"bearerAuth": []}],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "name": {"type": "string"},
                                        "path": {"type": "string"},
                                        "format": {
                                            "type": "string",
                                            "enum": ["json", "csv"]
                                        }
                                    },
                                    "required": ["name", "path", "format"]
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Ingest started"
                        },
                        "400": {
                            "description": "Invalid request"
                        },
                        "403": {
                            "description": "Path not allowed (security)"
                        }
                    }
                }
            },
            "/api/system/info": {
                "get": {
                    "tags": ["system"],
                    "summary": "System information",
                    "description": "Get system configuration and runtime information",
                    "operationId": "getSystemInfo",
                    "security": [{"bearerAuth": []}],
                    "responses": {
                        "200": {
                            "description": "System information",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/SystemInfo"
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/api/auth/token": {
                "post": {
                    "tags": ["auth"],
                    "summary": "Generate auth token",
                    "description": "Generate a new authentication token (admin only)",
                    "operationId": "generateToken",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "role": {
                                            "type": "string",
                                            "enum": ["admin", "operator", "viewer"]
                                        },
                                        "expires_in": {
                                            "type": "integer",
                                            "description": "Token validity in seconds"
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Token generated",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "token": {"type": "string"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/metrics": {
                "get": {
                    "tags": ["metrics"],
                    "summary": "Prometheus metrics",
                    "description": "Prometheus-format metrics endpoint",
                    "operationId": "getMetrics",
                    "responses": {
                        "200": {
                            "description": "Metrics in Prometheus format",
                            "content": {
                                "text/plain": {
                                    "schema": { "type": "string" }
                                }
                            }
                        }
                    }
                }
            },
            "/api/metrics": {
                "get": {
                    "tags": ["metrics"],
                    "summary": "Metrics JSON",
                    "description": "System metrics in JSON format",
                    "operationId": "getMetricsJson",
                    "responses": { "200": { "description": "Metrics JSON" } }
                }
            },
            "/api/query/sql": {
                "post": {
                    "tags": ["query"],
                    "summary": "Execute SQL query",
                    "description": "Execute a SQL query over graph data",
                    "operationId": "executeSql",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": { "query": { "type": "string" } },
                                    "required": ["query"]
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": { "description": "Query executed successfully" },
                        "400": { "description": "Invalid SQL syntax" }
                    }
                }
            },
            "/api/graph/history": {
                "get": {
                    "tags": ["graph"],
                    "summary": "Time-travel history",
                    "description": "Query historical graph state at a specific timestamp",
                    "operationId": "timeTravel",
                    "parameters": [
                        {
                            "name": "qid",
                            "in": "query",
                            "required": true,
                            "schema": { "type": "string" }
                        },
                        {
                            "name": "as_of",
                            "in": "query",
                            "required": true,
                            "schema": { "type": "integer", "format": "int64" },
                            "description": "Unix timestamp in microseconds"
                        }
                    ],
                    "responses": {
                        "200": { "description": "Historical node state" },
                        "400": { "description": "Missing parameters" }
                    }
                }
            },
            "/api/graph/node/{qid}/property/{key}": {
                "get": {
                    "tags": ["graph"],
                    "summary": "Get property",
                    "description": "Get a single property value from a node",
                    "operationId": "getProperty",
                    "parameters": [
                        { "name": "qid", "in": "path", "required": true, "schema": { "type": "string" } },
                        { "name": "key", "in": "path", "required": true, "schema": { "type": "string" } }
                    ],
                    "responses": { "200": { "description": "Property value" } }
                },
                "put": {
                    "tags": ["graph"],
                    "summary": "Set property",
                    "description": "Set a property value on a node",
                    "operationId": "setProperty",
                    "parameters": [
                        { "name": "qid", "in": "path", "required": true, "schema": { "type": "string" } },
                        { "name": "key", "in": "path", "required": true, "schema": { "type": "string" } }
                    ],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "type": "object" }
                            }
                        }
                    },
                    "responses": { "200": { "description": "Property set" } }
                }
            },
            "/api/vector/index": {
                "post": {
                    "tags": ["vector"],
                    "summary": "Index vector",
                    "description": "Add a vector embedding to the HNSW index for similarity search",
                    "operationId": "vectorIndex",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "qid": { "type": "string" },
                                        "vector": {
                                            "type": "array",
                                            "items": { "type": "number" }
                                        }
                                    },
                                    "required": ["qid", "vector"]
                                }
                            }
                        }
                    },
                    "responses": { "200": { "description": "Vector indexed" } }
                }
            },
            "/api/vector/search": {
                "post": {
                    "tags": ["vector"],
                    "summary": "Vector search",
                    "description": "Find nearest neighbors by vector similarity (k-NN via HNSW)",
                    "operationId": "vectorSearch",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "vector": { "type": "array", "items": { "type": "number" } },
                                        "k": { "type": "integer", "default": 10 }
                                    },
                                    "required": ["vector"]
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Search results",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "results": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "qid": { "type": "string" },
                                                        "distance": { "type": "number" }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/api/vector/node/{qid}": {
                "get": {
                    "tags": ["vector"],
                    "summary": "Get vector",
                    "description": "Get the vector embedding for a node",
                    "operationId": "vectorGetNode",
                    "parameters": [
                        { "name": "qid", "in": "path", "required": true, "schema": { "type": "string" } }
                    ],
                    "responses": { "200": { "description": "Vector data" } }
                },
                "delete": {
                    "tags": ["vector"],
                    "summary": "Delete vector",
                    "description": "Remove a vector from the HNSW index",
                    "operationId": "vectorDeleteNode",
                    "parameters": [
                        { "name": "qid", "in": "path", "required": true, "schema": { "type": "string" } }
                    ],
                    "responses": { "200": { "description": "Vector removed" } }
                }
            },
            "/api/ingest": {
                "get": {
                    "tags": ["ingest"],
                    "summary": "List ingest tasks",
                    "description": "Get all active and completed ingest tasks",
                    "operationId": "listIngests",
                    "responses": {
                        "200": {
                            "description": "List of ingest tasks",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": { "$ref": "#/components/schemas/IngestTask" }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/api/ingest/{name}": {
                "delete": {
                    "tags": ["ingest"],
                    "summary": "Cancel ingest",
                    "description": "Cancel an active ingest task by name",
                    "operationId": "deleteIngest",
                    "parameters": [
                        { "name": "name", "in": "path", "required": true, "schema": { "type": "string" } }
                    ],
                    "responses": { "200": { "description": "Ingest cancelled" } }
                }
            },
            "/api/streams": {
                "get": {
                    "tags": ["streams"],
                    "summary": "List streams",
                    "description": "Get all active stream sources (Kafka, etc.)",
                    "operationId": "listStreams",
                    "responses": {
                        "200": {
                            "description": "List of stream sources",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": { "$ref": "#/components/schemas/StreamSource" }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/api/streams/kafka": {
                "post": {
                    "tags": ["streams"],
                    "summary": "Start Kafka stream",
                    "description": "Start consuming from a Kafka topic",
                    "operationId": "startKafkaStream",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "name": { "type": "string" },
                                        "brokers": { "type": "string" },
                                        "topic": { "type": "string" },
                                        "group_id": { "type": "string" }
                                    },
                                    "required": ["name", "brokers", "topic"]
                                }
                            }
                        }
                    },
                    "responses": { "200": { "description": "Stream started" } }
                }
            },
            "/api/streams/{name}": {
                "delete": {
                    "tags": ["streams"],
                    "summary": "Stop stream",
                    "description": "Stop and remove a stream source",
                    "operationId": "deleteStream",
                    "parameters": [
                        { "name": "name", "in": "path", "required": true, "schema": { "type": "string" } }
                    ],
                    "responses": { "200": { "description": "Stream stopped" } }
                }
            },
            "/api/recipes": {
                "get": {
                    "tags": ["recipes"],
                    "summary": "List recipes",
                    "description": "Get all registered recipes",
                    "operationId": "listRecipes",
                    "responses": {
                        "200": {
                            "description": "List of recipes",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": { "$ref": "#/components/schemas/Recipe" }
                                    }
                                }
                            }
                        }
                    }
                },
                "post": {
                    "tags": ["recipes"],
                    "summary": "Create recipe",
                    "description": "Register a new recipe (YAML/JSON pipeline definition)",
                    "operationId": "createRecipe",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/CreateRecipeRequest" }
                            }
                        }
                    },
                    "responses": { "201": { "description": "Recipe created" } }
                }
            },
            "/api/recipes/{name}": {
                "get": {
                    "tags": ["recipes"],
                    "summary": "Get recipe",
                    "description": "Get a recipe definition by name",
                    "operationId": "getRecipe",
                    "parameters": [
                        { "name": "name", "in": "path", "required": true, "schema": { "type": "string" } }
                    ],
                    "responses": { "200": { "description": "Recipe definition" } }
                },
                "delete": {
                    "tags": ["recipes"],
                    "summary": "Delete recipe",
                    "description": "Remove a recipe",
                    "operationId": "deleteRecipe",
                    "parameters": [
                        { "name": "name", "in": "path", "required": true, "schema": { "type": "string" } }
                    ],
                    "responses": { "200": { "description": "Recipe deleted" } }
                }
            },
            "/api/recipes/{name}/execute": {
                "post": {
                    "tags": ["recipes"],
                    "summary": "Execute recipe",
                    "description": "Run a recipe immediately",
                    "operationId": "executeRecipe",
                    "parameters": [
                        { "name": "name", "in": "path", "required": true, "schema": { "type": "string" } }
                    ],
                    "responses": {
                        "200": {
                            "description": "Recipe execution started",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/RecipeRunStatus" }
                                }
                            }
                        }
                    }
                }
            },
            "/api/recipes/{name}/runs": {
                "get": {
                    "tags": ["recipes"],
                    "summary": "Get recipe runs",
                    "description": "Get execution history for a recipe",
                    "operationId": "getRecipeRuns",
                    "parameters": [
                        { "name": "name", "in": "path", "required": true, "schema": { "type": "string" } }
                    ],
                    "responses": {
                        "200": {
                            "description": "Run history",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": { "$ref": "#/components/schemas/RecipeRunStatus" }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/api/storage/status": {
                "get": {
                    "tags": ["storage"],
                    "summary": "Storage status",
                    "description": "Get tiered storage status and statistics",
                    "operationId": "storageStatus",
                    "responses": { "200": { "description": "Storage status" } }
                }
            },
            "/api/storage/migrate": {
                "post": {
                    "tags": ["storage"],
                    "summary": "Migrate storage",
                    "description": "Manually trigger data migration between storage tiers",
                    "operationId": "storageMigrate",
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "from": { "type": "string" },
                                        "to": { "type": "string" }
                                    }
                                }
                            }
                        }
                    },
                    "responses": { "200": { "description": "Migration triggered" } }
                }
            },
            "/api/udf/register": {
                "post": {
                    "tags": ["udf"],
                    "summary": "Register UDF",
                    "description": "Register a user-defined function (native, WASM, Python, JS)",
                    "operationId": "udfRegister",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/UdfDefinition" }
                            }
                        }
                    },
                    "responses": { "201": { "description": "UDF registered" } }
                }
            },
            "/api/udf": {
                "get": {
                    "tags": ["udf"],
                    "summary": "List UDFs",
                    "description": "Get all registered user-defined functions",
                    "operationId": "udfList",
                    "responses": { "200": { "description": "UDF list" } }
                }
            },
            "/api/udf/execute": {
                "post": {
                    "tags": ["udf"],
                    "summary": "Execute UDF",
                    "description": "Execute a user-defined function",
                    "operationId": "udfExecute",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "name": { "type": "string" },
                                        "args": { "type": "object" }
                                    },
                                    "required": ["name"]
                                }
                            }
                        }
                    },
                    "responses": { "200": { "description": "UDF result" } }
                }
            },
            "/api/udf/{name}": {
                "delete": {
                    "tags": ["udf"],
                    "summary": "Delete UDF",
                    "description": "Remove a registered UDF",
                    "operationId": "udfDelete",
                    "parameters": [
                        { "name": "name", "in": "path", "required": true, "schema": { "type": "string" } }
                    ],
                    "responses": { "200": { "description": "UDF removed" } }
                }
            },
            "/api/ws/query": {
                "get": {
                    "tags": ["websocket"],
                    "summary": "WebSocket query stream",
                    "description": "WebSocket endpoint for streaming Cypher query results",
                    "operationId": "wsQuery",
                    "responses": {
                        "101": { "description": "WebSocket upgrade" }
                    }
                }
            },
            "/api/ws/sq/{id}": {
                "get": {
                    "tags": ["websocket"],
                    "summary": "WebSocket SQ stream",
                    "description": "WebSocket endpoint for streaming Standing Query results",
                    "operationId": "wsSqResults",
                    "parameters": [
                        { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
                    ],
                    "responses": {
                        "101": { "description": "WebSocket upgrade" }
                    }
                }
            },
            "/api/cluster/stats": {
                "get": {
                    "tags": ["cluster"],
                    "summary": "Cluster stats",
                    "description": "Get cluster-wide statistics (only available in cluster mode)",
                    "operationId": "clusterStats",
                    "responses": { "200": { "description": "Cluster statistics" } }
                }
            },
            "/api/cluster/raft": {
                "get": {
                    "tags": ["cluster"],
                    "summary": "Raft status",
                    "description": "Get Raft consensus status (only available with --raft-port)",
                    "operationId": "raftStatus",
                    "responses": { "200": { "description": "Raft status" } }
                }
            }
        },
        "components": {
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "bearerFormat": "JWT",
                    "description": "HMAC-SHA256 signed token (use /api/v2/auth/token to generate)"
                }
            },
            "schemas": {
                "HealthResponse": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string", "example": "healthy"},
                        "mode": {"type": "string", "example": "single-node"},
                        "active_nodes": {"type": "integer"},
                        "shards": {"type": "integer"},
                        "standing_queries": {"type": "integer"},
                        "readiness": {"type": "string"},
                        "liveness": {"type": "string"},
                        "durability": {"type": "string"},
                        "version": {"type": "string"},
                        "uptime_seconds": {"type": "integer"}
                    }
                },
                "CypherRequest": {
                    "type": "object",
                    "required": ["query"],
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Cypher query string",
                            "example": "MATCH (n:Person) RETURN n.name LIMIT 10"
                        }
                    }
                },
                "CypherResponse": {
                    "type": "object",
                    "properties": {
                        "columns": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "rows": {
                            "type": "array",
                            "items": {
                                "type": "array",
                                "items": {}
                            }
                        }
                    }
                },
                "NodeResponse": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "properties": {
                            "type": "object",
                            "additionalProperties": true
                        },
                        "labels": {
                            "type": "array",
                            "items": {"type": "string"}
                        }
                    }
                },
                "Edge": {
                    "type": "object",
                    "properties": {
                        "source": {"type": "string"},
                        "target": {"type": "string"},
                        "label": {"type": "string"},
                        "properties": {
                            "type": "object",
                            "additionalProperties": true
                        }
                    }
                },
                "AddEdgeRequest": {
                    "type": "object",
                    "required": ["label", "target"],
                    "properties": {
                        "label": {"type": "string"},
                        "target": {"type": "string"},
                        "properties": {
                            "type": "object",
                            "additionalProperties": true
                        }
                    }
                },
                "StandingQuery": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "pattern": {"type": "string"},
                        "created_at": {"type": "string", "format": "date-time"}
                    }
                },
                "CreateStandingQueryRequest": {
                    "type": "object",
                    "required": ["pattern"],
                    "properties": {
                        "pattern": {
                            "type": "string",
                            "description": "Cypher-like pattern to match continuously"
                        }
                    }
                },
                            "SystemInfo": {
                    "type": "object",
                    "properties": {
                        "version": {"type": "string"},
                        "num_shards": {"type": "integer"},
                        "max_nodes_per_shard": {"type": "integer"},
                        "rocksdb_enabled": {"type": "boolean"},
                        "wal_enabled": {"type": "boolean"}
                    }
                },
                "IngestTask": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "path": {"type": "string"},
                        "format": {"type": "string"},
                        "status": {"type": "string"},
                        "events_processed": {"type": "integer"},
                        "started_at": {"type": "string", "format": "date-time"}
                    }
                },
                "StreamSource": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "source_type": {"type": "string"},
                        "topic": {"type": "string"},
                        "brokers": {"type": "string"},
                        "started_at": {"type": "string", "format": "date-time"}
                    }
                },
                "Recipe": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "version": {"type": "string"},
                        "description": {"type": "string"},
                        "standing_queries": {"type": "array"},
                        "ingest_sources": {"type": "array"},
                        "outputs": {"type": "array"}
                    }
                },
                "CreateRecipeRequest": {
                    "type": "object",
                    "required": ["name", "config"],
                    "properties": {
                        "name": {"type": "string"},
                        "config": {"type": "object", "description": "Recipe YAML/JSON definition"}
                    }
                },
                "RecipeRunStatus": {
                    "type": "object",
                    "properties": {
                        "run_id": {"type": "string"},
                        "recipe_name": {"type": "string"},
                        "status": {"type": "string", "enum": ["running", "success", "error"]},
                        "started_at": {"type": "string", "format": "date-time"},
                        "finished_at": {"type": "string", "format": "date-time"},
                        "result": {"type": "object"},
                        "error": {"type": "string"}
                    }
                },
                "UdfDefinition": {
                    "type": "object",
                    "required": ["name", "type"],
                    "properties": {
                        "name": {"type": "string"},
                        "type": {"type": "string", "enum": ["native", "wasm", "python", "javascript"]},
                        "source": {"type": "string", "description": "UDF source code or path"},
                        "entry_point": {"type": "string"},
                        "config": {"type": "object"}
                    }
                }
            }
        }
    })
}

/// Serve Swagger UI HTML (self-contained, no CDN dependencies)
pub fn swagger_ui_html() -> String {
    r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Nexora-RS API Documentation</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; color: #333; }
  header { background: #1a1a2e; color: #fff; padding: 20px 30px; }
  header h1 { font-size: 24px; }
  header p { opacity: 0.7; margin-top: 4px; font-size: 14px; }
  .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
  .section { background: #fff; border-radius: 8px; margin: 16px 0; padding: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
  .section h2 { font-size: 18px; color: #1a1a2e; margin-bottom: 12px; border-bottom: 2px solid #4ecdc4; padding-bottom: 8px; }
  .endpoint { display: flex; align-items: center; padding: 10px 0; border-bottom: 1px solid #eee; gap: 12px; font-size: 14px; }
  .method { display: inline-block; padding: 4px 10px; border-radius: 4px; font-weight: bold; font-size: 12px; min-width: 56px; text-align: center; }
  .get { background: #d4edda; color: #155724; }
  .post { background: #cce5ff; color: #004085; }
  .delete { background: #f8d7da; color: #721c24; }
  .path { font-family: monospace; color: #e83e8c; flex: 1; }
  .desc { color: #666; flex: 2; }
  .tags { display: flex; gap: 6px; flex-wrap: wrap; margin: 8px 0; }
  .tag { background: #e8f4f8; color: #0c6b7d; padding: 2px 8px; border-radius: 12px; font-size: 12px; }
  .method-group { margin: 20px 0; }
  .method-group h3 { font-size: 16px; color: #555; margin-bottom: 10px; }
  .search-box { width: 100%; padding: 12px 16px; border: 2px solid #ddd; border-radius: 8px; font-size: 14px; margin-bottom: 20px; outline: none; }
  .search-box:focus { border-color: #4ecdc4; }
  .badge { display: inline-block; background: #4ecdc4; color: #fff; padding: 2px 8px; border-radius: 4px; font-size: 11px; margin-left: 8px; }
  a { color: #e83e8c; text-decoration: none; }
  a:hover { text-decoration: underline; }
  .footer { text-align: center; padding: 20px; color: #999; font-size: 12px; }
</style>
</head>
<body>
<header>
  <h1>🚀 Nexora-RS API v2</h1>
  <p>Streaming Graph Engine — Interactive API Reference</p>
</header>
<div class="container">
  <input class="search-box" type="text" id="search" placeholder="🔍 Filter APIs... (e.g. health, cypher, graph, ingest)" oninput="filterEndpoints()">
  <div class="tags" id="tagFilters"></div>
  <div id="apiList">Loading...</div>
</div>
<div class="footer">Nexora-RS &copy; 2026 | <a href="/api/openapi.json">OpenAPI Spec</a></div>
<script>
async function loadSpec() {
  const res = await fetch('/api/v2/openapi.json');
  const spec = await res.json();
  renderAPI(spec);
}
function renderAPI(spec) {
  const paths = spec.paths;
  const tags = spec.tags || [];
  const tagSet = new Set();
  let html = '';
  const sortedPaths = Object.keys(paths).sort();

  for (const path of sortedPaths) {
    const ops = paths[path];
    for (const [method, op] of Object.entries(ops)) {
      const opTags = op.tags || ['general'];
      opTags.forEach(t => tagSet.add(t));
      html += '<div class="section" data-tags="' + opTags.join(',') + '">';
      html += '<h2>' + (op.summary || path) + '</h2>';
      html += '<div class="endpoint">';
      html += '<span class="method ' + method + '">' + method.toUpperCase() + '</span>';
      html += '<span class="path">' + path + '</span>';
      html += '<span class="desc">' + (op.description || '') + '</span>';
      html += '</div>';
      html += '<div class="tags">';
      for (const t of opTags) {
        html += '<span class="tag">' + t + '</span>';
      }
      html += '</div>';
      if (op.security && op.security.length > 0) {
        html += '<small style="color:#c00">🔒 Auth required</small> ';
      }
      html += '<small style="color:#999">operationId: ' + op.operationId + '</small>';
      html += '</div>';
    }
  }

  document.getElementById('apiList').innerHTML = html;
  renderTagFilters([...tagSet].sort());
}
function renderTagFilters(tags) {
  let html = '';
  for (const tag of tags) {
    html += '<span class="tag" style="cursor:pointer" onclick="filterByTag(\'' + tag + '\')">' + tag + '</span> ';
  }
  html += '<span class="tag" style="cursor:pointer;background:#4ecdc4;color:#fff" onclick="showAll()">All</span>';
  document.getElementById('tagFilters').innerHTML = html;
}
function filterEndpoints() {
  const q = document.getElementById('search').value.toLowerCase();
  document.querySelectorAll('.section').forEach(s => {
    s.style.display = s.textContent.toLowerCase().includes(q) ? '' : 'none';
  });
}
function filterByTag(tag) {
  document.querySelectorAll('.section').forEach(s => {
    const tags = s.getAttribute('data-tags') || '';
    s.style.display = tags.includes(tag) ? '' : 'none';
  });
}
function showAll() {
  document.querySelectorAll('.section').forEach(s => s.style.display = '');
}
loadSpec();
</script>
</body>
</html>"#
    .to_string()
}
