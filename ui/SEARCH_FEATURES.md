# Graph Browser Advanced Search Features

## Overview

The Graph Browser now includes a comprehensive search panel with four different search modes:

1. **Quick Search** - Fast text-based search
2. **Filter Search** - Property-based filtering with operators
3. **Cypher Query** - Native Cypher query language support
4. **SQL Query** - SQL syntax for graph queries

## How to Access

Click the **"Advanced Search"** button in the Graph Browser toolbar to open the search panel.

## Search Modes

### 1. Quick Search

Fast, simple text-based search across common node properties.

**Features:**
- Search by name, type, or any property
- Quick filter buttons for common terms (Alice, Bob, Person, Zone, Forklift)
- Press Enter to execute search

**Example:**
```
Alice
```

### 2. Filter Search

Property-based filtering with comparison operators.

**Components:**
- **Property dropdown**: Select which property to filter (name, type, status, speed, label)
- **Operator dropdown**: Choose comparison operator
  - `equals` - exact match
  - `contains` - substring match
  - `gt` - greater than (numeric)
  - `lt` - less than (numeric)
- **Value input**: Enter the value to compare

**Example:**
```
Property: speed
Operator: gt (greater than)
Value: 100
```

### 3. Cypher Query

Execute native Cypher queries for complex graph pattern matching.

**Preset Templates:**
- **All Nodes**: `MATCH (n) RETURN n LIMIT 20`
- **With Edges**: `MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 20`
- **Filter by Type**: `MATCH (n:Person) WHERE n.age > 30 RETURN n`

**Parameter Substitution:**
Use `$term` in your query for dynamic parameter substitution.

**Examples:**
```cypher
MATCH (n) WHERE n.name CONTAINS $term RETURN n LIMIT 20
MATCH (n:Person)-[:KNOWS]->(m) RETURN n, m
MATCH (n) WHERE n.speed > 100 RETURN n ORDER BY n.speed DESC
```

### 4. SQL Query

Query graph data using familiar SQL syntax (translated to Cypher internally).

**Preset Templates:**
- **All Nodes**: `SELECT * FROM nodes LIMIT 20`
- **By Type**: `SELECT * FROM nodes WHERE type = 'Person' LIMIT 20`
- **With Edge Count**: `SELECT n.*, COUNT(e.*) as edge_count FROM nodes n LEFT JOIN edges e ON n.id = e.source GROUP BY n.id LIMIT 20`

**Examples:**
```sql
SELECT * FROM nodes WHERE name LIKE '%Alice%' LIMIT 20
SELECT * FROM nodes WHERE type = 'Person' AND age > 30
SELECT * FROM nodes WHERE status = 'active'
```

## Search Results

Search results are displayed in a scrollable table showing:
- **Label**: Display name of the node
- **ID**: Shortened hex-encoded NexoraId
- **Properties**: List of available properties
- **Action**: "Load" button to add the node to the graph visualization

Click **"Load"** on any result to:
1. Add the node to the graph canvas
2. Load its properties
3. Fetch and display its connected edges

## Keyboard Shortcuts

- **Enter** - Execute search in Quick Search or Filter mode
- **Escape** - Clear the main search input

## Integration with Main Graph

The Advanced Search works seamlessly with the main graph browser:
- Search results can be loaded directly into the graph visualization
- Once loaded, nodes can be expanded, have properties modified, and edges added
- All standard graph browser features (context menu, double-click expand, etc.) work on searched nodes

## Backend API Endpoints

The search features interact with these backend endpoints:

- `/api/v2/graph/search` - Property-based filtering (Filter mode)
- `/api/v2/cypher` - Cypher query execution
- `/api/v2/sql` - SQL query execution (translated to Cypher)
- `/api/v2/graph/node/{id}/edges` - Fetch node edges after loading

## Tips

1. **Start with Quick Search** for simple lookups
2. **Use Filter mode** for property-based queries with specific operators
3. **Switch to Cypher** for complex graph patterns and traversals
4. **Try SQL mode** if you're more comfortable with SQL syntax
5. **Preset buttons** provide quick starting templates - modify them for your needs
6. **Search results** are limited to 20 by default - adjust LIMIT in your queries
7. **History dropdown** in the main toolbar tracks recently loaded nodes

## Future Enhancements

Planned features:
- Regex pattern matching
- Full-text search across all properties
- Save and load custom query templates
- Export search results to CSV/JSON
- Visual query builder
- Search result pagination
