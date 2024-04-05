# Neo4j Nodestream Plugin for Nodestream

This plugin provides a [Nodestream](https://github.com/nodestream-proj/nodestream) interface to Neo4j. 

## Installation

```bash
pip install nodestream-plugin-neo4j
```

## Usage

```yaml
# nodestream.yaml
targets:
  neo4j:
    database: neo4j
    uri: bolt://localhost:7687
    username: neo4j
    password: neo4j
    database_name: neo4j # optional; name of the database to use.
    use_enterprise_features: false # optional; use enterprise features (e.g. node key constraints)
```

### Extractor 

The `Neo4jExtractor` class represents an extractor that reads records from a Neo4j database. It takes a single Cypher query as
input and yields the records read from the database. The extractor will automatically paginate through the database until it reaches the end. Therefore, the query needs to include a `SKIP` and `LIMIT` clause. For example:

```yaml
- implementation: nodestream_plugin_neo4j.extractor:Neo4jExtractor
  arguments:
    query: MATCH (p:Person) WHERE p.name = $name RETURN p.name SKIP $offset LIMIT $limit
    uri: bolt://localhost:7687
    username: neo4j
    password: neo4j
    database_name: my_database # Optional; defaults to neo4j
    limit: 100000 # Optional; defaults to 100
    parameters:
      # Optional; defaults to {}
      # Any parameters to be passed to the query
      # For example, if you want to pass a parameter called "name" with the value "John Doe", you would do this:
      name: John Doe
```

The extractor will automatically add the `SKIP` and `LIMIT` clauses to the query. The extractor will also automatically add the `offset` and `limit` parameters to the query. The extractor will start with `offset` set to `0` and `limit` set to `100` (unless overridden by setting `limit`) The extractor will continue to paginate through the database until the query returns no results. 

## Concepts 

### Migrations 

The plugin supports migrations. Migrations are used to create indexes and constraints on the database. 

As part of the migration process, the plugin will create  `__NodestreamMigration__` nodes in the database. 
This node will have a `name` property that is set to the name of the migration. 

Additionally, the plugin will create a `__NodestreamMigrationLock__` node in the database. 
This node will be exit when the migration process is running and will be deleted when the migration process is complete.
This is used to prevent multiple migration processes from running at the same time.
