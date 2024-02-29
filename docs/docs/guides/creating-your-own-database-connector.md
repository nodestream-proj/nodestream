# Creating A Database Connector

## Introduction

This guide will walk through the process of creating a database connector for a hypothetical graph database called `smith`.

!!! caution

    Building a database connector is an advanced topic.
    Also it is likely that you will not need to build a database connector unless you are using a graph database that is not supported by nodestream.


## Prerequisites

Before you begin, you will need to have a basic understanding of the following concepts:

- **Labeled Property Graphs** The graph database that you are creating a connector for should be a [labeled property graph](https://www.oxfordsemantic.tech/faqs/what-is-a-labeled-property-graph).
- **Cypher** Ideally, the graph database that you are creating a connector for should have a query language similar to [Cypher](https://neo4j.com/developer/cypher-query-language/).
- **Python** You should have an Intermediate understanding of Python.


## `DatabaseConnector`

The `DatabaseConnector` class is the main class that you will need to implement. 
This class is responsible for creating instances of `QueryExecutor`, `TypeRetriever`, and `Migrator` that will be used by nodestream to interact with the database.

In the example below, we will create a `DatabaseConnector` for a hypothetical graph database called `smith`. 
This scaffold will be used as a starting point for creating a `DatabaseConnector` for a real graph database. 

```python
from nodestream.databases import DatabaseConnector, TypeRetriever
from nodestream.databases.query_executor import QueryExecutor
from nodestream.schema.migrations import Migrator

from smith import GraphDatabase, Driver

class SmithDatabaseConnector(DatabaseConnector, alias="smith"):
    @classmethod
    def from_file_data(
        cls,
        uri: str,
        username: str,
        password: str
    ):
        driver = GraphDatabase.driver(uri, auth=(username, password))
        return cls(driver)
    
    def __init__(
        self,
        driver: Driver,
    ) -> None:
        self.driver = driver

    def make_query_executor(self) -> QueryExecutor:
        ...

    def make_type_retriever(self) -> TypeRetriever:
        ...

    def make_migrator(self) -> Migrator:
        ...
```

## `QueryExecutor`

The `QueryExecutor` class is responsible for executing ingestion operations on the graph database.
The interface for the `QueryExecutor` class is shown below.

```python
from nodestream.databases.query_executor import QueryExecutor, OperationOnNodeIdentity, OperationOnRelationshipIdentity
from nodestream.model import Node, RelationshipWithNodes, TimeToLiveConfiguration, IngestionHook

class SmithQueryExecutor(QueryExecutor):
    async def upsert_nodes_in_bulk_with_same_operation(
        self, operation: OperationOnNodeIdentity, nodes: Iterable[Node]
    ):
        ...

    async def upsert_relationships_in_bulk_of_same_operation(
        self,
        shape: OperationOnRelationshipIdentity,
        relationships: Iterable[RelationshipWithNodes],
    ):
        ...

    async def perform_ttl_op(self, config: TimeToLiveConfiguration):
        ...

    async def execute_hook(self, hook: IngestionHook):
        ...
```

### Upsert Methods

The `upsert_nodes_in_bulk_with_same_operation` and `upsert_relationships_in_bulk_of_same_operation` methods are responsible for upserting nodes and relationships in bulk. 
Internally, `nodestream` batches like operations together to reduce the number of queries that are executed. 
Therefore, the `upsert_nodes_in_bulk_with_same_operation` and `upsert_relationships_in_bulk_of_same_operation` methods should be implemented to upsert multiple nodes and relationships in a single query via the provided `OperationOnNodeIdentity` and `OperationOnRelationshipIdentity` objects. 
The `OperationOnNodeIdentity` and `OperationOnRelationshipIdentity` objects define the shape of the node or relationship that is being upserted and the rule for how to upsert it. 

Ultimately, the `upsert_nodes_in_bulk_with_same_operation` and `upsert_relationships_in_bulk_of_same_operation` methods should be implemented to execute a single query that upserts multiple nodes or relationships in a single query.


### Time To Live

The `perform_ttl_op` method is responsible for performing a time to live operation on the graph database.
The `TimeToLiveConfiguration` object defines the configuration for the time to live operation.
Ultimately, the `perform_ttl_op` method should be implemented to execute a single query that performs the time to live operation on a given node type.

### Hooks
Ingestion hooks are a way to execute custom logic before or after a node or relationship is ingested into the graph database.
The `execute_hook` method is responsible for executing the ingestion hook and is called at the appropriate time during the ingestion process.
The `IngestionHook` object defines the configuration for the ingestion hook and has a `as_cypher_query_and_parameters` method that returns the cypher query and parameters that should be executed.


## `TypeRetriever`

The `TypeRetriever` class is responsible for retrieving nodes and relationships by their labels/types. 
The interface for the `TypeRetriever` class is shown below:

```python

class SmithDataRetriever(TypeRetriever):
    def get_nodes_of_type(self, type: str) -> AsyncGenerator[Node, None]:
        pass

    def get_relationships_of_type(self, type: str) -> AsyncGenerator[RelationshipWithNodes, None]:
        pass
```

The `get_nodes_of_type` and `get_relationships_of_type` methods are responsible for retrieving nodes and relationships of a given type from the graph database. `nodestream` will use these methods to retrieve nodes and relationships of a given type from the graph database. 

The interface expects an `AsyncGenerator` that yields `Node` and `RelationshipWithNodes` objects to allow for efficient streaming of data from the graph database such as paginating through a large number of nodes or relationships.

## `Migrator`

The `Migrator` class is responsible for creating and updating the schema of the graph database. 

```python
from nodestream.schema.migrations import Migrator
from nodestream.schema.migrations.operations import Operation


class SmithMigrator(Migrator):
    def __init__(self, ...) -> None:
        # Initialize the migrator
        ...

    async def execute_operation(self, operation: Operation) -> None:
        # Execute the operation
        ...
```

Fundementally, the `Migrator` class responds to operations following the [Command Pattern](https://refactoring.guru/design-patterns/command).
To facilitate this, the `Migrator` can use the `OperationTypeRoutingMixin` to route operations to the correct method.

```diff
- from nodestream.schema.migrations import Migration, 
+ from nodestream.schema.migrations import Migrator, OperationTypeRoutingMixin
+ from nodestream.schema.migrations.operations import CreateNode


- class SmithMigrator(Migrator):
+ class SmithMigrator(Migrator, OperationTypeRoutingMixin):
    def __init__(self, ...) -> None:
        # Initialize the migrator
        ...

-    async def execute_operation(self, operation: Operation) -> None:
-        # Execute the operation
-        ...

+    async def execute_create_node(self, operation: CreateNode) -> None:
+        # Execute the operation
+
+    # TODO: Implement methods for other operation types
```

Commands are routed to the correct method by the `OperationTypeRoutingMixin` based on the type of the operation
by convention, the method name should be `execute_{operation_type}` where `{operation_type}` is the type of the operation.

You should implement a method for each operation type that you want to support in your database connector (Ideally all of them).
The complete inventory of operations can be found in the [Reference](https://nodestream-proj.github.io/nodestream/latest/python_reference/schema/migrations/operations).


## Registering the Connector 


Database Connectors are registered via the [entry_points](https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins) API of a Python Package. Specifically, the `entry_point` named `databases` inside of the `nodestream.plugins` group is loaded. The `entry_point` should be a module that contains at least one database connector class. At runtime, the module will be loaded and all classes that inherit from `nodestream.databases:DatabaseConnector` will be registered. The `alias` attribute of the class will be used as the name of the database connector.

The `entry_point` should be a module that contains at least one audit class. At runtime, the module will be loaded and all classes that inherit from `nodestream.project.audits:Audit` will be registered. The `name` attribute of the class will be used as the name of the audit.

Depending on how you are building your package, you can register your audit plugin in one of the following ways:

=== "pyproject.toml"
    ```toml
    [project.entry-points."nodestream.plugins"]
    databases = "nodestream_plugin_cool.databases"
    ```

=== "pyproject.toml (poetry)"
    ```toml
    [tool.poetry.plugins."nodestream.plugins"]
    databases = "nodestream_plugin_cool.databases"
    ```

=== "setup.cfg"
    ```ini
    [options.entry_points]
    nodestream.plugins =
        databases = nodestream_plugin_cool.databases
    ```

=== "setup.py"
    ```python
    from setuptools import setup

    setup(
        # ...,
        entry_points = {
            'nodestream.plugins': [
                'databases = nodestream_plugin_cool.databases',
            ]
        }
    )
    ```
