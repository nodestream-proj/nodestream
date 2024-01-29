# Technical Details

## `asyncio`

Nodestream is built on top of the more modern aspects of python. Primarily asyncio to improve performance. 
To understand how it does so, it’s important to understand what exactly asnycio is and how it works. 

`asyncio` is a way of modeling concurrency in an application. 
Note that concurrency is different than parallelism. 
Parallelism deals with multiple operations executing simultaneously while concurrency deals with tasks that can be executed independently of each other and start, run, and complete in overlapping time periods


![concurrency](https://files.realpython.com/media/Screen_Shot_2018-10-17_at_3.18.44_PM.c02792872031.jpg)

!!! note

    Images from [https://realpython.com/async-io-python/](https://realpython.com/async-io-python/)

`asyncio` operates on the observation that in some systems, the cpu is spending a lot of time _blocking_ or waiting for IO operations such as network calls, file reads, and the like to finish before it can continue operating. 
To make better use of the CPU, `asyncio` introduces a series of language features that facilitate defining tasks and signaling when they need to wait. The language runtime can the “swap in” a different task that it can work on that’s not (currently) IO bound. 
In workloads where there is an abundant amout of IO, the overhead of managing these tasks is worth it because  we are able to make better use CPU time that would otherwise be wasted.

![asyncio](https://eng.paxos.com/hs-fs/hubfs/_02_Paxos_Engineering/Event-Loop.png?width=800&name=Event-Loop.png)

!!! note

    Images from [https://eng.paxos.com/python-3s-killer-feature-asyncio](https://eng.paxos.com/python-3s-killer-feature-asyncio)

So how does nodestream leverage it? All the performance sensitive apis in nodestream allow for asynchronous operations. Most importantly, each step in a pipeline is executed asyncronously. To do so, each step is given two components at exectuion time:

* A reference to the step before it
* An outbox which is used to store completed output

Each step runs in its own asynchronous loop awaiting results from the upstream step, processing them, and putting results in the outbox.
This design allows for each step to execute independently and not be constrained by IO bottlenecks down stream. 
Take for example a standard pipeline of three components:

1. An file extractor
1. An interpreter
1. A Graph database writer

The slowest step in the pipeline is the graph database writer. 
It takes large chunks of data, converts it all to queries and submits generally long running queries it has to await.
While this is happening, python can suspend the task waiting on the database allowing the other steps to continue processing.
This allows for the extractor and interpreter to continue processing data and not be constrained by the database writer.

## Migrations 

`nodestream` from versions `0.1`` through `0.11` automatically requested the creation of indexes and constraints on the database. 
This was done by introspecting the schema of the entire project and generating the appropriate queries to create the indexes and constraints.
This was a very powerful feature but it had a few drawbacks:
- **It was redundant.** The same indexes and constraints were being created with `IF NOT EXISTS` clauses every time the pipeline was run.
- **It was slow.** The queries were being executed serially and the pipeline was locked until they were all completed.
- **It was error prone.** If the database was not in a state that allowed for the creation of the indexes and constraints, the pipeline would fail.
- **It was high friction.** There was no way to refactor the database without manual intervention. If the schema changed, the pipeline would fail and the user would have to manually remove the indexes, constraints, and sometimes data before running the pipeline again.

To address these issues, `nodestream` 0.12 introduced the concept of migrations.
Migrations are a way of encapsulating changes to the database schema in a way that can be applied incrementally. 
Conceptually, they are similar to the migrations in the [Django](https://docs.djangoproject.com/en/5.0/topics/migrations/), [Rails](https://guides.rubyonrails.org/v3.2/migrations.html), [Neo4j Migrations](https://neo4j.com/labs/neo4j-migrations/2.0/), and [Flyway](https://documentation.red-gate.com/fd/migrations-184127470.html) frameworks.


### Scope

Some changes to the database are tricier to deal with than others, for the 0.11 release, the following changes are supported:
- **Creating indexes.** Creating indexes on node/relationship properties.
- **Droping indexes.** Dropping indexes on node/relationship properties.
- **Renaming indexes.** Renaming indexes on node/relationship properties (Drop and create).
- **Creating Node / Property Types.** Registering new node and property types with the database and creating a constraint on the node type.
- **Renaming Node / Relationship Types** Renaming a node or relationship type.
- **Deleting Node / Relationship Types** Deleting a node or relationship type.
- **Adding Properties** Adding a property on a node or relationship type with a default value.
- **Renaming Properties** Renaming a property on a node or relationship type.
- **Deleting Properties** Deleting a property on a node or relationship type.
- **Extending Keys** Adding a property to a key constraint.
- **Renaming Keys** Renaming a key constraint property. 

Notably, some use cases that are not supported:

- **Deleting Part of a Key** Deleting a property from a key constraint. This is not supported because it would require deleting all nodes that violate the constraint and re-creating them (which is not supported). While this is possible to do, the added complexity make this case not a 
good candidate for a first pass at migrations.

- **Adding a property with a non-trivial default value** Adding a property with a default value that is not a constant. This is not supported because it would require evaluating python code per item being migrated which is not supported due to the additional complexity it would introduce. In frameworks like Django, this is supported because the framework can rely on the ORM to provide a consistent interface for evaluating the default value. In nodestream, there is no such interface.

### Migration Anatomy 

A migration is a yaml file that contains a list of operations to be performed on the database. 
Each operation represents a change to the database schema (e.g creating and index, creating a node type, etc).
The operations are executed in the order they are defined in the file. 
The file is named with a timestamp and a description of the migration.
While the timestamp will usually correspond to the order the migrations are applied, a list of `dependencies` is used to explicitly define the order of execution.

Internally, `nodestream` will build a migration graph from all migrations in the project and their dependencies.
It will then execute the migrations in topological order to ensure that all dependencies are met before a migration is applied.

### Change Detection

To determine if a migration needs to be created, `nodestream` will compare the current state of the project to the state 
described in the migrations. 
If there are any differences, a new migration will be created with the appropriate operations to bring the migrations up to date with the current state of the project.
The design of this process is inspired by [Django Migrations](https://docs.djangoproject.com/en/5.0/topics/migrations/). 
Specifcically, the [Autodetector](https://github.com/django/django/blob/main/django/db/migrations/autodetector.py) class. 


### Developer Experience

The developer experience has several core goals:

- **Low friction.** The developer should be able to make changes to the database schema without having to manually manage the migrations.
- **Fast.** The developer should be able to make changes to the database schema without having to wait for the migrations to be applied.
- **Safe.** The developer should be able to make changes to the database schema without having to worry about breaking the pipeline.

To achieve these goals, `nodestream` provides a set of commands that allow the developer to manage the migrations.

- `nodestream migratations make [--pipeline <pipeline_name> --scope <scope_name>]` - Diff the current state of the project with the migrations and create a new migration if there are any differences. If `--pipeline` is specified, only migrations for that pipeline will be created. If `--scope` is specified, only migrations for that scope will be created.
- `nodestream migrations run [--dry-run]` - Apply all migrations to the database. If `--dry-run` is specified, the migrations will not be applied but the list of migrations that would be applied will be printed.
- `nodestream migrations show -t <target>` - Show the migrations that will be applied to the target database.
- `nodestream run --auto-migrate` - Run the pipeline and apply any migrations that need to be applied before. 

Additionally, when migrating to `nodestream` 0.12, the developer can run `nodestream migrations make` to generate the initial set of migrations for the project that will be compatible with 0.11 and below indexes and constraints.


### Connector API

The connector API for migrations defines how the database backend should interface with the migrations framework.
It is an extensions of the existing `nodestream.databases:DatabaseConnector` class. 
Starting with `0.12`, the `DatabaseConnector` class will be extended with the following optional method:

```python
from nodestream.schema.migrations import Migrator

class DatabaseConnector(ABC):
    # class contents omitted for brevity 
    @abstractmethod
    def get_migrator(self) -> Migrator:
        pass
```

The `Migrator` class is responsible for executing the migrations on the database.
It's interface is defined as follows: 

- `Migrator.exectute_operation(operation: Operation) -> None` - Execute the given operation on the database. This method handles the logic of executing the operation and raising an exception if the operation fails. 
- `Migrator.get_completed_migrations() -> List[Migration]` - Get the current state of the database. This method should return a list of migrations that have been applied to the database so that the migrator can determine which migrations need to be applied and interface with the developer. 

In addition to core methods, the `Migrator` class also provides additional hooks for applying a lock to the database, handling transactions, and more for databases that support additional consistency guarantees.


## Database Connector Plugins 

`Nodestream` is designed to be extensible. 
The core of `nodestream` is designed to be as generic as possible and provide a set of extension points that allow for the creation of plugins that can be used to extend the functionality of nodestream.

One of the most important extension points is the `DatabaseConnector` class.
This class is responsible for interfacing with the database and providing a consistent interface for the rest of the application to interact with the database.
The `DatabaseConnector` class is an abstract class that defines the interface that all database connectors must implement.
While most projects won't need to implement their own database connector, every project will need to use one. 

Prior to nodestream 0.12, `neo4j` was housed in the core of nodestream. 
This meant that every project that used nodestream had to install `neo4j` even if they didn't use it.
This was not ideal because it added additional dependencies to the project and increased the size of a final application.
To address this, `nodestream` 0.12 pulled `neo4j` out of the core and into a separate package called `nodestream-plugin-neo4j`.

