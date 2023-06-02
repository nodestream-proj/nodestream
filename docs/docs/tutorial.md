# Tutorial

Welcome! And thank you for checking out nodestream. This guide will get you up and running with the basics of
nodestream. To demonstrate it, we'll be building an ingestion of an org chart.

## Generating a New Project

To generate a nodestream project, its simple. Make sure you have installed nodestream with:

```bash
pip install nodestream
```

After doing so, lets generate a simple project. To do so, run

```
nodestream new --db neo4j my_first_graph
```

In this example, we are signaling that we want to use `neo4j` as the graph database and naming our
project `my_first_graph`. `cd` into that new directory.

## Touring the Scaffolded Project

In your newly created `my_first_graph` directory, you should see a folder structure that looks like so:

```
.
├── my_first_graph
│   ├── __init__.py
│   ├── argument_resolvers.py
│   ├── normalizers.py
│   └── value_providers.py
├── nodestream.yaml
└── pipelines
    └── sample.yaml
```

Essentially it produces three things.

1. A basically empty python package named the same thing as your project (`my_first_graph`).
2. A pipelines folder which stores the definitions of your etl pipelines.
3. A `nodestream.yaml` file which acts as your project configuration.

Expanding on the `nodestream.yaml` file, it looks a little like this:

```yaml
imports:
- my_first_graph.argument_resolvers
- my_first_graph.normalizers
- my_first_graph.value_providers
- nodestream.databases.neo4j
scopes:
  default:
    pipelines:
    - pipelines/sample.yaml
```

This file has two sections:

1. `imports` are dotted module paths that should be imported and initialized at project start. This is where you can
   inject extra behaviors by [Extending Nodestream](../extending-nodestream/). You can also see that `nodestream`
   itself has used this section to initialize the module for the `neo4j` database connector.

2. `scopes` is where pipelines go. A `scope` represents a logical grouping of pipelines that make sense for your
   application. Think of them like a folder.

You can see the project status by running `nodestream show`. That should produce an output like this:

```
+---------+--------+-----------------------+-------------+
| scope   | name   | file                  | annotations |
+---------+--------+-----------------------+-------------+
| default | sample | pipelines/sample.yaml |             |
+---------+--------+-----------------------+-------------+
```

## Managing Pipelines

In order to demonstrate how one can manage pipelines in nodestream, lets remove the default pipleine and add it back.
Start by running:

```bash
nodestream remove default sample
```

This removes the pipeline in the `default` scope with the name `sample`. Now if we run `nodestream show` again you
will see the pipeline is gone:

```
+-------+------+------+-------------+
| scope | name | file | annotations |
+-------+------+------+-------------+
```

Further more you can also see that the pipeline file is removed from the project directory:

```
.
├── my_first_graph
│   ├── __init__.py
│   ├── argument_resolvers.py
│   ├── normalizers.py
│   └── value_providers.py
├── nodestream.yaml
└── pipelines
```

Now lets generate a new pipeline running:

```bash
nodestream scaffold org-chart
```

That will produce a new pipeline file called `org-chart.yaml`

```
.
├── my_first_graph
│   ├── __init__.py
│   ├── argument_resolvers.py
│   ├── normalizers.py
│   └── value_providers.py
├── nodestream.yaml
└── pipelines
    └── org-chart.yaml
```

The output of `nodestream show` should now look like this:

```
+---------+-----------+--------------------------+-------------+
| scope   | name      | file                     | annotations |
+---------+-----------+--------------------------+-------------+
| default | org-chart | pipelines/org-chart.yaml |             |
+---------+-----------+--------------------------+-------------+
```

## Introducing our Data Set

Before anything, lets create a directory to store our data:

```bash
mkdir data
```

Now, lets examine our data. In this example, we are building an org chart. Let's take a look at a couple records:

```json
{
    "employee_id": "jdoe",
    "first_name": "Jane",
    "last_name": "Doe",
    "title": "CEO",
    "reports_to": null
}
```

```json
{
    "employee_id": "bsmith",
    "first_name": "Bob",
    "last_name": "Smith",
    "title": "CTO",
    "reports_to": "jdoe"
}
```

For the purposes of this demonstration, create TWO `.json` files in the newly created `data` directory with these contents
and create a couple more records that fit the same schema if you choose. So your project structure should look
approximately like this:

```
.
├── data
│   ├── bsmith.json
│   └── jdoe.json
├── my_first_graph
│   ├── __init__.py
│   ├── argument_resolvers.py
│   ├── normalizers.py
│   └── value_providers.py
├── nodestream.yaml
└── pipelines
    └── org-chart.yaml
```

## Implement the Pipeline

*Cracks Figers*...

Alright, now lets get down to building out the pipeline. If you open the `pipelines/org-chart.yaml` file it should look
like this:

```yaml
- arguments:
    stop: 100000
  factory: range
  implementation: nodestream.pipeline:IterableExtractor
- arguments:
    interpretations:
    - key:
        number: !jmespath 'index'
      node_type: Number
      type: source_node
  implementation: nodestream.interpreting:Interpreter
- arguments:
    batch_size: 1000
    database: neo4j
    password: neo4j123
    uri: bolt://localhost:7687
    username: neo4j
  implementation: nodestream.databases:GraphDatabaseWriter
```

Each pipeline file is laid out as a series of `Step`s which are chained together and executed in order for each
record. The first item in the pipeline is referred to generally as an `Extractor`.

### Loading our Data Files

In the scaffold, we have an `IterableExtractor` configured to return records for each number in the range [0,100000).
Not exactly the most interesting. Lets get to work wiring this up to use our newly created data instead.
Replace the first block with the following:

```yaml
- implementation: nodestream.extractors:FileExtractor
  arguments:
    globs:
      - data/*.json
# remainder of the pipeline unchanged.
```

This block tells nodestream to initialize the `FileExtractor` class in the `nodestream.extractors` module to handle
the first step of the pipeline. To initialize it, it passes the `arguments` provided. In this case, the `FileExtractor`
expects a list of [glob strings](https://en.wikipedia.org/wiki/Glob_(programming)). Every file that matches these glob
strings is loaded and passed as a record in the pipeline.

For more information on the `FileExtractor` or `Extractors` in general, refer to the [Extractors Reference](./reference/extractors.md).
For more information on the file formats and how they are supported, view the [File Format Reference](./reference/file-formats.md).

### Interpreting the Data Into Nodes and Relationships

The `Step` that you will spend the most time becoming familiar with is the `Interpreter`. The interpreter takes
instructions on how to convert a document or record like our JSON data and map it into nodes and relationships that
we want to be in our graph.

To do so, it relies on `Interpretation`s. An `Interpretation` is a single one of the aforementioned instructions. While
there is tremendous depth to the `Interpreter` and `Interpretation`s, we are going to keep it simple for this tutorial.

Replace the second step (the existing interpreter block) with the following (we'll go over it bit by bit):

```yaml
- implementation: nodestream.interpreting:Interpreter
  arguments:
    interpretations:
    - type: source_node
      key:
        employee_id: !jmespath 'employee_id'
      properties:
        first_name: !jmespath 'first_name'
        last_name: !jmespath 'last_name'
        title: !jmespath 'CEO'
      node_type: Employee
    - type: relationship
      node_type: Employee
      relationship_type: REPORTS_TO
      node_key:
        employee_id: !jmespath 'reports_to'
```

At the top of the block, its follows the same pattern we saw with the `FileExtractor` from before
with the `implementation` and `arguments` sections. However, the `arguments` are obviously very different.
Lets investigate each of the two interpretations:

#### Source Node

```yaml
- type: source_node
  key:
    employee_id: !jmespath 'employee_id'
  properties:
    first_name: !jmespath 'first_name'
    last_name: !jmespath 'last_name'
    title: !jmespath 'CEO'
  node_type: Employee
```

A source node represents the node at the conceptual "center" of the ingest. Typically this represents the central
entity that you are modeling with the Ingest. In our case, its the employee for whom the record represents. We've
decided to call this type of node an `Employee`.

The `key` block tells nodestream what set of properties represents a unique node of the `node_type` and how to get
the values. In our case, we use the `employee_id` field the record and extract that using the
`!jmespath` [Value Provider](./reference/value-providers.md).

We take a similar approach with the properties field. We extract the properties `first_name`, `last_name`, and `title`
from their corresponding locations in the record, again with `!jmespath`. Note that we have kept the field names on the
node the same as the json document, but this does not need to be the case.

#### Relationship

A graph database would be a lot less useful if we did not create relationships to other data. In our case, we want to
model the org chart, so we need to draw the relationship to the employee's boss.

```yaml
- type: relationship
  node_type: Employee
  relationship_type: REPORTS_TO
  node_key:
    employee_id: !jmespath 'reports_to'
```

Here we tell the interpreter that we want to relate to an `Employee` node with a relationship labels `REPORTS_TO`. For
nodestream to know which `Employee` node to relate to, we need to specify the key of the related node. In our case,
we can do that by extrating the value of `reports_to`  and mapping it to the `employee_id` key.

## Testing it Out

Alright! We've done a lot of work. Lets see if our results can pay off. But before we get started, we need to have
a database to connect to. In the beginning we selected neo4j. The easiest way to get a local neo4j database is to run it
via docker. Below is a command to load a docker neo4j database:

```bash
docker run \
    --restart always \
    --publish=7474:7474 --publish=7687:7687 \
    --env NEO4J_AUTH=neo4j/neo4j123 \
    --env NEO4J_PLUGINS='["apoc","bloom"]' \
    neo4j:5
```


After that, we are finally ready! Drum roll please...

```bash
nodestream run org-chart -v
```

Should give you output like this:

```
Running: Initialize Logger
Running: Initialize Project
Running: Run Pipeline
 - Finished running pipeline: 'org-chart' (1 sec)
```
