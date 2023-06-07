# Implemeting Time to Live

This guide will walk you through the process of implementing a time to live (TTL) feature in your application. This feature will allow you to set a time limit on the amount of time a document is available in your database. After the time limit has expired, the document will be deleted from the database.

This guide assumes that you have already created a NodeStream application. If you have not, please see the Getting Started guide.

## Overview

The TTL feature is implemented by building two data pipelines. One for evicting nodes and one for evicting relationships. The evictor pipelines are intended to run at a specified interval. The evictor pipelines will delete the nodes and relationships that have expired.


## Implementing the Node Evictor Pipeline

The node evictor pipeline will delete nodes that have expired. The pipeline will be implemented by creating a new extractor and a new writer. The extractor will read the node TTL configurations from the database and the writer will delete the nodes that have expired.

### Creating the Node TTL Evictor

The node TTL configuration extractor will read the node TTL configurations from the pipeline. The extractor will be implemented by using the `TimeToLiveConfigurationExtractor` class. The extractor will be configured to read the node TTL configurations from the database.

```yaml file="pipelines/node-ttl-evictor.yaml"
- implementation: nodestream.extractors:TimeToLiveConfigurationExtractor
  arguments:
    graph_object_type: NODE
    configurations:
      - object_type: Person
        expiry_in_hours: 96
      - object_type: Occupation
        expiry_in_hours: 48
- implementation: nodestream.writers:GraphDatabaseWriter
  arguments:
    batch_size: 100
    # TODO: Fill in the database configuration for your application
```

You can view all the configuration options in the `TimeToLiveConfigurationExtractor` documentation [here](../reference/extractors.md).

### Creating the Relationship TTL Evictor

The relationship TTL Evictor is implemented in the same way as the node TTL Evictor. The only difference is that the `graph_object_type` argument is set to `RELATIONSHIP`.


```yaml file="pipelines/relationship-ttl-evictor.yaml"
- implementation: nodestream.extractors:TimeToLiveConfigurationExtractor
  arguments:
    graph_object_type: RELATIONSHIP
    configurations:
      - object_type: REPORTS_TO
        expiry_in_hours: 96
      - object_type: PERFORMS
        expiry_in_hours: 48
- implementation: nodestream.writers:GraphDatabaseWriter
  arguments:
    batch_size: 100
    # TODO: Fill in the database configuration for your application
```

Again, You can view all the configuration options in the `TimeToLiveConfigurationExtractor` documentation [here](../reference/extractors.md).

### Running the Evictor Pipelines

The evictor pipelines can be run at whatever interval is correct for your application. Note that the data will only be eviceted when the piplines are run and not the moment the TTL expires. The evictor pipelines can be run by using the `nodestream` command line tool.

```bash
nodestream run node-ttl-evictor
nodestream run relationship-ttl-evictor
```
