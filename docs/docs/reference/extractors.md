# Extractors

## `StreamExtractor`

The `StreamExtractor` provides a convenient abstraction for extracting records from different types of streams
by allowing customization of the underlying stream system and the record format. By implementing the `StreamConnector`
and `StreamRecordFormat` subclasses, one can easily adapt the extraction process to various stream sources and
record formats.

The documentation below contains information on the supported `StreamConnector` and `StreamRecordFormat` options and
how to configure them. See the [Customizing The Stream Extractor](../guides/customizing-the-stream-extractor.md) guide
to learn how to add your own implementations of these classes.

#### Top Level Arguments

```yaml
- implementation: nodestream.pipeline.extractors.streams:StreamExtractor
  arguments:
     # rest of the stream extractor format arguments
     timeout: 10 # default 60. Number of seconds to await records.
     max_records: 1 # default 100. Max number of records to get at one time.
```

### `StreamConnector`

The `StreamConnector` describes how to poll data from the underlying streaming mechanism.

#### `Kafka`

```yaml
- implementation: nodestream.pipeline.extractors.streams:StreamExtractor
  arguments:
     # rest of the stream extractor format arguments
     connector: kafka
     topic: my-topic-with-data
     group_id: my_group_id
     bootstrap_servers:
      - localhost:9092
      - localhost:9093
```

### `StreamRecordFormat`

The `StreamRecordFormat` parses the raw data from the `StreamConnector`.

#### `json`

The `json` format simply calls `json.loads` on the data provided from the `StreamConnector`. To use it, you can
set the `record_format` to be `json` in the `StreamExtractor` configuration. For example:

```yaml
- implementation: nodestream.pipeline.extractors.streams:StreamExtractor
  arguments:
     # rest of the stream extractor format
     record_format: json
```

## `QueueExtractor`

The `QueueExtractor` provides a convenient abstraction for extracting records from different types of queues
by allowing customization of the underlying queue system and the record format. By implementing the `QueueConnector`
and `StreamRecordFormat` subclasses, one can easily adapt the extraction process to various queue sources and
record formats.

The documentation below contains information on the supported `QueueConnector` options and
how to configure them. See the [Customizing The Queue Extractor](../guides/customizing-the-queue-extractor.md) guide
to learn how to add your own implementations of these classes.

#### Top Level Arguments

```yaml
- implementation: nodestream.pipeline.extractors.queues:QueueExtractor
  arguments:
     # rest of the queue extractor format arguments
     max_batch_size: 1 # default 10. Max number of records to retrieve at one time, max size depends on connector implementation.
     max_batches: 1 # default 10. Number of batches to process
```

### `QueueConnector`

The `QueueConnector` describes how to poll data from the underlying queue mechanism.

#### `AWS SQS`

```yaml
- implementation: nodestream.pipeline.extractors.queues:QueueExtractor
  arguments:
     # rest of the stream extractor format arguments
     connector: sqs
     queue_url: "https://sqs.us-east-1.amazonaws.com/177715257436/MyQueue"
```

### Additional Arguments
With the previous minimal configuration, it will use your currently active aws credentials to read messages from
`https://sqs.us-east-1.amazonaws.com/177715257436/MyQueue`. However, there are many options you can add to this:

| Parameter Name          	| Type   	| Description                                                                                                                                                                               	|
|-------------------------	|--------	|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
| message_system_attribute_names                  	| String 	| A list of attributes that need to be returned along with each message. (Default: "All")                                                                                                     	|
| message_attribute_names                  	| String 	| A list of attribute names to receive. (Default: "All")                                                                                                     	|
| delete_after_read                  	| Boolean 	| Deletes the batch of messages from the queue after they are yielded to the next pipeline step. (Default: True)               
| assume_role_arn         	| String 	| The ARN of a role to assume before interacting with the SQS Queue. Of course the appropriate configuration is needed on both the current credentials as well as the target role.             	|
| assume_role_external_id 	| String 	| The external id that is required to assume role. Only used when `assume_role_arn` is set and only needed when the role is configured to require an external id.                           	|
| **session_args          	| Any    	| Any other argument that you want sent to the [boto3.Session](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html) that will be used to interact with AWS. 	|

## `AthenaExtractor`

The `AthenaExtractor` issues a query to Amazon Athena, and returns yields each row as a record to the pipeline. For
example, the following `AthenaExtractor` configuration:

```yaml
- implementation: nodestream.pipeline.extractors.stores.aws:AthenaExtractor
  arguments:
    query: SELECT name, version FROM python_package_versions;
    workgroup: MY_WORKGROUP_NAME
    database: package_registry_metadata;
    output_location: s3://my_bucket/some_path
```

produces records with the following shape:

```json
{"name": "nodestream", "version": "0.2.0"}
```

### Arguments

| Parameter Name          	| Type   	| Description                                                                                                                                                                               	|
|-------------------------	|--------	|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
| query                   	| String 	| The actual query to run. The results yielded by the extractor will reflect the shape of the data returned from the query.                                                                 	|
| workgroup               	| String 	| The workgroup name to use to execute the query under. See the [AWS Docs](https://docs.aws.amazon.com/athena/latest/ug/user-created-workgroups.html) for more information.                 	|
| output_location         	| String 	| The output location string to store results for Athena. See the [AWS Docs](https://docs.aws.amazon.com/athena/latest/ug/querying.html) for more information.                              	|
| database                	| String 	| The name of the athena logical database to execute the query in. See the [AWS Docs](https://docs.aws.amazon.com/athena/latest/ug/user-created-workgroups.html) for more information.      	|
| assume_role_arn         	| String 	| The ARN of a role to assume before interacting with the bucket. Of course the appropriate configuration is needed on both the current credentials as well as the target role.             	|
| assume_role_external_id 	| String 	| The external id that is required to assume role. Only used when `assume_role_arn` is set and only needed when the role is configured to require an external id.                           	|
| **session_args          	| Any    	| Any other argument that you want sent to the [boto3.Session](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html) that will be used to interact with AWS. 	|

## `S3Extractor`

The `S3Extractor` pulls files down from S3 and yields the records read from each file using the
[appropriate file format parser](./file-formats.md). A simple example would look like this:

```yaml
- implementation: nodestream.pipeline.extractors.stores.aws:S3Extractor
  arguments:
    bucket: my-awesome-bucket
```

### Additional Arguments
With the previous minimal configuration, it will use your currently active aws credentials to read all objects from
`my-awesome-bucket`. However, there are many options you can add to this:

| Parameter Name          	| Type   	| Description                                                                                                                                                                               	|
|-------------------------	|--------	|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
| prefix                  	| String 	| Filter the objects pulled from S3 to only the ones that have this prefix in the name.                                                                                                     	|
| object_format           	| String 	| Regardless of the file's extension, use the format provided from the list of [file format](./file-formats.md) supported.                                                                  	|
| archive_dir             	| String 	| After a object has been processed, move the object for its current location to an a specified `archive` folder inside the bucket. Objects inside this folder are ignored when processing. 	|
| assume_role_arn         	| String 	| The ARN of a role to assume before interacting with the bucket. Of course the appropriate configuration is needed on both the current credentials as well as the target role.             	|
| assume_role_external_id 	| String 	| The external id that is required to assume role. Only used when `assume_role_arn` is set and only needed when the role is configured to require an external id.                           	|
| **session_args          	| Any    	| Any other argument that you want sent to the [boto3.Session](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html) that will be used to interact with AWS. 	|


## `FileExtractor`

The `FileExtractor` class represents an extractor that reads records from files specified by glob patterns.
It takes a collection of file paths as input and yields the records read from each file using the
[appropriate file format parser](./file-formats.md). The files are read and yield in sorted order by file name so that
the records are always yielded in the same order. 

```yaml
- implementation: nodestream.pipeline.extractors:FileExtractor
  arguments:
    globs:
      - people/*.json
      - other_people/*.json
```

## `RemoteFileExtractor`

The `RemoteFileExtractor` class represents an extractor that reads records from files specified by URLs. It takes a
collection of URLs as input and yields the records read from each file using the [appropriate file format parser](./file-formats.md).

```yaml
- implementation: nodestream.pipeline.extractors:RemoteFileExtractor
  arguments:
    memory_spooling_max_size_in_mb: 10 # Optional
    urls:
      - https://example.com/people.json
      - https://example.com/other_people.json
```

The `RemoteFileExtractor` will use the value of `memory_spooling_max_size_in_mb` to determine how much memory to use when spooling the file contents. 
If the file is larger than the specified amount, it will be downloaded to a temporary file on disk and read from there. 
If the file is smaller than the specified amount, it will be downloaded to memory and read from there. 
The default value is 5 MB.

## `SimpleApiExtractor`

The `SimpleApiExtractor` class represents an extractor that reads records from a simple API. It takes a single URL as
input and yields the records read from the API. The API must return a JSON array of objects either directly or as the
value of specified key.

For example, if the API returns the following JSON:

```json
{
  "people": [
    {
      "name": "John Doe",
      "age": 42
    },
    {
      "name": "Jane Doe",
      "age": 42
    }
  ]
}
```

Then the extractor can be configured as follows:

```yaml
- implementation: nodestream.pipeline.extractors:SimpleApiExtractor
  arguments:
    url: https://example.com/people
    yield_from: people
```

If the API returns a JSON array directly, then the `yield_from` argument can be omitted.

The `SimpleApiExtractor` will automatically paginate through the API until it reaches the end if the API supports
limit-offset style pagination through a query parameter. 
By default, no pagination is performed. The query parameter name can be configured using the `offset_query_param` argument.

For example, if the API supports pagination through a `page` query parameter, then the extractor can be configured as follows:

```yaml
- implementation: nodestream.pipeline.extractors:SimpleApiExtractor
  arguments:
    url: https://example.com/people
    offset_query_param: page
```

You can also specify headers to be sent with the request using the `headers` argument.
  
```yaml
- implementation: nodestream.pipeline.extractors:SimpleApiExtractor
  arguments:
    url: https://example.com/people
    headers:
      x-api-key: !env MY_API_KEY
```


## `TimeToLiveConfigurationExtractor`

"Extracts" time to live configurations from the file and yields them one at a time to the graph database writer.


One can configure a Node TTL like this:
```yaml
- implementation: nodestream.pipeline.extractors.ttl:TimeToLiveConfigurationExtractor
  arguments:
    graph_object_type: NODE
    configurations:
      - object_type: Person
        expiry_in_hours: 96
      - object_type: Occupation
        expiry_in_hours: 48
```

and one can configure a Relationship TTL like this:

```yaml
- implementation: nodestream.pipeline.extractors.ttl:TimeToLiveConfigurationExtractor
  arguments:
    graph_object_type: RELATIONSHIP
    configurations:
      - object_type: REPORTS_TO
        expiry_in_hours: 96
      - object_type: PERFORMS
        expiry_in_hours: 48
```

Additionally, you can introduce the parameter override_expiry_in_hours that will overwrite all expiry in hours for the configuration, or apply that time for anything that doesn't explicitly state an expiry time.
```yaml
- implementation: nodestream.pipeline.extractors.ttl:TimeToLiveConfigurationExtractor
  arguments:
    graph_object_type: RELATIONSHIP
    configurations:
      - object_type: REPORTS_TO
      - object_type: PERFORMS
    override_expiry_in_hours: 4
```




### Arguments

Each configuration can include the following arguments:

| Parameter Name          	| Type   	| Description                                                                                                                                                                               	                                                       |
|-------------------------	|--------	|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| object_type             	| String 	| The object type to apply the TTL to.                                                                                                                                                      	                                                       |
| expiry_in_hours         	| Integer 	| The number of hours after which the object should be deleted. Optional if override_expiry_in_hours is set.                                                                                                                                        |
| enabled                 	| Boolean 	| Whether or not the TTL is enabled. Defaults to `True`.                                                                                                                                     	                                                      |
| batch_size              	| Integer 	| The number of objects to delete in a single batch. Defaults to `100`.                                                                                                                     	                                                       |
| custom_query            	| String 	| A custom query to use to delete the objects. If not provided, the default query will be used. The custom query is database implementation specific.                                                                                             	 |


## `Neo4jExtractor`

The `Neo4jExtractor` class represents an extractor that reads records from a Neo4j database. It takes a single Cypher query as
input and yields the records read from the database. The extractor will automatically paginate through the database until it reaches the end. Therefore, the query needs to include a `SKIP` and `LIMIT` clause. For example:

```yaml
- implementation: nodestream.databases.neo4j.extractor:Neo4jExtractor
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
