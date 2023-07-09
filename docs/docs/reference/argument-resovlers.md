# Argument Resolvers

An `ArgumentResolver` allows you to inline a value into the Pipeline file before the pipeline is initialized. This can be
useful for passing configuration from files, environment, secret stores, and the like. By default, nodestream ships with
a few built in argument resolvers.

## `!include`

Loads another yaml file and inlined the contents of that file into the location its supplied. For example,
imagine you want to store the list of file globs separate from the `FileExtractor` itself. The file path can be
referenced relatively or absolutely.

### Example

```yaml title="pipelines/file_targets.yaml"
- people/*.json
- other_people/*.json
```

```yaml title="pipelines/ingest_files.yaml"
- implementation: nodestream.pipeline.extractors:FileExtractor
  arguments:
    globs: !include file_targets.yaml
```

## `!env`

Inline the value of an environment variable at the current location.

### Example

```bash
export MY_DATABASE_PASSWORD=$(cat database_password.txt)
nodestream run pipeline_hitting_database
```

```yaml title="pipelines/pipeline_hitting_database.yaml"
- implementation: nodestream.databases:GraphDatabaseWriter
  arguments:
    batch_size: 1000
    database: neo4j
    uri: bolt://localhost:7687
    username: neo4j
    password: !env MY_DATABASE_PASSWORD
```
