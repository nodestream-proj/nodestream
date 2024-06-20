# Customizing the Queue Extractor

The `QueueExtractor` is responsible for extracting data from a queue source and converting it into a stream of records. The `QueueExtractor` is a subclass of `nodestream.pipeline.extractors:Extractor` and is responsible for the following: 

- Polling data from the queue source
- Converting the raw data into a stream of records
- Yielding the records to the `Pipeline`

The `QueueExtractor` is configured via a series of configurations options. To learn more about the `QueueExtractor` and its configuration options, see the [Queue Extractor](../reference/extractors.md) reference. The queue extractor and the format of the records on that stream
are abstracted from the `QueueExtractor` itself via the `QueueConnector` and `RecordFormat` classes. This allows you to create a `QueueExtractor` that can be used with any queue source and any record format.

## Creating a new Queue Connector

If you want to connect to a different queue source, you can create a new `QueueConnector` subclass. The `QueueConnector` class is responsible for polling data from the queue source and yielding it to the `QueueExtractor`. The `QueueConnector` class is an abstract class, so you must implement the `poll` method, which takes no arguments and yields the raw data from the queue source.

For example, the following `QueueConnector` subclass polls data from a file (as a trivial example):

```python
from nodestream.pipeline.extractors.queues import QueueConnector

class FileConnector(QueueConnector, alias="file"):
    def __init__(self, file_path):
        self.file_path = file_path

    def poll(self):
        with open(self.file_path, "r") as f:
            yield f.read()
```

The `alias` class attribute is used to specify the name of the queue connector. This name is used in the `queue` configuration option of the `QueueExtractor` to specify which queue connector to use. For more information see the [Queue Extractor](../reference/extractors.md) reference.

### Registering the Queue Connector

Queue connectors are registered via the [entry_points](https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins) API of a Python Package. Specifically, the `entry_point` named `queue_connectors` inside of the `nodestream.plugins` group is loaded. It is expected to be a subclass of `nodestream.pipeline.extractors.queues:QueueConnector` as directed above.

The `entry_point` should be a module that contains at least one queue connector class. At runtime, the module will be loaded and all classes that inherit from `nodestream.pipeline.extractors.queues:QueueConnector` will be registered. The `alias` attribute of the class will be used as the name of the queue connector.

Depending on how you are building your package, you can register your queue connector plugin in one of the following ways:

=== "pyproject.toml"
    ```toml
    [project.entry-points."nodestream.plugins"]
    queue_connectors = "nodestream_plugin_cool.queue_connectors"
    ```

=== "pyproject.toml (poetry)"
    ```toml
    [tool.poetry.plugins."nodestream.plugins"]
    queue_connectors = "nodestream_plugin_cool.queue_connectors"
    ```

=== "setup.cfg"
    ```ini
    [options.entry_points]
    nodestream.plugins =
        queue_connectors = nodestream_plugin_cool.queue_connectors
    ```

=== "setup.py"
    ```python
    setup(
        ...
        entry_points={
            "nodestream.plugins": [
                "queue_connectors = nodestream_plugin_cool.queue_connectors"
            ]
        },
        ...
    )
    ```


## Creating a new Record Format

If your queue data comes in a different format, you can create a new `RecordFormat` subclass. The `RecordFormat` class is responsible for parsing the raw data from the queue connector and yielding it to the `QueueExtractor`. The `RecordFormat` class is an abstract class, so you must implement the `parse` method, which takes a single argument, `data`, and yields the parsed records.

For example, the following `RecordFormat` subclass parses data as yaml:

```python
from nodestream.pipeline.extractors.queues import RecordFormat

class YamlFormat(RecordFormat, alias="yaml"):
    def parse(self, data):
        import yaml
        yield from yaml.load_all(data)
```

The `alias` class attribute is used to specify the name of the record format. This name is used in the `format` configuration option of the `QueueExtractor` to specify which record format to use. For more information see the [Queue Extractor](../reference/extractors.md) reference.

### Registering the Record Format

Record formats are registered via the [entry_points](https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins) API of a Python Package. Specifically, the `entry_point` named `record_formats` inside of the `nodestream.plugins` group is loaded. It is expected to be a subclass of `nodestream.pipeline.extractors.streams:RecordFormat` as directed above. An instance of the class is created and the `parse` method is called with the raw data from the stream connector.

The `entry_point` should be a module that contains at least one record format class. At runtime, the module will be loaded and all classes that inherit from `nodestream.pipeline.extractors.streams:RecordFormat` will be registered. The `alias` attribute of the class will be used as the name of the record format.

Depending on how you are building your package, you can register your record format plugin in one of the following ways:

=== "pyproject.toml"
    ```toml
    [project.entry-points."nodestream.plugins"]
    record_formats = "nodestream_plugin_cool.record_formats"
    ```

=== "setup.py"
    ```python
    setup(
        ...
        entry_points={
            "nodestream.plugins": [
                "record_formats = nodestream_plugin_cool.record_formats",
            ],
        },
        ...
    )
    ```

=== "setup.cfg"
    ```ini
    [options.entry_points]
    nodestream.plugins =
        record_formats = nodestream_plugin_cool.record_formats
    ```
