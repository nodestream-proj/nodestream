# Customizing the Stream Extractor

The `StreamExtractor` is responsible for extracting data from a stream source and converting it into a stream of records. The `StreamExtractor` is a subclass of `nodestream.pipeline.extractors:Extractor` and is responsible for the following: 

- Polling data from the stream source
- Converting the raw data into a stream of records
- Yielding the records to the `Pipeline`

The `StreamExtractor` is configured via a series of configurations options. To learn more about the `StreamExtractor` and its configuration options, see the [Stream Extractor](../reference/extractors.md) reference. The stream extractor and the format of the records on that stream
are abstracted from the `StreamExtractor` itself via the `StreamConnector` and `RecordFormat` classes. This allows you to create a `StreamExtractor` that can be used with any stream source and any record format.

## Creating a new Stream Connector

If you want to connect to a different stream source, you can create a new `StreamConnector` subclass. The `StreamConnector` class is responsible for polling data from the stream source and yielding it to the `StreamExtractor`. The `StreamConnector` class is an abstract class, so you must implement the `poll` method, which takes no arguments and yields the raw data from the stream source.

For example, the following `StreamConnector` subclass polls data from a file (as a trivial example):

```python
from nodestream.pipeline.extractors.streams import StreamConnector

class FileConnector(StreamConnector, alias="file"):
    def __init__(self, file_path):
        self.file_path = file_path

    def poll(self):
        with open(self.file_path, "r") as f:
            yield f.read()
```

The `alias` class attribute is used to specify the name of the stream connector. This name is used in the `stream` configuration option of the `StreamExtractor` to specify which stream connector to use. For more information see the [Stream Extractor](../reference/extractors.md) reference.

### Registering the Stream Connector

Stream connectors are registered via the [entry_points](https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins) API of a Python Package. Specifically, the `entry_point` named `stream_connectors` inside of the `nodestream.plugins` group is loaded. It is expected to be a subclass of `nodestream.pipeline.extractors.streams:StreamConnector` as directed above.

The `entry_point` should be a module that contains at least one stream connector class. At runtime, the module will be loaded and all classes that inherit from `nodestream.pipeline.extractors.streams:StreamConnector` will be registered. The `alias` attribute of the class will be used as the name of the stream connector.

Depending on how you are building your package, you can register your stream connector plugin in one of the following ways:

=== "pyproject.toml"
    ```toml
    [project.entry-points."nodestream.plugins"]
    stream_connectors = "nodestream_plugin_cool.stream_connectors"
    ```

=== "setup.cfg"
    ```ini
    [options.entry_points]
    nodestream.plugins =
        stream_connectors = nodestream_plugin_cool.stream_connectors
    ```

=== "setup.py"
    ```python
    setup(
        ...
        entry_points={
            "nodestream.plugins": [
                "stream_connectors = nodestream_plugin_cool.stream_connectors"
            ]
        },
        ...
    )
    ```


## Creating a new Record Format

If your stream data comes in a different format, you can create a new `RecordFormat` subclass. The `RecordFormat` class is responsible for parsing the raw data from the stream connector and yielding it to the `StreamExtractor`. The `RecordFormat` class is an abstract class, so you must implement the `parse` method, which takes a single argument, `data`, and yields the parsed records.

For example, the following `RecordFormat` subclass parses data as yaml:

```python
from nodestream.pipeline.extractors.streams import RecordFormat

class YamlFormat(RecordFormat, alias="yaml"):
    def parse(self, data):
        import yaml
        yield from yaml.load_all(data)
```

The `alias` class attribute is used to specify the name of the record format. This name is used in the `format` configuration option of the `StreamExtractor` to specify which record format to use. For more information see the [Stream Extractor](../reference/extractors.md) reference.

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
