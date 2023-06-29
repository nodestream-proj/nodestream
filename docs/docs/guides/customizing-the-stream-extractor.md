# Customizing the Stream Extractor

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
