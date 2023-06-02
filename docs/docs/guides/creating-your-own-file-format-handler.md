# Handling a File Format

There are instances where you might need to support custom file formats for reading data into a nodestream pipeline's `FileExtractor`.
This guide will walk you through the process of creating your own file format implementation for your project.

## Define Your File Format Class

To create your own file format, you need to define a class that implements the required behavior.
Let's say we want to create a file format for reading JSON files. Here's an example implementation:

```python
import yaml
from io import StringIO
from typing import Iterable

from nodestream.extractor.files import SupportedFileFormat

class YamlFileFormat(SupportedFileFormat, alias=".yaml"):
    def read_file_from_handle(self, fp: StringIO) -> Iterable[dict]:
        return [yaml.safe_load(fp)]
```

In this example, `YamlFileFormat` is a class that reads a YAML file from a file handle (`fp`) and returns an
iterable of dictionaries representing the YAML document. In order to support your file format, you need to
implement the method `read_file_from_handle`. This method reads the file and returns the data in a desired format.

The method signature should match the expected behavior of the project or the specific module you're working with.
In our example, the `read_file_from_handle` method takes a `StringIO` file handle and returns an iterable of dictionaries.

## Make sure your module is imported

Wherever you have your class defined, nodestream needs to know that its something that should be imported. To do
so, add your module to the imports section of your `nodestream.yaml` file. For example:

```yaml
imports:
  - nodestream.databases.neo4j # an existing import
  - my_project.some_sub_package.file_formats
```
