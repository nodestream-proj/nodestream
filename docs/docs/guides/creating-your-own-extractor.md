# Creating an Extractor

An extractor is a component that extracts data from a source and yields it to the pipeline. Extractors are used to extract data from a variety of sources, including files, databases, and streams. Extractors are defined as classes that inherit from `nodestream.pipeline:Extractor`. The class must implement the `extract_records` method, which takes no arguments and is an async generator for the extracted data.

For example, lets implement a crude file extractor that reads a file and yields each line as a record:

NOTE: If you want to implement a file extractor, you should use the `FileExtractor` class instead of implementing your own.

```python
from nodestream.pipeline import Extractor

class FileExtractor(Extractor):
    def __init__(self, file_path):
        self.file_path = file_path

    async def extract_records(self):
        with open(self.file_path, "r") as f:
            for line in f:
                yield line
```

## Using an Extractor

To use an extractor, you must add it to your pipeline configuration. For example, the following pipeline configuration uses the `FileExtractor` defined above:

```yaml
- implementation: my_module:FileExtractor
  arguments:
    file_path: /path/to/file
```
