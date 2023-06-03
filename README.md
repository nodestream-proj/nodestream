
# Nodestream

_Fast, Declarative ETL for Graph Databases._

[![Demo](docs/img/demo.gif)](https://badge.fury.io/py/nodestream)

#### Badges

[![Continuous Integration](https://github.com/nodestream-proj/nodestream/actions/workflows/ci.yaml/badge.svg)](https://github.com/nodestream-proj/nodestream/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/nodestream-proj/nodestream/branch/main/graph/badge.svg?token=HAPEVKQ6OQ)](https://codecov.io/gh/nodestream-proj/nodestream)
[![GPLv3 License](https://img.shields.io/badge/License-GPL%20v3-yellow.svg)](https://opensource.org/licenses/)
[![PyPI version](https://badge.fury.io/py/nodestream.svg)](https://badge.fury.io/py/nodestream)

## Features

- Flexible and extensible YAML based DSL for ETL jobs
- Connect to data sources like Kafka, AWS Athena, flat files, and more.
- Developer friendly features such as snapshot testing
- Highly optimized with async and tuned query generation


## Getting Started

Install nodestream with `pip`

```bash
  pip install nodestream
  nodestream new --database neo4j my_project && cd my_project
  nodestream run sample -v
```


## Documentation

Visit our [Documentation](https://nodestream-proj.github.io/nodestream) on Github Pages.


## Contributing

Contributions are always welcome!

See `contributing.md` for ways to get started.

Please adhere to this project's `code of conduct`.


## Authors

- Zach Probst ([@zprobst](https://www.github.com/zprobst))
- Chad Cloes ([@ccloes](https://www.github.com/ccloes))
- Oshri Rozenberg([@orozen](https://www.github.com/orozen))
- Kevin Neal ([@khneal](https://www.github.com/khneal))

