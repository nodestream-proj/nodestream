# Nodestream 

<picture >
  <source media="(prefers-color-scheme: dark)" srcset="./docs/img/NodeSteamLogo_Small_Light.png">
  <img alt="Nodestream Logo" src="./docs/img/NodeSteamLogo_Small_Dark.png" width="300" align="right">
</picture>


> A Declarative framework for Building, Maintaining, and Analyzing Graph Data 🚀

[![Continuous Integration](https://github.com/nodestream-proj/nodestream/actions/workflows/ci.yaml/badge.svg)](https://github.com/nodestream-proj/nodestream/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/nodestream-proj/nodestream/branch/main/graph/badge.svg?token=HAPEVKQ6OQ)](https://codecov.io/gh/nodestream-proj/nodestream)
[![ApacheV2 License](https://img.shields.io/badge/License-Apache%202.0-yellow.svg)](https://opensource.org/license/apache-2-0/)
<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-14-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->

**Nodestream allows you to work with graphs declaratively.** With nodestream, you unlock a bounty of features purpose built for working with graphs. 
Semantically model your graph and map labels and properties directly to your data. 
Better yet, you are not locked into your choices. 
Nodestream works with you as you evolve your application by providing migration utilities to change your data schema. 
Nodestream even decouples you from the underyling database technology so you can even change databases.

#### Highlights

- Connect to data sources like [Kafka](https://nodestream-proj.github.io/docs/docs/reference/extractors/#streamextractor), [files](https://nodestream-proj.github.io/docs/docs/reference/extractors/#unifiedfileextractor), [apis](https://nodestream-proj.github.io/docs/docs/reference/extractors/#simpleapiextractor), and [more](https://nodestream-proj.github.io/docs/docs/reference/extractors/)!
- Evolve your application over time with database migrations ([Docs](https://nodestream-proj.github.io/docs/docs/tutorials-intermediate/working-with-migrations/))
- Use your favorite Graph Database to fit any tech stack ([Docs](https://nodestream-proj.github.io/docs/docs/category/database-support/))
- Clean up your own data with TTLs ([Docs](https://nodestream-proj.github.io/docs/docs/tutorials-intermediate/removing-data/#implementing-a-ttl-pipeline))
- Infinite Customizability Since Nearly Everything is Pluggable!

> [Website](https://nodestream-proj.github.io/docs/) • [Blog](https://nodestream-proj.github.io/docs/blog/) • [Discussions](https://github.com/orgs/nodestream-proj/discussions)  • [Contributing](#contributing) • [Contributing Developer Guides](https://nodestream-proj.github.io/docs/docs/category/developer-reference/) • [Talks from Maintainers](https://www.youtube.com/watch?v=2F-xx4LcTng&list=PLUiAbWRQecSOorv_V6TzfUBoIZyf-6r6R&pp=gAQBiAQB)

## Features 

Nodestream has a pleasant CLI interface to get new projects up and running fast. 

![Demo](https://raw.githubusercontent.com/nodestream-proj/nodestream/e94d0faa024c0f8da1e83a4ff6d83746504d197e/docs/img/demo.gif)

Not a fan of the defaults? You can change out databases very easily

![Using Another Database](https://nodestream-proj.github.io/docs/assets/images/neptune-2c1c78b173e824fc1e824f54287e467f.gif)

Then you can start to model your data and nodestream will evolve your database for you. No more messing with constraints or writing database queries. 

![Running Migrations](https://nodestream-proj.github.io/docs/assets/images/migrations-1ede1ab3d5438cdca24d66cfa6d66231.gif)

## Getting Started

Conviced? Install nodestream with `pip` to get started. 

```bash
  pip install nodestream
  nodestream new --database neo4j my_project && cd my_project
  nodestream run sample -v
```

We highly recommend following our tutorials [here](https://nodestream-proj.github.io/docs/docs/category/tutorial---basics/)

## Packages 

Nodestream is built on a Highly Pluggable and Modular Architecture. Thus... we have a lot of packages to keep track of. 

| Package                      	| Description                                                                   	| Version                                                                                                                        	|
|------------------------------	|-------------------------------------------------------------------------------	|--------------------------------------------------------------------------------------------------------------------------------	|
| `nodestream`                 	| The core library. Declarative ingestion.                                      	| ![PyPI Version](https://badge.fury.io/py/nodestream.svg)                                 	|
| `nodestream-plugin-neo4j`     	| Neo4j database connector.                                                     	| ![PyPI Version](https://badge.fury.io/py/nodestream-plugin-neo4j.svg)         	|
| `nodestream-plugin-neptune`  	| AWS Neptune database connector.                                               	| ![PyPI Version](https://badge.fury.io/py/nodestream-plugin-neptune.svg)   	|
| `nodestream-plugin-dotenv`   	| Adds DotEnv integration.                                                      	| ![PyPI Version](https://badge.fury.io/py/nodestream-plugin-dotenv.svg)     	|
| `nodestream-plugin-pedantic` 	| A series of lints to enforce reasonable naming standards, etc.                	| ![PyPI Version](https://badge.fury.io/py/nodestream-plugin-pedantic.svg) 	|
| `nodestream-plugin-shell`    	| An integration with nodestream to run shell commands.                         	| ![PyPI Version](https://badge.fury.io/py/nodestream-plugin-shell.svg)       	|
| `nodestream-plugin-sbom`     	| Import SBOM files in CycloneDX and SPDX into an opinionated graph data model. 	| ![PyPI Version](https://badge.fury.io/py/nodestream-plugin-sbom.svg)         	|
| `nodestream-plugin-akamai`   	| Parse Akamai properties, redirect configs, and much more and ingests them.    	| ![PyPI Version](https://badge.fury.io/py/nodestream-plugin-akamai.svg)     	|
| `nodestream-plugin-k8s`      	| In incubation. A plugin that orchestrates Nodestream on k8s.                  	| ![PyPI Version](https://badge.fury.io/py/nodestream-plugin-k8s.svg)           	|


## Contributors

Nodestream is a community project. We welcome all contributions. 
Be sure to checkout or [Contributing Docs](https://nodestream-proj.github.io/docs/docs/category/developer-reference/) and our [Code of Conduct](./CODE_OF_CONDUCT.md) before contributing. 

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/zprobst"><img src="https://avatars.githubusercontent.com/u/22159145?v=4?s=100" width="100px;" alt="Zach Probst"/><br /><sub><b>Zach Probst</b></sub></a><br /><a href="https://github.com/nodestream-proj/nodestream/commits?author=zprobst" title="Code">💻</a> <a href="https://github.com/nodestream-proj/nodestream/pulls?q=is%3Apr+reviewed-by%3Azprobst" title="Reviewed Pull Requests">👀</a> <a href="#maintenance-zprobst" title="Maintenance">🚧</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/ccloes"><img src="https://avatars.githubusercontent.com/u/1000538?v=4?s=100" width="100px;" alt="Chad Cloes"/><br /><sub><b>Chad Cloes</b></sub></a><br /><a href="https://github.com/nodestream-proj/nodestream/commits?author=ccloes" title="Code">💻</a> <a href="https://github.com/nodestream-proj/nodestream/pulls?q=is%3Apr+reviewed-by%3Accloes" title="Reviewed Pull Requests">👀</a> <a href="#maintenance-ccloes" title="Maintenance">🚧</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/angelosantos4"><img src="https://avatars.githubusercontent.com/u/142852840?v=4?s=100" width="100px;" alt="asantos4"/><br /><sub><b>asantos4</b></sub></a><br /><a href="https://github.com/nodestream-proj/nodestream/commits?author=angelosantos4" title="Code">💻</a> <a href="https://github.com/nodestream-proj/nodestream/pulls?q=is%3Apr+reviewed-by%3Aangelosantos4" title="Reviewed Pull Requests">👀</a> <a href="#maintenance-angelosantos4" title="Maintenance">🚧</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/grantleehoffman"><img src="https://avatars.githubusercontent.com/u/603848?v=4?s=100" width="100px;" alt="Grant Hoffman"/><br /><sub><b>Grant Hoffman</b></sub></a><br /><a href="https://github.com/nodestream-proj/nodestream/commits?author=grantleehoffman" title="Code">💻</a> <a href="https://github.com/nodestream-proj/nodestream/pulls?q=is%3Apr+reviewed-by%3Agrantleehoffman" title="Reviewed Pull Requests">👀</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/khneal"><img src="https://avatars.githubusercontent.com/u/40273388?v=4?s=100" width="100px;" alt="khneal"/><br /><sub><b>khneal</b></sub></a><br /><a href="https://github.com/nodestream-proj/nodestream/commits?author=khneal" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/orozen"><img src="https://avatars.githubusercontent.com/u/62594754?v=4?s=100" width="100px;" alt="orozen"/><br /><sub><b>orozen</b></sub></a><br /><a href="https://github.com/nodestream-proj/nodestream/commits?author=orozen" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://sites.google.com/view/ai4allrobotics"><img src="https://avatars.githubusercontent.com/u/66497192?v=4?s=100" width="100px;" alt="Sophia Don Tranho"/><br /><sub><b>Sophia Don Tranho</b></sub></a><br /><a href="https://github.com/nodestream-proj/nodestream/commits?author=sophiadt" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/bechbd"><img src="https://avatars.githubusercontent.com/u/6898505?v=4?s=100" width="100px;" alt="bechbd"/><br /><sub><b>bechbd</b></sub></a><br /><a href="https://github.com/nodestream-proj/nodestream/commits?author=bechbd" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/yasonk"><img src="https://avatars.githubusercontent.com/u/6750414?v=4?s=100" width="100px;" alt="yasonk"/><br /><sub><b>yasonk</b></sub></a><br /><a href="https://github.com/nodestream-proj/nodestream/commits?author=yasonk" title="Code">💻</a> <a href="https://github.com/nodestream-proj/nodestream/pulls?q=is%3Apr+reviewed-by%3Ayasonk" title="Reviewed Pull Requests">👀</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/stuartio"><img src="https://avatars.githubusercontent.com/u/22449467?v=4?s=100" width="100px;" alt="Stuart Macleod"/><br /><sub><b>Stuart Macleod</b></sub></a><br /><a href="https://github.com/nodestream-proj/nodestream/commits?author=stuartio" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Cole-Greer"><img src="https://avatars.githubusercontent.com/u/112986082?v=4?s=100" width="100px;" alt="Cole Greer"/><br /><sub><b>Cole Greer</b></sub></a><br /><a href="https://github.com/nodestream-proj/nodestream/commits?author=Cole-Greer" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Aclucas1"><img src="https://avatars.githubusercontent.com/u/21301692?v=4?s=100" width="100px;" alt="Austin Lucas"/><br /><sub><b>Austin Lucas</b></sub></a><br /><a href="https://github.com/nodestream-proj/nodestream/commits?author=Aclucas1" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/rreddy2"><img src="https://avatars.githubusercontent.com/u/18334818?v=4?s=100" width="100px;" alt="rreddy2"/><br /><sub><b>rreddy2</b></sub></a><br /><a href="https://github.com/nodestream-proj/nodestream/commits?author=rreddy2" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://jondotcomdotorg.net/"><img src="https://avatars.githubusercontent.com/u/314260?v=4?s=100" width="100px;" alt="Jon Bristow"/><br /><sub><b>Jon Bristow</b></sub></a><br /><a href="https://github.com/nodestream-proj/nodestream/commits?author=jbristow" title="Code">💻</a></td>
    </tr>
  </tbody>
  <tfoot>
    <tr>
      <td align="center" size="13px" colspan="7">
        <img src="https://raw.githubusercontent.com/all-contributors/all-contributors-cli/1b8533af435da9854653492b1327a23a4dbd0a10/assets/logo-small.svg">
          <a href="https://all-contributors.js.org/docs/en/bot/usage">Add your contributions</a>
        </img>
      </td>
    </tr>
  </tfoot>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

### Contributing 

Need a quick reference guide on how to contribute? Here you go!

#### Getting Setup 

To get started you'll need to install poery.

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

You then can install the project dependencies with the following command:

```bash
poetry install
```

No need to active a virtual environment. Poetry handles that for you with `poetry run` and `poetry shell`.

#### Running Tests

To run tests for the entire project, run the following command:

```bash
poetry run pytest
```
