# Contribution Guidelines

We fully embrace the open source model, and if you have something to add, we would love to review it and get it merged in!

## Before You Start

Clone the project

```bash
  git clone git@github.com:nodestream-proj/nodestream.git
```

Go to the project directory

```bash
  cd nodestream
```

Install dependencies

_(You'll need to have Python 3.11 and [poetry](https://python-poetry.org/) installed)_

```bash
  poetry install
```

Run the Tests

```bash
  poetry run pytest
```

## Contribution Process

Please note we have a [Code of Conduct](https://github.com/nodestream-proj/nodestream/blob/main/.github/CODE_OF_CONDUCT.md), please follow it in all your interactions with the project.

## Communicating with the Project

Before starting work, please check the [issues](https://github.com/nodestream-proj/nodestream/issues) to see what's being discussed and worked on. If there's an open, unresolved issue for what you want to work on, please comment on it stating that you would like to tackle the changes. If there's not an issue, please add one and also state that you'll work on it.

## Contributon Strategy

1. Ensure all code matches the "Code Expectations" discussed below
1. Commit and push your code to your fork
1. Open a pull request

## Code Expectations

#### Code Coverage

All new code will be unit tested to 90% coverage.

#### Object Orientation

`nodestream` relies on a heavily object oriented model. Most natural extensions of the framework should use that design philosophy.

## Contact Information

The best way to contact the project Maintainers is through the [issues](https://github.com/nodestream-proj/nodestream/issues); we'll get back to you within a few business days.
