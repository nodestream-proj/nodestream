# Nodestream

Nodestream is a modern and simple framework for performing ETL into Graph Databases.

The key features are:

1. **Fast to Build Pipelines**: By building data pipelines on a user friendly and extensible DSL, you can assemble and reuse modular
components to go from implementation to production in an hour.

2. **Fewer Bugs**: Less code == less bugs. Combine that with an Integration and End to End testing tools, and you can build confidence in your robust.

3. **Robust and Resilient**: Based on a battlehardend and optimized components, nodestream is capable of doing a lot of work very quick while handling errors inteligently.


## Getting Started

Get started with our [Tutorial](./docs/tutorial.md).

## Shell Completion

### BASH - Ubuntu / Debian
```bash
nodestream completions bash | sudo tee /etc/bash_completion.d/nodestream.bash-completion
```

### BASH - Mac OSX (with Homebrew "bash-completion")

```bash
nodestream completions bash > $(brew --prefix)/etc/bash_completion.d/nodestream.bash-completion
```

### ZSH

```zsh
mkdir ~/.zfunc
echo "fpath+=~/.zfunc" >> ~/.zshrc
nodestream completions zsh > ~/.zfunc/_test
```

### FISH

```bash
nodestream completions fish > ~/.config/fish/completions/nodestream.fish
```
