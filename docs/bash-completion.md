# BASH - Ubuntu / Debian
nodestream completions bash | sudo tee /etc/bash_completion.d/nodestream.bash-completion

# BASH - Mac OSX (with Homebrew "bash-completion")
nodestream completions bash > $(brew --prefix)/etc/bash_completion.d/nodestream.bash-completion

# ZSH - Config file
mkdir ~/.zfunc
echo "fpath+=~/.zfunc" >> ~/.zshrc
nodestream completions zsh > ~/.zfunc/_test

# FISH
nodestream completions fish > ~/.config/fish/completions/nodestream.fish