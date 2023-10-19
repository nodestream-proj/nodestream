#!/bin/bash

# This script is used to release a new version of the project.
# It accepts a single argument, which is the version number to release.
# It will perform the following operations:
#
#   1.) Update the version in the pyproject.toml file.
#   2.) Create a branch for the release. 
#   3.) Commit the changes to the branch.
#   4.) Tag the commit with the version number.
#   5.) Push the branch and tag to the remote repository.
#   6.) Publish the docs to GitHub Pages.
#   7.) Publish the package to PyPI.
#
# Before doing so, it will make sure that you are on the main branch.
# It will also make sure that the working directory is clean and that
# there are no untracked files. 
#
# NOTE: You must have the Poetry package manager installed to use this script and have poetry configured with a PyPI token.

set -e

# Validate that the version number was provided.
if [ -z "$1" ]; then
    echo "Usage: $0 <version>"
    exit 1
fi

# Validate that the working directory is clean.
if [ -n "$(git status --porcelain)" ]; then
    echo "Working directory is not clean."
    exit 1
fi

# Validate that we are on the main branch.
if [ "$(git branch --show-current)" != "main" ]; then
    echo "Not on main branch."
    exit 1
fi

# Validate that the version number is not already in use.
if [ -n "$(git tag --list "$1")" ]; then
    echo "Version number already in use."
    exit 1
fi

# Update the version number in the pyproject.toml file.
poetry version "$1"

# Create a branch for the release.
git checkout -b "release-$1"

# Commit the changes to the branch.
git add pyproject.toml
git commit -m "Release $1"

# Tag the commit with the version number.
git tag "$1"

# Push the branch and tag to the remote repository.
git push --set-upstream origin "release-$1"
git push origin "$1"
git push --tags 

# Publish the docs to GitHub Pages.
poetry run mike deploy --push --update-aliases "$1" latest

# Publish the package to PyPI.
poetry publish --build

# Switch back to the main branch.
git checkout main
