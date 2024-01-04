#!/bin/bash

set -e

CURRENT_VERSION=#(poetry version -s)

# Move up to the directory for the organization.
cd .. 

for plugin in $(ls | grep nodestream-plugin); do
    cd $plugin

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

    # Create a branch for the release.
    git checkout -b "release-$CURRENT_VERSION"
    echo "Updating $plugin"

    # Update the version number in the pyproject.toml file.
    poetry version "$CURRENT_VERSION"
    poetry add nodestream@^$CURRENT_VERSION

    # Commit the changes to the branch.
    git add pyproject.toml
    git commit -m "Release $CURRENT_VERSION"
    
    # Tag the commit with the version number.
    git tag "$CURRENT_VERSION"
    
    # Push the branch and tag to the remote repository.
    git push origin "release-$CURRENT_VERSION"
    git push origin "$CURRENT_VERSION"

    # Reset our state.
    git checkout main
    cd ..
done
