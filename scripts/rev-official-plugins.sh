#!/bin/bash

set -e

ORIGINAL_DIR="$(pwd)"
trap 'cd "$ORIGINAL_DIR"' EXIT

CURRENT_VERSION=$(poetry version -s)

# Discover all plugin repos matching the nodestream-plugin-* prefix
PLUGINS=(../nodestream-plugin-*)

for plugin_path in "${PLUGINS[@]}"; do
    # Skip if the glob didn't match anything or entry isn't a directory
    [ -d "$plugin_path" ] || continue

    plugin=$(basename "$plugin_path")
    cd "$plugin_path"

    # Determine the default branch for this repository (prefer main, then master).
    if git rev-parse --verify main >/dev/null 2>&1; then
        DEFAULT_BRANCH=main
    elif git rev-parse --verify master >/dev/null 2>&1; then
        DEFAULT_BRANCH=master
    else
        DEFAULT_BRANCH=$(git branch --show-current)
    fi

    echo "Stashing changes for $plugin"
    git stash || true
    git checkout "$DEFAULT_BRANCH"

    # Fetch all remote refs so that remote tracking branches (including release
    # branches) are available locally for checkout.
    git fetch origin

    # Fast-forward the default branch. If we can't, skip this plugin.
    if ! git merge --ff-only "origin/$DEFAULT_BRANCH"; then
        echo "Cannot fast-forward $DEFAULT_BRANCH for $plugin; skipping this plugin."
        cd - >/dev/null
        continue
    fi

    # Checkout the release branch or create it if it doesn't exist.
    REMOTE_HAS_RELEASE=$(git ls-remote --heads origin "refs/heads/release-${CURRENT_VERSION}" | head -1)
    if git show-ref --verify --quiet "refs/heads/release-$CURRENT_VERSION"; then
        echo "Checking out existing local release-$CURRENT_VERSION"
        git checkout "release-$CURRENT_VERSION"
    elif [ -n "$REMOTE_HAS_RELEASE" ]; then
        echo "Creating local tracking branch release-$CURRENT_VERSION from origin"
        git checkout -b "release-$CURRENT_VERSION" "origin/release-$CURRENT_VERSION"
    else
        echo "Creating new release-$CURRENT_VERSION from $DEFAULT_BRANCH"
        git checkout -b "release-$CURRENT_VERSION" "$DEFAULT_BRANCH"
    fi

    echo "Updating $plugin"

    # Try multiple Python versions for this plugin's Poetry environment.
    PYTHON_CANDIDATES=("3.13" "3.12" "3.11")
    UPDATE_SUCCEEDED=false

    for PY_VER in "${PYTHON_CANDIDATES[@]}"; do
        echo "Attempting update for $plugin using Python $PY_VER"

        # Try to switch Poetry's virtualenv to the requested Python version.
        # If this fails (Python not installed), just try the next version.
        if ! poetry env use "$PY_VER" >/dev/null 2>&1; then
            echo "Python $PY_VER not available for $plugin; trying next version."
            continue
        fi

        # Reset any partial changes before retrying with a new Python.
        git restore pyproject.toml poetry.lock 2>/dev/null || true

        # Update the version number in the pyproject.toml file.
        if ! poetry version "$CURRENT_VERSION"; then
            echo "Failed to bump version for $plugin with Python $PY_VER; trying next version."
            continue
        fi

        # Attempt to update the nodestream dependency.
        if poetry add "nodestream@^$CURRENT_VERSION"; then
            UPDATE_SUCCEEDED=true
            break
        else
            echo "Poetry dependency update failed for $plugin with Python $PY_VER; trying next version."
        fi
    done

    if [ "$UPDATE_SUCCEEDED" = false ]; then
        echo "All Python versions failed for $plugin; reverting and skipping this plugin."
        git restore pyproject.toml poetry.lock 2>/dev/null || true
        git checkout "$DEFAULT_BRANCH"
        cd - >/dev/null
        continue
    fi

    # Only commit/tag/push if there are actual changes.
    if ! git diff --quiet; then
        git add -A
        git commit -m "Release $CURRENT_VERSION"
        
        # Tag the commit with the version number, forcing update if the tag exists.
        git tag --force "$CURRENT_VERSION"
        
        git push origin "release-$CURRENT_VERSION"
        git push origin "$CURRENT_VERSION"
    else
        echo "No changes to commit for $plugin; skipping commit, tag, and push."
    fi

    git checkout "$DEFAULT_BRANCH"
    cd - >/dev/null
done
