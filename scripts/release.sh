#!/bin/bash
set -e

BUMP_TYPE=$1

if [ -z "$BUMP_TYPE" ]; then
    echo "Usage: ./scripts/release.sh [patch|minor|major]"
    echo "Example: ./scripts/release.sh patch"
    exit 1
fi

echo "🚀 Preparing $BUMP_TYPE release..."

# Bump version
python scripts/bump_version.py $BUMP_TYPE

# Get new version
NEW_VERSION=$(python -c "import tomllib; print(tomllib.load(open('pyproject.toml', 'rb'))['project']['version'])")

echo "📝 New version: $NEW_VERSION"

# Commit and tag
git add pyproject.toml
git commit -m "Bump version to $NEW_VERSION"
git tag "v$NEW_VERSION"

echo "✅ Version bumped and tagged!"
echo "🚀 Pushing to GitHub (this will trigger the release)..."

git push && git push --tags

echo "🎉 Release $NEW_VERSION initiated!"
echo "Check GitHub Actions for build status: https://github.com/bilpapster/stream-DaQ/actions"