#!/bin/bash

python scripts/bump_version.py minor
git add ./pyproject.toml
git commit -m "Release version $(python -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["project"]["version"])')"
git tag v$(python -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["project"]["version"])')
git push && git push --tags
