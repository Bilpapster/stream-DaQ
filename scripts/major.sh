#!/bin/bash

python scripts/bump_version.py major
git add ./pyproject.toml
git commit -m "Major release $(python -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["project"]["version"])')"
git tag v$(python -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["project"]["version"])')
git push && git push --tags