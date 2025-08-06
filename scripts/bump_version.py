"""Version management script for Stream DaQ"""

import argparse
import re
import sys
from pathlib import Path


def get_current_version():
    """Get current version from pyproject.toml"""
    pyproject_path = Path("pyproject.toml")
    content = pyproject_path.read_text()

    match = re.search(r'version = "([^"]+)"', content)
    if not match:
        raise ValueError("Version not found in pyproject.toml")

    return match.group(1)


def bump_version(current_version, bump_type):
    """Bump version according to semantic versioning"""
    major, minor, patch = map(int, current_version.split('.'))

    if bump_type == 'patch':
        patch += 1
    elif bump_type == 'minor':
        minor += 1
        patch = 0
    elif bump_type == 'major':
        major += 1
        minor = 0
        patch = 0
    else:
        raise ValueError(f"Invalid bump type: {bump_type}")

    return f"{major}.{minor}.{patch}"


def update_version_in_file(new_version):
    """Update version in pyproject.toml"""
    pyproject_path = Path("pyproject.toml")
    content = pyproject_path.read_text()

    updated_content = re.sub(
        r'version = "[^"]+"',
        f'version = "{new_version}"',
        content
    )

    pyproject_path.write_text(updated_content)


def main():
    parser = argparse.ArgumentParser(description='Bump Stream DaQ version')
    parser.add_argument('bump_type', choices=['patch', 'minor', 'major'],
                        help='Type of version bump')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without making changes')

    args = parser.parse_args()

    try:
        current_version = get_current_version()
        new_version = bump_version(current_version, args.bump_type)

        print(f"Current version: {current_version}")
        print(f"New version: {new_version}")

        if not args.dry_run:
            update_version_in_file(new_version)
            print(f"✅ Version bumped to {new_version}")
            print("Next steps:")
            print("1. Review changes: git diff")
            print(f"2. Commit changes: git add . && git commit -m 'Bump version to {new_version}'")
            print(f"3. Create tag: git tag v{new_version}")
            print("4. Push: git push && git push --tags")
        else:
            print("(Dry run - no changes made)")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()