This guide is meant for maintainers of the Stream DaQ project. 
**If you are a user, this guide IS NOT for you :)** But, we got you covered! Here is the [official documentation](https://bilpapster.github.io/stream-DaQ/), covering things that really matter to you! If you are a maintainer, you are probably here because you want to bump the version of Stream DaQ. This guide will help you understand how to do that correctly.

## Versioning Guide for Stream DaQ (MAINTAINERS ONLY)

### Versioning Scheme
Stream DaQ follows [Semantic Versioning](https://semver.org/). The version number
is structured as `MAJOR.MINOR.PATCH`, where:
- **MAJOR** version changes indicate incompatible API changes.
- **MINOR** version changes add functionality in a backward-compatible manner.
- **PATCH** version changes are for backward-compatible bug fixes.

If you are not sure about the bump type, then it is probably `patch`. But here is a quick cheat sheet to help you decide:

### Versioning Steps
1. **Identify the type of change**:
   - **Bug Fix**: Increment the PATCH version.
   - **New Feature**: Increment the MINOR version (after discussing with Vassilis).
   - **Breaking Change**: Increment the MAJOR version (after discussing with Vassilis).
   - **Documentation Update**: No version change needed.
   - **Dependency Update**: No version change needed unless it affects functionality.

2. Use the `release.sh` script to automate the versioning process. The script only needs the type of the bump (e.g.,`scripts/release.sh patch`). Please run this command from the root directory of the project.

    **IMPORTANT:** This script will (automatically) do the following:
   - Update the version in `pyproject.toml`.
   - Create a new tag in Git.
   - Push the changes to the remote repository.
   - Create a new release on GitHub.
    
    **This means that all changed files in your working directory will be committed and pushed to the remote repository.** If this is not the functionality you want, you can temporarily add the word `ignore` at any place in the name of the files you do not want to commit (yet). There is already a rule for this (`*ignore*`) in `.gitignore`, so they will not be committed. If after reading this you are still unsure about how to proceed, ask Vassilis :)

If the process is successful, you will see a message like this:
```
└❯ bash scripts/release.sh patch # script runs from the root directory
🚀 Preparing patch release...
Current version: 0.1.9
New version: 0.1.10
✅ Version bumped to 0.1.10
Next steps:
1. Review changes: git diff
2. Commit changes: git add . && git commit -m 'Bump version to 0.1.10'
3. Create tag: git tag v0.1.10
4. Push: git push && git push --tags
📝 New version: 0.1.10
[main eb9cc7c] Bump version to 0.1.10
 1 file changed, 1 insertion(+), 1 deletion(-)
✅ Version bumped and tagged!
🚀 Pushing to GitHub (this will trigger the release)...
Enumerating objects: 12, done.
Counting objects: 100% (12/12), done.
Delta compression using up to 20 threads
Compressing objects: 100% (8/8), done.
Writing objects: 100% (8/8), 1.16 KiB | 1.16 MiB/s, done.
Total 8 (delta 5), reused 0 (delta 0), pack-reused 0
remote: Resolving deltas: 100% (5/5), completed with 4 local objects.
To https://github.com/Bilpapster/stream-DaQ.git
   636de78..eb9cc7c  main -> main
Total 0 (delta 0), reused 0 (delta 0), pack-reused 0
To https://github.com/Bilpapster/stream-DaQ.git
 * [new tag]         v0.1.10 -> v0.1.10
 * [new tag]         v0.1.9 -> v0.1.9
🎉 Release 0.1.10 initiated!
Check GitHub Actions for build status: https://github.com/bilpapster/stream-DaQ/actions
```

If so, well done! You have successfully bumped the version of Stream DaQ! If not, please check the error message and try to fix it. If you are still stuck, ask Vassilis for help.
