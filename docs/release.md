# Release

Releases are driven by GoReleaser and GitHub Actions.

## Create a release

Tag and push a version:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The release workflow builds archives for Linux, macOS, and Windows, publishes checksums, creates a GitHub Release, and updates the Homebrew formula when `HOMEBREW_REPO_TOKEN` is configured.

## Validate release config

```bash
goreleaser check
```
