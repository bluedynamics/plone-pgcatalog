# Release Process

## Version Management

This project uses `hatch-vcs` — the git tag is the single source of truth for
the version. There is no version string in any file to maintain manually.

## Prerequisites

### PyPI Trusted Publishing (one-time setup)

Both Test PyPI and PyPI use OIDC trusted publishing (no API tokens needed).

1. **Test PyPI**: Go to https://test.pypi.org/manage/project/plone.pgcatalog/settings/publishing/
   - Add a GitHub publisher: owner=`bluedynamics`, repo=`plone-pgcatalog`,
     workflow=`release.yaml`, environment=`release-test-pypi`

2. **PyPI**: Go to https://pypi.org/manage/project/plone.pgcatalog/settings/publishing/
   - Add a GitHub publisher: owner=`bluedynamics`, repo=`plone-pgcatalog`,
     workflow=`release.yaml`, environment=`release-pypi`

3. **GitHub Environments**: In the repo settings, create two environments:
   - `release-test-pypi`
   - `release-pypi` (optionally add required reviewers for extra safety)

### Tools

```bash
uv tool install hatch  # optional, for local builds
```

## Making a Release

### 1. Ensure `main` is clean and CI passes

```bash
git checkout main
git pull
pytest
```

### 2. Tag the release

Tags must follow PEP 440. The `v` prefix is optional but conventional:

```bash
git tag v0.1.0
```

For pre-releases:

```bash
git tag v0.1.0a1   # alpha
git tag v0.1.0b1   # beta
git tag v0.1.0rc1  # release candidate
```

### 3. Push the tag

```bash
git push origin v0.1.0
```

This triggers the release workflow, which builds the sdist + wheel and
publishes to **PyPI** (or Test PyPI for pre-release tags, depending on
workflow configuration).

### 4. Create a GitHub Release

1. Go to https://github.com/bluedynamics/plone-pgcatalog/releases/new
2. Select the tag you just pushed: `v0.1.0`
3. Set the release title: `v0.1.0`
4. Add release notes (or use "Generate release notes")
5. For pre-releases, check "Set as a pre-release"
6. Click "Publish release"

### 5. Verify

- Check https://pypi.org/project/plone.pgcatalog/ for the new version
- Verify installation: `uv pip install plone.pgcatalog==0.1.0`

## Building Locally (for testing)

```bash
hatch build           # creates sdist + wheel in dist/
ls dist/
# plone.pgcatalog-0.1.0.tar.gz
# plone.pgcatalog-0.1.0-py3-none-any.whl
```

Note: Without a git tag on the current commit, `hatch-vcs` generates a dev
version like `0.0.1.dev42+gabcdef0`. This is expected and correct for
development builds.

## What Gets Published

| Artifact | Contents |
|----------|----------|
| sdist (`.tar.gz`) | Source code, excluding `example/` |
| wheel (`.whl`) | Pure Python wheel (`py3-none-any`), the `plone.pgcatalog` package |

## Checklist

- [ ] All tests pass (`pytest`)
- [ ] README.md is up to date
- [ ] `main` branch is clean (no uncommitted changes)
- [ ] Tag follows PEP 440 (`v0.1.0`, not `0.1.0` or `release-0.1.0`)
- [ ] GitHub Release created with release notes
- [ ] Package visible on PyPI
