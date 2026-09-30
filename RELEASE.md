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

### 2. Date the changelog section

`CHANGES.md` carries the upcoming version as `## <version> (unreleased)` while
work accumulates. Replace `(unreleased)` with today's date and commit that
alone:

```bash
# ## 1.0.0rc2 (unreleased)  ->  ## 1.0.0rc2 (2026-09-30)
git add CHANGES.md
git commit -m "Release 1.0.0rc2"
git push origin main
```

If the version that ended up in the header is not the one you are releasing,
fix it here.

### 3. Tag the release

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

### 4. Push the tag

```bash
git push origin v0.1.0
```

Pushing the tag publishes nothing on its own -- no workflow in `release.yaml`
triggers on a tag push. The tag matters because `hatch-vcs` derives the version
from it, and the GitHub Release in the next step has to point at it.

### 5. Create a GitHub Release

**This is the step that publishes to PyPI.** The `release-pypi` job runs only
on the `release: published` event.

With the `gh` CLI:

```bash
gh release create v0.1.0 --title v0.1.0 --verify-tag --notes-file notes.md
```

Add `--prerelease` for alpha, beta, and release-candidate tags.

Or in the web UI:

1. Go to https://github.com/bluedynamics/plone-pgcatalog/releases/new
2. Select the tag you just pushed: `v0.1.0`
3. Set the release title: `v0.1.0`
4. Add release notes (or use "Generate release notes")
5. For pre-releases, check "Set as a pre-release"
6. Click "Publish release"

The `release-pypi` environment may have required reviewers configured, in which
case the upload waits for an approval.

### 6. Verify

- Watch the run: `gh run list --event release --limit 1`
- Check https://pypi.org/project/plone.pgcatalog/ for the new version
- Verify installation: `uv pip install plone.pgcatalog==0.1.0`
  (a pre-release needs that exact pin, or `--pre`)

## What triggers what

`release.yaml` has three triggers, and only one of them reaches PyPI:

| Trigger | Publishes to | When |
|----------|--------------|------|
| `workflow_run` after CI on `main` | Test PyPI | every successful CI run on `main`, as a `.dev` version |
| `release: published` | PyPI | you publish a GitHub Release |
| `workflow_dispatch` on `main` | Test PyPI | manual run |

Every merge to `main` therefore lands a dev version on Test PyPI by itself.
Nothing reaches PyPI until a GitHub Release is published.

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
- [ ] CI is green on `main`
- [ ] README.md is up to date
- [ ] `CHANGES.md` section for this version is dated, not `(unreleased)`
- [ ] `main` branch is clean (no uncommitted changes)
- [ ] Tag follows PEP 440 (`v0.1.0`, not `0.1.0` or `release-0.1.0`)
- [ ] Tag pushed
- [ ] GitHub Release published -- this, not the tag, is what uploads to PyPI
- [ ] Pre-release flag set for alpha, beta, and rc tags
- [ ] Package visible on PyPI
