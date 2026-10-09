# Contributing to Nisshi

Thanks for considering a contribution. This document covers how to file issues,
submit changes, and get them merged.

## Before you start

For anything beyond a small fix, open an issue first. It saves everyone time
if a design direction gets agreed before code is written. Bug reports and
typo fixes can go straight to a pull request.

## What we accept

Most contributions — bug fixes, new features, documentation, and tests — are
welcome through the normal pull request flow described below. Changes with
broader impact, as defined in [GOVERNANCE.md](GOVERNANCE.md#decision-making)
(for example a breaking API or wire-protocol change, a new storage backend,
or a change to the governance model itself), need discussion and agreement
from a majority of maintainers before they merge, not just one reviewer's
approval. If you're unsure which category a change falls into, ask in an
issue before opening the PR.

## Development setup

The project uses [`just`](https://github.com/casey/just) as its task runner:

```shell
cp example.env .env    # edit as needed
just ci                # start postgres, minio, and lakehouse via docker compose
just                   # fmt, build, test, clippy
```

See `CLAUDE.md` at the repo root for the full command reference and
architecture overview.

## Making a change

1. Fork the repo and create a branch off `main`.
2. Make your change. Match the existing code style; `just fmt` and
   `just clippy` will catch most of it. Write comments by the
   [comment rules](.claude/rules/comments.md), and choose names by the
   [naming rules](.claude/rules/naming.md).
3. Add or update tests. `just test` runs the full suite.
4. Open a pull request against `main`. Fill in the PR template — it's short
   on purpose. Merging requires an approval from a code owner and all
   required checks passing; see `.github/CODEOWNERS` for who that is.

Keep pull requests focused on one change. A large, mixed-purpose PR is
harder to review and harder to revert if something goes wrong.

## Sign off your commits (DCO)

This project's contributor-agreement position is the Developer Certificate
of Origin: no separate contributor license agreement (CLA) is required.

Every commit must include a `Signed-off-by` line certifying you wrote it or
otherwise have the right to submit it under this project's license (the
[Developer Certificate of Origin](https://developercertificate.org/)).

Add it automatically:

```shell
git commit -s -m "your commit message"
```

If you forgot on commits already made:

```shell
git rebase --signoff main
```

This is a new expectation for the project going forward — existing history
predates it, and there's no automated check enforcing it yet. Please sign
off new commits from here on; a reviewer will ask if it's missing.

## Code of conduct

Participation in this project is governed by our
[Code of Conduct](CODE_OF_CONDUCT.md).

## Reporting a security issue

Don't open a public issue for a security vulnerability — see
[SECURITY.md](SECURITY.md) instead.
