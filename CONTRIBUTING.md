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

Before you ask for review, check that:

- every commit is signed off (see [below](#sign-off-your-commits-dco));
- `just fmt`, `just clippy`, and `just test` pass locally;
- tests cover the behavior you changed;
- the docs describe any user-facing behavior you changed.

## Pull request titles and merging

Pull requests are squash-merged through the merge queue. Each one becomes a
single commit on `main`: the PR title is the commit's subject, and the PR
description is its body. The changelog is generated from these commits when
a release is cut, so don't edit `CHANGELOG.md` in a pull request. Write the
description for someone reading `git log`: what a user or operator sees
differently, and why.

The title must follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/),
and a check fails the PR until it does:

```text
type(scope): summary
```

- `type` is one of `feat`, `fix`, `perf`, `refactor`, `docs`, `test`,
  `build`, `ci`, `chore`, or `revert`.
- `scope` is optional. Use the crate or storage backend the change is about,
  for example `dynostore`, `sql`, or `proxy`.
- Add `!` after the type or scope for a breaking change, for example
  `feat(sql)!: summary`, and say what breaks in the description.
- GitHub's Revert button titles its pull request `Revert "<title>"`, which
  fails the check. Retitle it `revert: <title>`.

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

Pull requests are squash-merged, so the commit on `main` doesn't carry the
`Signed-off-by` lines. The commits in the pull request are the record, so
sign off each of them.

This is a new expectation for the project going forward — existing history
predates it, and there's no automated check enforcing it yet. Please sign
off new commits from here on; a reviewer will ask if it's missing.

## Code of conduct

Participation in this project is governed by our
[Code of Conduct](CODE_OF_CONDUCT.md).

## Reporting a security issue

Don't open a public issue for a security vulnerability — see
[SECURITY.md](SECURITY.md) instead.
