# Governance

Nisshi is maintained by the group listed in [`.github/CODEOWNERS`](.github/CODEOWNERS).
There is no single project lead; decisions are made by the maintainer group
as a whole.

## Decision-making

Every change merges through the same mechanism, enforced by the repository's
branch protection: at least one code owner's approval and all required
checks passing. That's a floor, not the whole story of how decisions get
made above it.

For most changes — bug fixes, features, dependency updates, documentation —
that one approval is effectively **lazy consensus**: whoever reviews first
and is satisfied merges it, and anyone who disagrees can say so during that
same review.

Changes with broader impact — a breaking API or wire-protocol change, a new
storage backend, a change to this governance model — need explicit
agreement from a majority of active maintainers discussed before merging,
not just one reviewer's approval.

Disagreements that don't resolve through discussion are decided by a vote of
active maintainers, simple majority.

## Becoming a maintainer

Maintainer status is offered to contributors who have made sustained,
substantive contributions and shown good judgment in reviews and design
discussion. An existing maintainer nominates the candidate; the nomination
passes by lazy consensus among current maintainers. New maintainers are
added to `CODEOWNERS` and given write access.

Maintainers who are no longer active are welcome to step back at any time,
and may be moved to an emeritus/inactive state after a sustained period of
inactivity, decided by the same lazy-consensus process.

## Code of Conduct

All participation in the project, including maintainer decisions, is
governed by our [Code of Conduct](CODE_OF_CONDUCT.md).
