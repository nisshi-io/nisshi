# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `DeleteGroups` refuses a group that still has members or a rebalance in
  progress with `NON_EMPTY_GROUP` (68), instead of deleting its state and
  committed offsets out from under a live consumer. The check runs through
  the group coordinator rather than storage alone, so a member whose
  session has expired (no `LeaveGroup` ever sent) is still correctly
  evicted first and the group remains deletable once genuinely empty; a
  group actually deleted has its coordinator-cached state forgotten too,
  so a new member joining under the same, just-freed group name starts a
  real new group instead of reusing stale state. `DescribeGroups` and
  `DeleteGroups` on the PostgreSQL and libSQL (including Turso) storage
  engines no longer error for a group that only ever committed offsets and
  never ran `JoinGroup` (a group row with no detail row); that case is now
  correctly reported as an empty group.
