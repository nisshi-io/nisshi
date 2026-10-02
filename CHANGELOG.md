# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- ListOffsets reads each partition from storage separately, up to 4 at once, and answers a partition still unread after 5 seconds with `REQUEST_TIMED_OUT`, which clients retry. A slow partition no longer delays the others or holds the request past the client's timeout.
