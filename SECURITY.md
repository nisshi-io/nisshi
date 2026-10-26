# Security Policy

## Supported Versions

Nisshi is pre-1.0 and moves quickly. Security fixes land on `main` and ship
in the next release. We don't maintain older release branches, so only the
latest release is supported. Upgrade to get security fixes.

## Reporting a Vulnerability

Please don't open a public issue, pull request or discussion for a security
vulnerability.

### How to report

**Preferred:** use [GitHub private vulnerability reporting][private-vuln]
(Security tab, then Report a vulnerability). It creates a private advisory
that only the maintainers and repository administrators can see until a
fix is ready.

**Fallback:** if you don't have a GitHub account, email
[support@solace.com](mailto:support@solace.com). Use this address for
vulnerability reports only.

### What to include

- The Nisshi version or commit you tested, and the storage backend in use
- A description of the vulnerability and its impact
- Steps to reproduce, or a proof of concept
- Any configuration that matters (authentication, TLS)
- Your preferred name for credit, if you want it

A partial report is still useful. Send what you have.

## What happens next

1. **Acknowledge.** We confirm we received your report.
2. **Assess.** We try to reproduce the issue and judge its severity. We may
   ask you for more detail.
3. **Fix.** If we confirm the issue, we develop and test a fix in private. If
   we can't confirm it, or decide not to fix it, we tell you, and you are free
   to disclose it. Please tell us before you do.
4. **Disclose.** We coordinate disclosure with you, release the fix, and
   publish a security advisory.
5. **Credit.** We credit you in the advisory if you want to be named.

## Scope

Reports we are interested in include:

- Authentication bypass (SASL, SCRAM)
- Crashes, hangs or unbounded resource use triggered by a crafted request
  on the Kafka wire protocol
- Data exposure or corruption through the broker or its storage backends
- Flaws in how Nisshi handles credentials or TLS

Out of scope: Nisshi has no authorizer yet. Every client that connects, or
that completes SASL authentication when the broker runs with
`--authentication`, can read, write and administer every topic and group,
and can create, change or delete any user's SCRAM credentials. A report that
shows only this is a known limitation, not a vulnerability. A client that
does any of this without completing authentication on a broker started with
`--authentication` is still in scope.

`nisshi proxy` does not authenticate clients either. Its origin broker must
not require authentication, so front only a broker you would expose without
`--authentication`. Access through the proxy is the same known limitation.

For vulnerabilities in a third-party dependency, report them to that
project. If Nisshi's use of the dependency makes the issue exploitable, tell
us too.

## Please don't

- Open public issues for vulnerabilities
- Disclose a vulnerability publicly before the date we agree with you or, if
  we have not agreed one, before a fix is released, unless we have told you we
  can't confirm or won't fix it
- Access, change or delete data that isn't yours
- Run tests against systems you don't own

[private-vuln]: https://github.com/nisshi-io/solace-nisshi/security/advisories/new
