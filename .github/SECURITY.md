# Security Policy

## Status

This project is in **early development** (pre-1.0); there are no released versions yet.
Supported versions will be listed here once releases begin.

Security is a **core design goal** of this framework (safe-by-default: mandatory
encryption, authenticated handshakes, DoS resistance, bounded deserialization).
Reports that identify gaps against that goal are especially welcome.

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Report privately via
[GitHub Security Advisories](https://github.com/itsakeyfut/game-server/security/advisories/new).

Please include:

- A description of the vulnerability and its potential impact
- Steps to reproduce or a minimal proof-of-concept
- Affected component(s) and version(s)
- Any suggested mitigations if known

You can expect an acknowledgment within **7 days** and a status update within **30 days**.

## Scope

In scope: vulnerabilities in the framework's transport / session / serialization /
security code and its reference service implementations.

Out of scope: vulnerabilities in games built on top of the framework (the game's own
logic and content), and in third-party dependencies (please report those upstream).
