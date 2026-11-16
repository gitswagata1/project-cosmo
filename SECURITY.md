# Security Policy

## Supported versions

Security fixes are applied to the latest version on the `main` branch.

## Reporting a vulnerability

Please **do not open a public issue** for security problems.

Report privately through GitHub:
[Report a vulnerability](https://github.com/gitswagata1/project-cosmo/security/advisories/new).

Include, where possible:

- A description of the issue and its impact
- Steps to reproduce, or a proof of concept
- Affected versions or commits

You can expect an acknowledgement within **72 hours** and a status update within **7 days**.
Once a fix is released, the report is disclosed responsibly and the reporter credited if
they wish.

## Good practice for users

- Never commit `.env` files, API keys or tokens. Use environment variables or a secret manager.
- Rotate any credential that may have been exposed.
- Keep dependencies up to date; this repository uses automated dependency updates.
