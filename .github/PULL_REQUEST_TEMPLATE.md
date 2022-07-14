- Closes #
### Required checklist
- [ ] Sample config files updated (both `/etc` folder and `NewDemoConfig` methods) (influxdb and plutonium)
- [ ] openapi swagger.yml updated (if modified API) - link openapi PR
- [ ] Signed [CLA](https://influxdata.com/community/cla/) (if not already signed)

### Description
1-3 sentences describing the PR (or link to well written issue)

### Context
Why was this added? What value does it add? What are risks/best practices?

### Affected areas (delete section if not relevant):
List of user-visible changes. As a user, what would I need to see in docs?
Examples:
CLI commands, subcommands, and flags
API changes
Configuration (sample config blocks)

### Severity (delete section if not relevant)
 i.e., ("recommend to upgrade immediately", "upgrade at your leisure", etc.)

### Note for reviewers:
Check the semantic commit type:
 - Feat: a feature with user-visible changes
 - Fix: a bug fix that we might tell a user “upgrade to get this fix for your issue”
 - Chore: version bumps, internal doc (e.g. README) changes, code comment updates, code formatting fixes… must not be user facing (except dependency version changes)
 - Build: build script changes, CI config changes, build tool updates
 - Refactor: non-user-visible refactoring
 - Check the PR title: we should be able to put this as a one-liner in the release notes
