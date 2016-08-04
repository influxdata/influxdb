# Compatibility Guarantee
## Introduction

The release of InfluxDB version 1 is a major milestone in the
development of the software lifecycle. InfluxDB 1 is a stable platform
for the growth of programs and projects that utilize InfluxDB as a
database. Unless otherwise stated, these compatibility guarantees apply
to every public interface on both the server and the client.

## Expectations

Although we expect that the vast majority of users will maintain this
compatibility over time, it is impossible to guarantee that no future
change will break any program. This document is an attempt to set
expectations for the compatibility of InfluxDB 1 in the future. There
are a number of ways in which a client or server that works today may
fail to do so after a future point release. They are unlikely but worth
recording.

* Security. A security issue in the specification or implementation may
  come to light whose resolution requires breaking compatibility. We
  reserve the right to address such security issues.
* Unspecified behavior. The InfluxDB specification tries to be explicit
  about most properties of the database, but there are some aspects that
  are undefined. Clients that depend on such unspecified behavior may
  break in future releases.
* Specification errors. If it becomes necessary to address an
  inconsistency or incompleteness in the specification, resolving the
  issue could affect existing clients. We reserve the right to address
  such issues. Except for security issues, no incompatible changes to
  the specification would be made.
* Bugs. If the software has a bug that violates the specification, a
  program that depends on the buggy behavior may break if the bug is
  fixed. We reserve the right to fix such bugs.
* Go packages in the `influxdb` repository. As part of normal
  development, exposed functions may become obsolete or may need to be
  changed. If you need to use one of these packages, including the
  client packages, please use some kind of vendoring or version pinning.
* Bare identifiers. InfluxQL identifiers do not need to be surrounded by
  quotes, but identifiers that are not surrounded by quotes may be
  mistaken for a reserved keyword. We reserve the right to add reserved
  keywords which may cause a bare identifier to start being interpreted
  as a reserved keyword instead. Client libraries should surround all
  identifiers with double quotes to maintain backwards compatibility.
* Private identifiers. InfluxQL identifiers that begin with an
  underscore are considered private identifiers and may be added,
  removed or changed between releases. Identifiers that start with an
  underscore may become reserved for a specific purpose in future
  releases.

Of course, for all these possibilities, should they arise, we would
endeavor whenever feasible to update the specification, configuration,
binaries, or libraries without affecting existing clients or code.

These same considerations apply to successive point releases. For
instance, servers and clients that work with InfluxDB 1.2 should be
compatible with InfluxDB 1.2.1, InfluxDB 1.3, InfluxDB 1.4, etc.,
although not necessarily with InfluxDB 1.1 since it may use added
features or have performed migrations only in InfluxDB 1.2.

Migrations will be automatic between successive point releases. If a
migration would not be reversible, an automatic backup will be taken.

Features added between releases, available in the source repository but
not part of the numbered binary releases, are under active development.
No promise of compatibility is made for software using such features
until they have been released.

Added features may be included in beta status for a given release. Any
feature that is in beta will be clearly labeled in the documentation and
is not subject to these compatibility guarantees.

Finally, although it is not a correctness issue, it is possible that the
performance of a write or query may be affected by changes in the
implementation of the server upon which it depends. No guarantee can be
made about the performance of a given call between releases.

Report unspecified behavior in the specification by opening an issue
with the [documentation](https://github.com/influxdata/docs.influxdata.com).

## Credits

This document is adapted from the Go 1 compatibility guarantee. Much of
the language in this document is taken from that document, but with
information for InfluxDB instead of Go.
