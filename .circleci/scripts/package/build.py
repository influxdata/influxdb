#!/usr/bin/env python3
import os
import re
import shutil
import subprocess
import tempfile
import yaml


def build_linux_archive(source, package, version):
    """
    Builds a Linux Archive.

    This archive contains the binary artifacts, configuration, and scripts
    installed by the DEB and RPM packages. This mimics the file-system. So,
    binaries are installed into "/usr/bin", configuration into "/etc", and
    scripts into their relevant directories. Permissions match those of
    the DEB and RPM packages.
    """
    with tempfile.TemporaryDirectory() as workspace:
        # fmt: off
        shutil.copytree(os.path.join(package["source"], "fs"),
            workspace, dirs_exist_ok=True, ignore=shutil.ignore_patterns(".keepdir"))
        # fmt: on

        for extra in package["extras"]:
            # fmt: off
            shutil.copy(extra["source"],
                os.path.join(workspace, extra["target"]))
            # fmt: on

        for binary in package["binaries"]:
            target = os.path.join(source["binary"], binary)

            if os.path.exists(target):
                # fmt: off
                shutil.copy(target,
                    os.path.join(workspace, "usr/bin", os.path.basename(target)))
                # fmt: on

        # After the package contents are copied into the working directory,
        # the permissions must be updated. Since the CI executor may change
        # occasionally (images/ORBs deprecated over time), the umask may
        # not be what we expect. This allows this packaging script to be
        # agnostic to umask/system configuration.
        for root, dirs, files in os.walk(workspace):
            for target in [os.path.join(root, f) for f in files]:
                # files in "usr/bin" are executable
                if os.path.relpath(root, workspace) == "usr/bin":
                    os.chmod(target, 0o0755)
                else:
                    # standard file permissions
                    os.chmod(target, 0o0644)
                # fmt: off
                shutil.chown(
                    target,
                    user  = "root",
                    group = "root")
                # fmt: on

            for target in [os.path.join(root, d) for d in dirs]:
                # standard directory permissions
                os.chmod(target, 0o0755)
                # fmt: off
                shutil.chown(
                    target,
                    user  = "root",
                    group = "root")
                # fmt: on

        for override in package["perm_overrides"]:
            target = os.path.join(workspace, override["target"])
            os.chmod(target, override["perms"])
            # "owner" and "group" should be a system account and group with
            # a well-defined UID and GID. Otherwise, the UID/GID might vary
            # between systems. When the archive is extracted/package is
            # installed, things may not behave as we would expect.
            # fmt: off
            shutil.chown(
                target,
                user  = override["owner"],
                group = override["group"])
            # fmt: on

        os.makedirs(source["target"], exist_ok=True)

        # fmt: off
        subprocess.check_call([
            "tar", "-czf",
            os.path.join(
                source["target"],
                "{:s}-{:s}_{:s}_{:s}.tar.gz".format(
                    package["name"],
                    version,
                    source["plat"],
                    source["arch"]
                )
            ),
            # ".keepdir" allows Git to track otherwise empty directories. The presence
            # of the directories allows `package["extras"]` and `package["binaries"]`
            # to be copied into the archive without requiring "mkdir". These should
            # directories are excluded from the final archive.
            "--exclude", ".keepdir",
            # This re-parents the contents of the archive with `package["name"]-version`.
            # It is undocumented, however, when matching, "--transform" always removes
            # the trailing slash. This regex must handle "./" and "./<more components>".
            "--transform",
            "s#^.\(/\|$\)#{:s}-{:s}/#".format(
                package["name"],
                version
            ),
            # compress everything within `workspace`
            "-C", workspace, '.'
        ])
        # fmt: on


def build_archive(source, package, version):
    """
    Builds Archive for other (not-Linux) Platforms.

    This archive contains binary artifacts and configuration. Unlike the
    linux archive, which contains the configuration and matches the file-
    system of the DEB and RPM packages, everything is located within the
    root of the archive. However, permissions do match those of the DEB
    and RPM packages.
    """
    with tempfile.TemporaryDirectory() as workspace:
        for extra in package["extras"]:
            # fmt: off
            target = os.path.join(workspace,
                os.path.basename(extra["target"]))
            # fmt: on

            shutil.copy(extra["source"], target)
            os.chmod(target, 0o0644)
            # fmt: off
            shutil.chown(
                target,
                user  = "root",
                group = "root")
            # fmt: on

        for binary in package["binaries"]:
            target = os.path.join(source["binary"], binary)

            if os.path.exists(target):
                # fmt: off
                shutil.copy(target,
                    os.path.join(workspace, os.path.basename(target)))
                # fmt: on

                os.chmod(target, 0o0755)
                # fmt: off
                shutil.chown(
                    target,
                    user  = "root",
                    group = "root")
                # fmt: on

        os.makedirs(source["target"], exist_ok=True)

        if source["plat"] == "darwin":
            # fmt: off
            subprocess.check_call([
                "tar", "-czf",
                os.path.join(
                    source["target"],
                    "{:s}-{:s}_{:s}_{:s}.tar.gz".format(
                        package["name"],
                        version,
                        source["plat"],
                        source["arch"]
                    )
                ),
                # This re-parents the contents of the archive with `package["name"]-version`.
                # It is undocumented, however, when matching, "--transform" always removes
                # the trailing slash. This regex must handle "./" and "./<more components>".
                "--transform",
                "s#^.\(/\|$\)#{:s}-{:s}/#".format(
                    package["name"],
                    version
                ),
                # compress everything within `workspace`
                "-C", workspace, '.'
            ])
            # fmt: on

        if source["plat"] == "windows":
            # preserve current working directory
            current = os.getcwd()

            for root, dirs, files in os.walk(workspace):
                for file in files:
                    # Unfortunately, it looks like "-r" cannot be combined with
                    # "-j" (which strips the path of input files). This changes
                    # directory to the current input file and *then* appends it
                    # to the archive.
                    os.chdir(os.path.join(workspace, root))

                    # fmt: off
                    subprocess.check_call([
                        "zip", "-r",
                        os.path.join(
                            os.path.join(current, source["target"]),
                            "{:s}-{:s}-{:s}.zip".format(
                                package["name"],
                                version,
                                source["plat"],
                                source["arch"]
                            )
                        ),
                        file
                    ])
                    # fmt: on

            # restore current working directory
            os.chdir(current)


def build_linux_package(source, package, version):
    """
    Constructs a DEB or RPM Package.
    """
    with tempfile.TemporaryDirectory() as workspace:
        # fmt: off
        shutil.copytree(package["source"], workspace,
            dirs_exist_ok=True, ignore=shutil.ignore_patterns(".keepdir"))
        # fmt: on

        for extra in package["extras"]:
            # fmt: off
            shutil.copy(extra["source"],
                os.path.join(workspace, "fs", extra["target"]))
            # fmt: on

        for binary in package["binaries"]:
            target = os.path.join(source["binary"], binary)

            if os.path.exists(target):
                # fmt: off
                shutil.copy(target,
                    os.path.join(workspace, "fs/usr/bin", os.path.basename(target)))
                # fmt: on

        # After the package contents are copied into the working directory,
        # the permissions must be updated. Since the CI executor may change
        # occasionally (images/ORBs deprecated over time), the umask may
        # not be what we expect. This allows this packaging script to be
        # agnostic to umask/system configuration.
        for root, dirs, files in os.walk(workspace):
            for target in [os.path.join(root, f) for f in files]:
                # files in "fs/usr/bin" are executable
                if os.path.relpath(root, workspace) == "fs/usr/bin":
                    os.chmod(target, 0o0755)
                else:
                    # standard file permissions
                    os.chmod(target, 0o0644)
                # fmt: off
                shutil.chown(
                    target,
                    user  = "root",
                    group = "root")
                # fmt: on

            for target in [os.path.join(root, d) for d in dirs]:
                # standard directory permissions
                os.chmod(target, 0o0755)
                # fmt: off
                shutil.chown(
                    target,
                    user  = "root",
                    group = "root")
                # fmt: on

        for override in package["perm_overrides"]:
            target = os.path.join(workspace, "fs", override["target"])
            os.chmod(target, override["perms"])
            # "owner" and "group" should be a system account and group with
            # a well-defined UID and GID. Otherwise, the UID/GID might vary
            # between systems. When the archive is extracted/package is
            # installed, things may not behave as we would expect.
            # fmt: off
            shutil.chown(
                target,
                user  = override["owner"],
                group = override["group"])
            # fmt: on

        os.makedirs(source["target"], exist_ok=True)
        fpm_wrapper(source, package, version, workspace, "rpm")
        fpm_wrapper(source, package, version, workspace, "deb")


def fpm_wrapper(source, package, version, workspace, package_type):
    """
    Constructs either a DEB/RPM Package.

    This wraps some configuration settings that are *only* relevant
    to `fpm`.
    """

    conffiles = []
    for root, dirs, files in os.walk(os.path.join(workspace, "fs/etc")):
        for file in files:
            # fmt: off
            conffiles.extend([
                "--config-files", os.path.join("/",  os.path.relpath(root, os.path.join(workspace, "fs")), file)
            ])
            # fmt: on

    # `source["arch"]` matches DEB architecture names. When building RPMs, it must
    #  be converted into RPM architecture names.
    architecture = source["arch"]
    if package_type == "rpm":
        if architecture == "amd64":
            architecture = "x86_64"
        if architecture == "arm64":
            architecture = "aarch64"

    # fmt: off
    p = subprocess.check_call([
        "fpm",
        "--log",            "error",
        # package description
        "--name",           package["name"],
        "--vendor",         "InfluxData",
        "--description",    "Distributed time-series database.",
        "--url",            "https://influxdata.com",
        "--maintainer",     "support@influxdb.com",
        "--license",        "MIT",
        # package configuration
        "--input-type",     "dir",
        "--output-type",    package_type,
        "--architecture",   architecture,
        "--version",        version,
        "--iteration",      "1",
        # maintainer scripts
        "--after-install",  os.path.join(workspace, "control/postinst"),
        "--after-remove",   os.path.join(workspace, "control/postrm"),
        "--before-install", os.path.join(workspace, "control/preinst"),
        # package relationships
        "--deb-recommends", "influxdb2-cli",
        "--conflicts",      "influxdb",
        "--depends",        "curl",
        # package conffiles
        *conffiles,
        # package options
        "--chdir",          os.path.join(workspace, "fs/"),
        "--package",        source["target"]
    ])
    # fmt: on


circle_tag = os.getenv("CIRCLE_TAG", default="")
circle_sha = os.getenv("CIRCLE_SHA1", default="DEADBEEF")
# Determine if `circle_tag` matches the semantic version regex. Otherwise,
# assume that `circle_tag` is not intended to tag a release. The regex is
# permissive of what occurs after the semantic version. This allows for
# alphas, betas, and release candidates.
if re.match("^v[0-9]+.[0-9]+.[0-9]+", circle_tag):
    version = circle_tag[1:]
else:
    # When `circle_tag` cannot be used to construct the package version,
    # use `circle_sha`. Since `circle_sha` can start with an alpha (non-
    # -numeric) character, prefix it with "2.x-".
    version = "2.x-" + circle_sha[:8]

with open(".circleci/scripts/package/config.yaml") as file:
    document = yaml.load(file, Loader=yaml.SafeLoader)

    # fmt: off
    for s, p in [
        (s, p)
        for s in document["sources" ]
        for p in document["packages"]
    ]:
    # fmt: on
        if s["plat"] == "linux":
            build_linux_archive(s, p, version)
            build_linux_package(s, p, version)
        if s["plat"] == "darwin" or s["plat"] == "windows":
            build_archive(s, p, version)
