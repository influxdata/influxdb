The InfluxDB 3 Processing engine is an embedded Python VM for running code
inside the database to process and transform data. This document discusses how
the processing engine is built within InfluxDB. For usage instructions, see:
https://docs.influxdata.com/influxdb3/core/

See the 'Discussion' section for more information on why the processing engine
is implemented the way it is.


## Implementation

InfluxDB uses the [PYO3 crate](https://crates.io/crates/pyo3) to build InfluxDB
with an embedded python and the processing engine is enabled during the build
by specifying the `system-py` feature to `cargo build`. Eg:

```
$ cargo build --features system-py
```

PYO3 will then inspect the system to find a python runtime to build and link
against. The resulting `influxdb3` binary will be dynamically linked to the
`libpython` that PYO3 found during the build. Eg, on a typical Debian or Ubuntu
system, if you install the following, then InfluxDB can be built against the
system's python:

```sh
# build dependencies
$ sudo apt-get install build-essential pkg-config libssl-dev clang lld \
    git protobuf-compiler python3 python3-dev python3-pip

# runtime dependencies
$ sudo apt-get install python3 python3-pip python3-venv

# build
$ cargo build --features system-py
```

The choice of python can be influenced by setting the `PYTHONHOME` environment
variable for `cargo build` or creating a `PYO3_CONFIG_FILE` file for more
specialized setups (such as 'Official builds', below). For details, see
https://pyo3.rs/main/building-and-distribution.html

In order for InfluxDB to successfully use the python it was built against, the
same `libpython` version as well as the full runtime environment of the python
install (ie, its standard library) must be available to InfluxDB in a location
that it can find it. Building against the system python can be a reasonable
choice for users who target their builds to a specific release of an OS as
InfluxDB will simply use the installed python from the system.


## Official builds

To provide a consistent, robust and maintained python environment for InfluxDB
that is portable across a range of operating systems, InfluxData's official
InfluxDB is built against a pre-built release of
[python-build-standalone](https://astral.sh/blog/python-build-standalone) (a
CPython standalone python distribution). For a given release of InfluxDB,
official builds will use the same version of python for all install methods and
operating systems.

The following operating systems and architectures are currently supported:

 * Linux amd64/arm64 (`tar.gz`, `deb` and `rpm`)
 * Darwin arm64 (`tar.gz`)
 * Windows amd64 (`zip`)
 * Docker (Linux amd64)

Due to constraints with `python-build-standalone` and statically linking, all
builds are dynamically linked to `python-build-standalone`'s `libpython` as
well as a few OS-specific libraries. Specifically:

 * Linux (seen with `ldd` and `strings` on the binary):
   * `python-build-standalone` is linked against `glibc` and is compatible with
     `glibc` [2.17+](https://github.com/astral-sh/python-build-standalone/blob/main/docs/running.rst#linux)
   * `influxdb3` is linked against `libpython` from `python-build-standalone` as
     well as `glibc` (currently compatible with `glibc` 2.36+ (though 2.35 is
     known to work; future releases will be built against an earlier `glibc`
     release to improve compatibility))
 * Darwin (seen with `otool -L`; cross-compiled with `osxcross`):
   * `python-build-standalone` is linked against:
     * `CoreFoundation.framework/Versions/A/CoreFoundation` compatibility
       version 150.0.0
     * `libSystem.B.dylib` compatibility version 1.0.0
   * `influxdb3` is linked against:
     * `CoreFoundation.framework/Versions/A/CoreFoundation` compatibility
       version 150.0.0
     * `IOKit.framework/Versions/A/IOKit` compatibility version 1.0.0
     * `libiconv.2.dylib` compatibility version 7.0.0
     * `libobjc.A.dylib` compatibility version 1.0.0
     * `libSystem.B.dylib` compatibility version 1.0.0
     * `Security.framework/Versions/A/Security` compatibility version 1.0.0
     * `SystemConfiguration.framework/Versions/A/SystemConfiguration`
       compatibility version 1.0.0
 * Windows (seen with `dumpbin /HEADERS ...` and `dumpbin /DEPENDENTS ...`):
   * `python-build-standalone` claims [Windows 8/Windows Server 2012](https://github.com/astral-sh/python-build-standalone/blob/main/docs/running.rst#windows) or newer. Specifically, it has:
     * 14.42 linker version
     * 6.00 operating system version
     * 6.00 subsystem version
   * `influxdb3` has:
     * 2.40 linker version
     * 4.00 operating system version
     * 5.02 subsystem version

At a high level, the build process for Official builds consists of:
 1. downloading an appropriate build of `python-build-standalone` for the
    target OS and architecture from https://github.com/astral-sh/python-build-standalone/releases
 2. unpacking the `python-build-standalone` build on disk
 3. creating a `pyo3` build configuration file to point to the unpacked
    directory and setting the `PYO3_CONFIG_FILE` environment variable to point
    to it. Eg (on Linux):

    ```
    implementation=CPython
    version=3.11
    shared=true
    abi3=false
    lib_name=python3.11
    lib_dir=/path/to/python-standalone/python/lib
    executable=/path/to/python-standalone/python/bin/python3.11
    pointer_width=64
    build_flags=
    suppress_build_script_link_lines=false
    ```

   PYO3 will try to auto-detect the location which can work well with a system
   python, but not with an unpacked `python-build-standalone`. While the
   `PYO3_PYTHON` environment variable can be used to point to the unpacked
   directory (eg, `PYO3_PYTHON=/path/to/python-standalone/python/bin/python3`),
   this was not sufficient. Defining the build configuration in the
   `PYO3_CONFIG_FILE` correctly worked for all supported environments with our
   current build process
 4. run `PYO3_CONFIG_FILE=/path/to/pyo3_config_file.txt cargo build --features=system-py`
    to build InfluxDB
 5. adjust the library search paths for Linux and Darwin so `libpython` can
    found (see 'Discussion', below)
 6. create official build artifacts:

   * Linux/Darwin `tar.gz` contain `influxdb3` and `python/...`
   * Linux `deb` and `rpm` contain `/usr/bin/influxdb3` and
     `/usr/lib/influxdb3/python`
   * Windows `zip` contains `influxdb3`, `*.dll` files from `python/...` and
     `python/...` (see 'Discussion', below)

Licensing information for `python-build-standalone` as distributed by official
builds of InfluxDB can found in the `python/LICENSE.md`.

With the above, `influxdb3` can be run in the normal way. Eg, on Linux:

```
# unpack tarball to /here
$ tar -C /here --strip-components=1 -zxvf /path/to/build/influxdb3-<VERSION>_linux_amd64.tar.gz

# without processing engine
$ /here/influxdb3 serve ...
$ /here/influxdb3 query ...

# with the processing engine without an activated venv
$ mkdir /path/to/plugins
$ /here/influxdb3 serve --plugin-dir /path/to/plugins ...        # server
$ /here/influxdb3 create database foo                            # client
$ /here/influxdb3 test schedule_plugin -d foo testme.py          # client

# create a venv
$ /here/python/bin/python3 -m venv /path/to/venv
$ source /path/to/venv/bin/activate
(venv)$ pip install requests
...
(venv)$ deactivate

# start server in the venv
$ source /path/to/venv/bin/activate                              # server
(venv)$ /here/influxdb3 serve --plugin-dir /path/to/plugins ...  # server
... <plugins can now 'import requests'> ...

$ /here/influxdb3 test schedule_plugin -d foo test-requests.py   # client
```

## Discussion

### Why python-build-standalone?

`python-build-standalone` is designed to be
[portable](https://astral.sh/blog/python-build-standalone#whats-a-standalone-python-distribution),
[maintained](https://astral.sh/blog/python-build-standalone#the-future-of-standalone-python-distributions)
and [permissively licensed](https://github.com/astral-sh/python-build-standalone/blob/main/docs/running.rst#licensing).
It is purpose-built for embedding and being redistributable and has a good
upstream maintenance story (https://github.com/astral-sh) with lots of users
and a corporate sponsor.

An alternative to using a standalone python distribution is to use the system
python. While this can be a reasonable choice on systems where the python
version and installation locations can be relied upon, it is not a good choice
for official builds since users would have to ensure they had a python
installation that met InfluxDB's requirements and because the myriad of
operating systems, architectures and installed python versions would be a
problem to support.

By choosing `python-build-standalone`, InfluxDB should deliver a consistent
experience across OSes and architectures for all users as well as providing a
reasonable maintenance story.


### Which builds of python-build-standalone are used?

`python-build-standalone` provides [many different builds](https://github.com/astral-sh/python-build-standalone/blob/main/docs/distributions.rst).
Official InfluxDB builds use the following `python-build-standalone`
[recommended](https://github.com/astral-sh/python-build-standalone/blob/main/docs/running.rst)
builds:

 * `aarch64-apple-darwin-install_only_stripped.tar.gz`
 * `aarch64-unknown-linux-gnu-install_only_stripped.tar.gz`
 * `x86_64-unknown-linux-gnu-install_only_stripped.tar.gz`
 * `x86_64-pc-windows-msvc-shared-install_only_stripped.tar.gz`


### How will InfluxData maintain the embedded interpreter?

The https://github.com/astral-sh project performs timely builds of CPython
micro-releases for `python-build-standalone` based on the release cadence of
upstream Python. InfluxData need only update the build to pull in the new
micro-release for security and maintenance releases. This is done by updating
the `PBS_DATE` and `PBS_VERSION` environment variables in
`.circleci/config.yaml`. See that file and
`.circleci/scripts/fetch-python-standalone.bash` for details.

astral-sh creates new builds for CPython minor releases as they become
available from upstream Python. Updating the official builds to pull in a new
minor release is straightforward, but processes for verifying builds of
InfluxDB with the new `python-build-standalone` minor release are TBD.


### How is python-build-standalone licensed?

Release builds of `python-build-standalone` are
[permissively licensed](https://github.com/astral-sh/python-build-standalone/blob/main/docs/running.rst#licensing)
and contain no copyleft code.

The licensing information from release builds of `python-build-standalone` are
obtained by extracting the `python/PYTHON.json` and `python/licenses/*` files
from the `<arch>-debug-full.tar.zst` (Linux/Darwin) and
`<arch>-pgo-full.tar.zst` release tarballs, placing them in the
`python/licenses` directory of the InfluxDB build and generating a
`python/LICENSE.md` file with provenance information.

Linux builds are dynamically linked against [`glibc`](https://www.gnu.org/software/libc/)
(which is permitted by the LGPL without copyleft attachment). InfluxDB does not
statically link against `glibc` nor does it redistribute `libc` (et al) in
official builds.


### Why not just statically link with, eg, MUSL?

In an ideal world, InfluxDB would build against a version of
`python-build-standalone` and statically link against it and not have to worry
about dynamic library compatibility. Unfortunately, this is not possible for
many reasons:

 * static `python-build-standalone` builds for Darwin are [not available](https://github.com/astral-sh/python-build-standalone/blob/main/docs/running.rst)
 * static `python-build-standalone` builds for Windows are [not stable](https://github.com/astral-sh/python-build-standalone/blob/main/docs/running.rst)
 * static `python-build-standalone` builds for Linux/arm64 (aarch64) are [not available](https://github.com/astral-sh/python-build-standalone/blob/main/docs/running.rst)
 * static `python-build-standalone` builds for Linux/amd64 (x86_64) are
  available using MUSL libc, but:
   * because they are static, they [cannot load compiled Python extensions](https://github.com/astral-sh/python-build-standalone/blob/main/docs/running.rst)
     (aka, 'wheels' that have compiled C, Rust, etc code instead of pure python)
     outside of the Python standard library, greatly diminishing the utility of
     the processing engine
   * there are historical [performance issues](https://edu.chainguard.dev/chainguard/chainguard-images/about/images-compiled-programs/glibc-vs-musl/#python-builds) with python and MUSL

It is theoretically possible to statically link `glibc`, but in practice this
is technically very problematic and statically linking `glibc` has copyleft
attachment.


### What about alpine?

Because MUSL can't be used with `python-build-standalone` without crippling the
InfluxDB processing engine, MUSL builds that are compatible with Alpine are not
available at this time. Alpine users can choose one of:

 * build InfluxDB on Alpine against Alpine's python
 * run InfluxDB within a chroot that contains `glibc`
 * run InfluxDB with [gcompat](https://git.adelielinux.org/adelie/gcompat) (untested)

See https://wiki.alpinelinux.org/wiki/Running_glibc_programs for details.

InfluxData may provide Alpine builds at a future date.


### GLIBC portability is a problem. How will you address that?

`glibc` is designed with portability and uses 'compat symbols' to achieve
[backward compatibility](https://developers.redhat.com/blog/2019/08/01/how-the-gnu-c-library-handles-backward-compatibility).
Most 3rd party applications for Linux use the system's `glibc` in some fashion
and this is possible because of 'compat symbols' and this has worked very well
for many, many years.

In essence, 'compat symbols' let `glibc` and the linker choose a particular
implementation of the function. All symbols in `glibc` are versioned and when a
library function changes in an incompatible way, `glibc` keeps the old
implementation in place (with the old symbol version) while adding the new
implementation with a new symbol version. In this manner, if an application is
compiled and linked against `glibc` 2.27, it will only ever lookup symbols that
are 2.27 or earlier. When 2.28 comes out, it updates any symbols it needs to to
2.28, leaving the rest as they are. When the application linked against 2.27
runs on a system with 2.28, everything is ok since 2.28 will resolve all the
2.27 symbols in the expected way the application needs.

Where portability becomes a problem is when the application is linked against a
newer version of `glibc` than is on the system. If the aforementioned
application compiled and linked against 2.27 was run on a system with 2.19, it
would fail to run because the symbol versions it is looking up (ie, anything
from 2.20 and later) are not available.

Unfortunately for developers seeking portability, compiling and linking against
the system's `glibc` means the application will reference the latest available
symbols in that `glibc`. There is no facility for telling the linker to only
use symbols from a particular `glibc` version and earlier. It's also difficult
to tell the linker to use an alternate `glibc` separate from the system's. As a
result, `glibc`-using software seeking wide Linux portability typically needs
to be compiled on an older system with a `glibc` with the desired version.

`python-build-standalone` and `rust` both support systems with `glibc` 2.17+,
which is covers distributions going back to 2014 (CentOS/RHEL 7 (EOL), Debian 8
(Jessie; EOL), Ubuntu 14.04 LTS (EOL), Fedora 21, etc.

Certain InfluxDB alpha releases are compiled against a too new `glibc` (2.36).
This will be addressed before release.


### How does InfluxDB find the correct libpython and the python runtime?

For the best user experience, users should not have to perform any extra setup
to use the InfluxDB processing engine. This is achieved by:

 * Using an appropriate `PYO3_CONFIG_FILE` file during the build (see 'Official
   builds', above)
 * Build artifacts putting the runtime in an expected location (see 'Official
   builds, above)
 * At runtime, ensuring that Linux and Darwin binaries look for the runtime in
   the expected location. Ideally this would be done with linker arguments at
   builds time, but current (alpha) builds adjust the library search paths like
   so:

    ```sh
    # linux
    $ patchelf --add-rpath '$ORIGIN/python/lib' target/.../influxdb3
    $ patchelf --add-rpath '$ORIGIN/../lib/influxdb3/python/lib' target/.../influxdb3

    # osx
    $ install_name_tool -change '/install/lib/libpython3.NN.dylib' \
        '@executable_path/python/lib/libpythonNN.dylib' target/.../influxdb3
    ```

 * The Windows `zip` file for the current (alpha) builds has copies of the
   top-level DLL files from the 'python/' directory alongside `influxdb3`.
   Windows requires that the dynamically linked DLLs needed by the application
   are either in the same directory as the binary or found somewhere in `PATH`
   (and open source tooling doesn't seem to support modifying this). For user
   convenience, the `*.dll` files are shipped alongside the binary on Windows
   to avoid having to setup the `PATH`. This may be addressed in a future
   release


### What limitations are there?

See https://github.com/influxdata/influxdb/issues?q=is%3Aissue%20state%3Aopen%20label%3Av3
