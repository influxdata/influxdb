#!/usr/bin/env python2.7
#
# This is the InfluxDB build script.
#
# Current caveats:
#   - Does not checkout the correct commit/branch (for now, you will need to do so manually)
#   - Has external dependencies for packaging (fpm) and uploading (boto)
#

import sys
import os
import subprocess
import time
import datetime
import shutil
import tempfile
import hashlib
import re

debug = False

################
#### InfluxDB Variables
################

# Packaging variables
PACKAGE_NAME = "influxdb"
INSTALL_ROOT_DIR = "/usr/bin"
LOG_DIR = "/var/log/influxdb"
DATA_DIR = "/var/lib/influxdb"
SCRIPT_DIR = "/usr/lib/influxdb/scripts"
CONFIG_DIR = "/etc/influxdb"
LOGROTATE_DIR = "/etc/logrotate.d"

INIT_SCRIPT = "scripts/init.sh"
SYSTEMD_SCRIPT = "scripts/influxdb.service"
PREINST_SCRIPT = "scripts/pre-install.sh"
POSTINST_SCRIPT = "scripts/post-install.sh"
POSTUNINST_SCRIPT = "scripts/post-uninstall.sh"
LOGROTATE_SCRIPT = "scripts/logrotate"
DEFAULT_CONFIG = "etc/config.sample.toml"

# Default AWS S3 bucket for uploads
DEFAULT_BUCKET = "influxdb"

CONFIGURATION_FILES = [
    CONFIG_DIR + '/influxdb.conf',
    LOGROTATE_DIR + '/influxdb',
]

PACKAGE_LICENSE = "MIT"
PACKAGE_URL = "https://github.com/influxdata/influxdb"
MAINTAINER = "support@influxdb.com"
VENDOR = "InfluxData"
DESCRIPTION = "Distributed time-series database."

prereqs = [ 'git', 'go' ]
optional_prereqs = [ 'gvm', 'fpm', 'rpmbuild' ]

fpm_common_args = "-f -s dir --log error \
--vendor {} \
--url {} \
--after-install {} \
--before-install {} \
--after-remove {} \
--license {} \
--maintainer {} \
--directories {} \
--directories {} \
--description \"{}\"".format(
     VENDOR,
     PACKAGE_URL,
     POSTINST_SCRIPT,
     PREINST_SCRIPT,
     POSTUNINST_SCRIPT,
     PACKAGE_LICENSE,
     MAINTAINER,
     LOG_DIR,
     DATA_DIR,
     DESCRIPTION)

for f in CONFIGURATION_FILES:
    fpm_common_args += " --config-files {}".format(f)

targets = {
    'influx' : './cmd/influx/main.go',
    'influxd' : './cmd/influxd/main.go',
    'influx_stress' : './cmd/influx_stress/influx_stress.go',
    'influx_inspect' : './cmd/influx_inspect/*.go',
    'influx_tsm' : './cmd/influx_tsm/*.go',
}

supported_builds = {
    'darwin': [ "amd64", "i386" ],
    # InfluxDB does not currently support Windows
    # 'windows': [ "amd64", "386", "arm" ],
    'linux': [ "amd64", "i386", "arm" ]
}

supported_packages = {
    "darwin": [ "tar" ],
    "linux": [ "deb", "rpm", "tar" ],
    "windows": [ "tar" ],
}

################
#### InfluxDB Functions
################

def create_package_fs(build_root):
    print "Creating package filesystem at root: {}".format(build_root)
    # Using [1:] for the path names due to them being absolute
    # (will overwrite previous paths, per 'os.path.join' documentation)
    dirs = [ INSTALL_ROOT_DIR[1:], LOG_DIR[1:], DATA_DIR[1:], SCRIPT_DIR[1:], CONFIG_DIR[1:], LOGROTATE_DIR[1:] ]
    for d in dirs:
        create_dir(os.path.join(build_root, d))
        os.chmod(os.path.join(build_root, d), 0755)

def package_scripts(build_root):
    print "Copying scripts and sample configuration to build directory"
    shutil.copyfile(INIT_SCRIPT, os.path.join(build_root, SCRIPT_DIR[1:], INIT_SCRIPT.split('/')[1]))
    os.chmod(os.path.join(build_root, SCRIPT_DIR[1:], INIT_SCRIPT.split('/')[1]), 0644)
    shutil.copyfile(SYSTEMD_SCRIPT, os.path.join(build_root, SCRIPT_DIR[1:], SYSTEMD_SCRIPT.split('/')[1]))
    os.chmod(os.path.join(build_root, SCRIPT_DIR[1:], SYSTEMD_SCRIPT.split('/')[1]), 0644)
    shutil.copyfile(LOGROTATE_SCRIPT, os.path.join(build_root, LOGROTATE_DIR[1:], "influxdb"))
    os.chmod(os.path.join(build_root, LOGROTATE_DIR[1:], "influxdb"), 0644)
    shutil.copyfile(DEFAULT_CONFIG, os.path.join(build_root, CONFIG_DIR[1:], "influxdb.conf"))
    os.chmod(os.path.join(build_root, CONFIG_DIR[1:], "influxdb.conf"), 0644)

def run_generate():
    # TODO - Port this functionality to InfluxDB, currently a NOOP
    print "NOTE: The `--generate` flag is currently a NNOP. Skipping..."
    # print "Running generate..."
    # command = "go generate ./..."
    # code = os.system(command)
    # if code != 0:
    #     print "Generate Failed"
    #     return False
    # else:
    #     print "Generate Succeeded"
    # return True
    pass
    
################
#### All InfluxDB-specific content above this line
################

def run(command, allow_failure=False, shell=False):
    out = None
    if debug:
        print "[DEBUG] {}".format(command)
    try:
        if shell:
            out = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=shell)
        else:
            out = subprocess.check_output(command.split(), stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print ""
        print ""
        print "Executed command failed!"
        print "-- Command run was: {}".format(command)
        print "-- Failure was: {}".format(e.output)
        if allow_failure:
            print "Continuing..."
            return None
        else:
            print ""
            print "Stopping."
            sys.exit(1)
    except OSError as e:
        print ""
        print ""
        print "Invalid command!"
        print "-- Command run was: {}".format(command)
        print "-- Failure was: {}".format(e)
        if allow_failure:
            print "Continuing..."
            return out
        else:
            print ""
            print "Stopping."
            sys.exit(1)
    else:
        return out

def create_temp_dir(prefix = None):
    if prefix is None:
        return tempfile.mkdtemp(prefix="{}-build.".format(PACKAGE_NAME))
    else:
        return tempfile.mkdtemp(prefix=prefix)

def get_current_version_tag():
    version = run("git describe --always --tags --abbrev=0").strip()
    return version

def get_current_rc():
    rc = None
    version_tag = get_current_version_tag()
    matches = re.match(r'.*-rc(\d+)', version_tag)
    if matches:
        rc, = matches.groups(1)
    return rc

def get_current_commit(short=False):
    command = None
    if short:
        command = "git log --pretty=format:'%h' -n 1"
    else:
        command = "git rev-parse HEAD"
    out = run(command)
    return out.strip('\'\n\r ')

def get_current_branch():
    command = "git rev-parse --abbrev-ref HEAD"
    out = run(command)
    return out.strip()

def get_system_arch():
    arch = os.uname()[4]
    if arch == "x86_64":
        arch = "amd64"
    return arch

def get_system_platform():
    if sys.platform.startswith("linux"):
        return "linux"
    else:
        return sys.platform

def get_go_version():
    out = run("go version")
    matches = re.search('go version go(\S+)', out)
    if matches is not None:
        return matches.groups()[0].strip()
    return None

def check_path_for(b):
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    for path in os.environ["PATH"].split(os.pathsep):
        path = path.strip('"')
        full_path = os.path.join(path, b)
        if os.path.isfile(full_path) and os.access(full_path, os.X_OK):
            return full_path

def check_environ(build_dir = None):
    print ""
    print "Checking environment:"
    for v in [ "GOPATH", "GOBIN", "GOROOT" ]:
        print "- {} -> {}".format(v, os.environ.get(v))

    cwd = os.getcwd()
    if build_dir is None and os.environ.get("GOPATH") and os.environ.get("GOPATH") not in cwd:
        print "!! WARNING: Your current directory is not under your GOPATH. This may lead to build failures."

def check_prereqs():
    print ""
    print "Checking for dependencies:"
    for req in prereqs:
        print "- {} ->".format(req),
        path = check_path_for(req)
        if path:
            print "{}".format(path)
        else:
            print "?"
    for req in optional_prereqs:
        print "- {} (optional) ->".format(req),
        path = check_path_for(req)
        if path:
            print "{}".format(path)
        else:
            print "?"
    print ""

def upload_packages(packages, bucket_name=None, nightly=False):
    if debug:
        print "[DEBUG] upload_packages: {}".format(packages)
    try:
        import boto
        from boto.s3.key import Key
    except ImportError:
        print "!! Cannot upload packages without the 'boto' Python library."
        return 1
    print "Connecting to S3...".format(bucket_name)
    c = boto.connect_s3()
    if bucket_name is None:
        bucket_name = DEFAULT_BUCKET
    bucket = c.get_bucket(bucket_name.split('/')[0])
    print "Using bucket: {}".format(bucket_name)
    for p in packages:
        if '/' in bucket_name:
            # Allow for nested paths within the bucket name (ex:
            # bucket/folder). Assuming forward-slashes as path
            # delimiter.
            name = os.path.join('/'.join(bucket_name.split('/')[1:]),
                                os.path.basename(p))
        else:
            name = os.path.basename(p)
        if bucket.get_key(name) is None or nightly:
            print "Uploading {}...".format(name)
            sys.stdout.flush()
            k = Key(bucket)
            k.key = name
            if nightly:
                n = k.set_contents_from_filename(p, replace=True)
            else:
                n = k.set_contents_from_filename(p, replace=False)
            k.make_public()
        else:
            print "!! Not uploading package {}, as it already exists.".format(p)
    print ""
    return 0

def run_tests(race, parallel, timeout, no_vet):
    print "Retrieving Go dependencies...",
    get_command = "go get -d -t ./..."
    sys.stdout.flush()
    run(get_command)
    get_command = "go get golang.org/x/tools/cmd/vet"
    sys.stdout.flush()
    run(get_command)
    print "done."
    print "Running tests:"
    print "\tRace: ", race
    if parallel is not None:
        print "\tParallel:", parallel
    if timeout is not None:
        print "\tTimeout:", timeout
    sys.stdout.flush()
    p = subprocess.Popen(["go", "fmt", "./..."], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    if len(out) > 0 or len(err) > 0:
        print "Code not formatted. Please use 'go fmt ./...' to fix formatting errors."
        print out
        print err
        return False
    if not no_vet:
        p = subprocess.Popen(["go", "tool", "vet", "-composites=true", "./"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if len(out) > 0 or len(err) > 0:
            print "Go vet failed. Please run 'go vet ./...' and fix any errors."
            print out
            print err
            return False
    else:
        print "Skipping go vet ..."
        sys.stdout.flush()
    test_command = "go test -v"
    if race:
        test_command += " -race"
    if parallel is not None:
        test_command += " -parallel {}".format(parallel)
    if timeout is not None:
        test_command += " -timeout {}".format(timeout)
    test_command += " ./..."
    code = os.system(test_command)
    if code != 0:
        print "Tests Failed"
        return False
    else:
        print "Tests Passed"
        return True

def build(version=None,
          branch=None,
          commit=None,
          platform=None,
          arch=None,
          nightly=False,
          rc=None,
          race=False,
          clean=False,
          outdir=".",
          goarm_version="6"):
    print ""
    print "-------------------------"
    print ""
    print "Build Plan:"
    print "- version: {}".format(version)
    if rc:
        print "- release candidate: {}".format(rc)
    print "- commit: {}".format(get_current_commit(short=True))
    print "- branch: {}".format(get_current_branch())
    print "- platform: {}".format(platform)
    print "- arch: {}".format(arch)
    if arch == 'arm' and goarm_version:
        print "- ARM version: {}".format(goarm_version)
    print "- nightly? {}".format(str(nightly).lower())
    print "- race enabled? {}".format(str(race).lower())
    print ""

    if not os.path.exists(outdir):
        os.makedirs(outdir)
    elif clean and outdir != '/':
        print "Cleaning build directory..."
        shutil.rmtree(outdir)
        os.makedirs(outdir)

    if rc:
        # If a release candidate, update the version information accordingly
        version = "{}rc{}".format(version, rc)

    # Set the architecture to something that Go expects
    if arch == 'i386':
        arch = '386'
    elif arch == 'x86_64':
        arch = 'amd64'
    
    print "Starting build..."
    tmp_build_dir = create_temp_dir()
    for b, c in targets.iteritems():
        print "Building '{}'...".format(os.path.join(outdir, b))

        build_command = ""
        build_command += "GOOS={} GOARCH={} ".format(platform, arch)
        if arch == "arm" and goarm_version:
            if goarm_version not in ["5", "6", "7", "arm64"]:
                print "!! Invalid ARM build version: {}".format(goarm_version)
            build_command += "GOARM={} ".format(goarm_version)
        build_command += "go build -o {} ".format(os.path.join(outdir, b))
        if race:
            build_command += "-race "
        go_version = get_go_version()
        if "1.4" in go_version:
            build_command += "-ldflags=\"-X main.buildTime '{}' ".format(datetime.datetime.utcnow().isoformat())
            build_command += "-X main.version {} ".format(version)
            build_command += "-X main.branch {} ".format(get_current_branch())
            build_command += "-X main.commit {}\" ".format(get_current_commit())
        else:
            build_command += "-ldflags=\"-X main.buildTime='{}' ".format(datetime.datetime.utcnow().isoformat())
            build_command += "-X main.version={} ".format(version)
            build_command += "-X main.branch={} ".format(get_current_branch())
            build_command += "-X main.commit={}\" ".format(get_current_commit())
        build_command += c
        run(build_command, shell=True)
    print ""

def create_dir(path):
    try:
        os.makedirs(path)
    except OSError as e:
        print e

def rename_file(fr, to):
    try:
        os.rename(fr, to)
    except OSError as e:
        print e
        # Return the original filename
        return fr
    else:
        # Return the new filename
        return to

def copy_file(fr, to):
    try:
        shutil.copy(fr, to)
    except OSError as e:
        print e

def go_get(branch, update=False):
    get_command = None
    if update:
        get_command = "go get -u -f -d ./..."
    else:
        get_command = "go get -d ./..."

    # 'go get' switches to master, so stash what we currently have
    stash = run("git stash create -a").strip()
    if len(stash) > 0:
        print "There are un-committed changes in your local branch, stashing them as {}".format(stash)
        # reset to ensure we don't have any checkout issues
        run("git reset --hard")

        print "Retrieving Go dependencies (moving to master)..."
        run(get_command)
        sys.stdout.flush()

        print "Moving back to branch '{}'...".format(branch)
        run("git checkout {}".format(branch))
        
        print "Applying previously stashed contents..."
        run("git stash apply {}".format(stash))
    else:
        print "Retrieving Go dependencies..."
        run(get_command)

        print "Moving back to branch '{}'...".format(branch)
        run("git checkout {}".format(branch))
        
def generate_md5_from_file(path):
    m = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            m.update(chunk)
    return m.hexdigest()

def build_packages(build_output, version, pkg_arch, nightly=False, rc=None, iteration=1):
    outfiles = []
    tmp_build_dir = create_temp_dir()
    if debug:
        print "[DEBUG] build_output = {}".format(build_output)
    try:
        print "-------------------------"
        print ""
        print "Packaging..."
        for p in build_output:
            # Create top-level folder displaying which platform (linux, etc)
            create_dir(os.path.join(tmp_build_dir, p))
            for a in build_output[p]:
                current_location = build_output[p][a]
                # Create second-level directory displaying the architecture (amd64, etc)
                build_root = os.path.join(tmp_build_dir, p, a, 'influxdb-{}-{}'.format(version, iteration))
                # Create directory tree to mimic file system of package
                create_dir(build_root)
                create_package_fs(build_root)
                # Copy in packaging and miscellaneous scripts
                package_scripts(build_root)
                # Copy newly-built binaries to packaging directory
                for b in targets:
                    if p == 'windows':
                        b = b + '.exe'
                    fr = os.path.join(current_location, b)
                    to = os.path.join(build_root, INSTALL_ROOT_DIR[1:], b)
                    if debug:
                        print "[{}][{}] - Moving from '{}' to '{}'".format(p, a, fr, to)
                    copy_file(fr, to)
                # Package the directory structure
                for package_type in supported_packages[p]:
                    print "Packaging directory '{}' as '{}'...".format(build_root, package_type)
                    name = PACKAGE_NAME
                    # Reset version, iteration, and current location on each run
                    # since they may be modified below.
                    package_version = version
                    package_iteration = iteration
                    package_build_root = build_root
                    current_location = build_output[p][a]

                    if package_type in ['zip', 'tar']:
                        package_build_root = os.path.join('/', '/'.join(build_root.split('/')[:-1]))
                        if nightly:
                            name = '{}-nightly_{}_{}'.format(name, p, a)
                        else:
                            name = '{}-{}-{}_{}_{}'.format(name, package_version, package_iteration, p, a)
                    
                    if package_type == 'tar':
                        # Add `tar.gz` to path to ensure a small package size
                        current_location = os.path.join(current_location, name + '.tar.gz')
                    elif package_type == 'zip':
                        current_location = os.path.join(current_location, name + '.zip')
                    
                    if rc is not None:
                        package_iteration = "0.rc{}".format(rc)
                    saved_a = a
                    if pkg_arch is not None:
                        a = pkg_arch
                    if a == '386':
                        a = 'i386'
                    
                    fpm_command = "fpm {} --name {} -a {} -t {} --version {} --iteration {} -C {} -p {} ".format(
                        fpm_common_args,
                        name,
                        a,
                        package_type,
                        package_version,
                        package_iteration,
                        package_build_root,
                        current_location)
                    if debug:
                        fpm_command += "--verbose "
                    if pkg_arch is not None:
                        a = saved_a
                    if package_type == "rpm":
                        fpm_command += "--depends coreutils --rpm-posttrans {}".format(POSTINST_SCRIPT)
                    out = run(fpm_command, shell=True)
                    matches = re.search(':path=>"(.*)"', out)
                    outfile = None
                    if matches is not None:
                        outfile = matches.groups()[0]
                    if outfile is None:
                        print "!! Could not determine output from packaging command."
                    else:
                        # Strip nightly version (the unix epoch) from filename
                        if nightly and package_type in ['deb', 'rpm']:
                            outfile = rename_file(outfile, outfile.replace("{}-{}".format(version, iteration), "nightly"))
                        outfiles.append(os.path.join(os.getcwd(), outfile))
                        # Display MD5 hash for generated package
                        print "MD5({}) = {}".format(outfile, generate_md5_from_file(outfile))
        print ""
        if debug:
            print "[DEBUG] package outfiles: {}".format(outfiles)
        return outfiles
    finally:
        # Cleanup
        shutil.rmtree(tmp_build_dir)

def print_usage():
    print "Usage: ./build.py [options]"
    print ""
    print "Options:"
    print "\t --outdir=<path> \n\t\t- Send build output to a specified path. Defaults to ./build."
    print "\t --arch=<arch> \n\t\t- Build for specified architecture. Acceptable values: x86_64|amd64, 386|i386, arm, or all"
    print "\t --goarm=<arm version> \n\t\t- Build for specified ARM version (when building for ARM). Default value is: 6"
    print "\t --platform=<platform> \n\t\t- Build for specified platform. Acceptable values: linux, windows, darwin, or all"
    print "\t --version=<version> \n\t\t- Version information to apply to build metadata. If not specified, will be pulled from repo tag."
    print "\t --pkgarch=<package-arch> \n\t\t- Package architecture if different from <arch>"
    print "\t --commit=<commit> \n\t\t- Use specific commit for build (currently a NOOP)."
    print "\t --branch=<branch> \n\t\t- Build from a specific branch (currently a NOOP)."
    print "\t --rc=<rc number> \n\t\t- Whether or not the build is a release candidate (affects version information)."
    print "\t --iteration=<iteration number> \n\t\t- The iteration to display on the package output (defaults to 0 for RC's, and 1 otherwise)."
    print "\t --race \n\t\t- Whether the produced build should have race detection enabled."
    print "\t --package \n\t\t- Whether the produced builds should be packaged for the target platform(s)."
    print "\t --nightly \n\t\t- Whether the produced build is a nightly (affects version information)."
    print "\t --update \n\t\t- Whether dependencies should be updated prior to building."
    print "\t --test \n\t\t- Run Go tests. Will not produce a build."
    print "\t --parallel \n\t\t- Run Go tests in parallel up to the count specified."
    print "\t --generate \n\t\t- Run `go generate` (currently a NOOP)."
    print "\t --timeout \n\t\t- Timeout for Go tests. Defaults to 480s."
    print "\t --clean \n\t\t- Clean the build output directory prior to creating build."
    print "\t --no-get \n\t\t- Do not run `go get` before building."
    print "\t --bucket=<S3 bucket>\n\t\t- Full path of the bucket to upload packages to (must also specify --upload)."
    print "\t --debug \n\t\t- Displays debug output."
    print ""

def print_package_summary(packages):
    print packages

def main():
    global debug
    
    # Command-line arguments
    outdir = "build"
    commit = None
    target_platform = None
    target_arch = None
    package_arch = None
    nightly = False
    race = False
    branch = None
    version = get_current_version_tag()
    rc = get_current_rc()
    package = False
    update = False
    clean = False
    upload = False
    test = False
    parallel = None
    timeout = None
    iteration = 1
    no_vet = False
    goarm_version = "6"
    run_get = True
    upload_bucket = None
    generate = False
    
    for arg in sys.argv[1:]:
        if '--outdir' in arg:
            # Output directory. If none is specified, then builds will be placed in the same directory.
            outdir = arg.split("=")[1]
        if '--commit' in arg:
            # Commit to build from. If none is specified, then it will build from the most recent commit.
            commit = arg.split("=")[1]
        if '--branch' in arg:
            # Branch to build from. If none is specified, then it will build from the current branch.
            branch = arg.split("=")[1]
        elif '--arch' in arg:
            # Target architecture. If none is specified, then it will build for the current arch.
            target_arch = arg.split("=")[1]
        elif '--platform' in arg:
            # Target platform. If none is specified, then it will build for the current platform.
            target_platform = arg.split("=")[1]
        elif '--version' in arg:
            # Version to assign to this build (0.9.5, etc)
            version = arg.split("=")[1]
        elif '--pkgarch' in arg:
            # Package architecture if different from <arch> (armhf, etc)
            package_arch = arg.split("=")[1]
        elif '--rc' in arg:
            # Signifies that this is a release candidate build.
            rc = arg.split("=")[1]
        elif '--race' in arg:
            # Signifies that race detection should be enabled.
            race = True
        elif '--package' in arg:
            # Signifies that packages should be built.
            package = True
        elif '--nightly' in arg:
            # Signifies that this is a nightly build.
            nightly = True
            # In order to cleanly delineate nightly version, we are adding the epoch timestamp
            # to the version so that version numbers are always greater than the previous nightly.
            version = "{}.n{}".format(version, int(time.time()))
        elif '--update' in arg:
            # Signifies that dependencies should be updated.
            update = True
        elif '--upload' in arg:
            # Signifies that the resulting packages should be uploaded to S3
            upload = True
        elif '--test' in arg:
            # Run tests and exit
            test = True
        elif '--parallel' in arg:
            # Set parallel for tests.
            parallel = int(arg.split("=")[1])
        elif '--timeout' in arg:
            # Set timeout for tests.
            timeout = arg.split("=")[1]
        elif '--clean' in arg:
            # Signifies that the outdir should be deleted before building
            clean = True
        elif '--iteration' in arg:
            iteration = arg.split("=")[1]
        elif '--no-vet' in arg:
            no_vet = True
        elif '--no-get' in arg:
            run_get = False
        elif '--goarm' in arg:
            # Signifies GOARM flag to pass to build command when compiling for ARM
            goarm_version = arg.split("=")[1]
        elif '--bucket' in arg:
            # The bucket to upload the packages to, relies on boto
            upload_bucket = arg.split("=")[1]
        elif '--generate' in arg:
            # Run go generate ./...
            # TODO - this currently does nothing for InfluxDB
            generate = True
        elif '--debug' in arg:
            print "[DEBUG] Using debug output"
            debug = True
        elif '--help' in arg:
            print_usage()
            return 0
        else:
            print "!! Unknown argument: {}".format(arg)
            print_usage()
            return 1

    if nightly and rc:
        print "!! Cannot be both nightly and a release candidate! Stopping."
        return 1

    # Pre-build checks
    check_environ()
    check_prereqs()
    
    if not commit:
        commit = get_current_commit(short=True)
    if not branch:
        branch = get_current_branch()
    if not target_arch:
        system_arch = get_system_arch()
        if 'arm' in system_arch:
            # Prevent uname from reporting ARM arch (eg 'armv7l')
            target_arch = "arm"
        else:
            target_arch = system_arch
    if not target_platform:
        target_platform = get_system_platform()
    if rc or nightly:
        # If a release candidate or nightly, set iteration to 0 (instead of 1)
        iteration = 0

    if target_arch == '386':
        target_arch = 'i386'
    elif target_arch == 'x86_64':
        target_arch = 'amd64'
    
    build_output = {}

    if generate:
        if not run_generate():
            return 1
    
    if test:
        if not run_tests(race, parallel, timeout, no_vet):
            return 1
        return 0

    if run_get:
        go_get(branch, update=update)

    platforms = []
    single_build = True
    if target_platform == 'all':
        platforms = supported_builds.keys()
        single_build = False
    else:
        platforms = [target_platform]

    for platform in platforms:
        build_output.update( { platform : {} } )
        archs = []
        if target_arch == "all":
            single_build = False
            archs = supported_builds.get(platform)
        else:
            archs = [target_arch]
        for arch in archs:
            od = outdir
            if not single_build:
                od = os.path.join(outdir, platform, arch)
            build(version=version,
                  branch=branch,
                  commit=commit,
                  platform=platform,
                  arch=arch,
                  nightly=nightly,
                  rc=rc,
                  race=race,
                  clean=clean,
                  outdir=od,
                  goarm_version=goarm_version)
            build_output.get(platform).update( { arch : od } )

    # Build packages
    if package:
        if not check_path_for("fpm"):
            print "!! Cannot package without command 'fpm'."
            return 1

        packages = build_packages(build_output, version, package_arch, nightly=nightly, rc=rc, iteration=iteration)
        if upload:
            upload_packages(packages, bucket_name=upload_bucket, nightly=nightly)
    print "Done!"
    return 0

if __name__ == '__main__':
    sys.exit(main())

