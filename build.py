#!/usr/bin/python2.7 -u

import sys
import os
import subprocess
import time
from datetime import datetime
import shutil
import tempfile
import hashlib
import re
import logging

LOG_LEVEL = logging.INFO

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
go_vet_command = "go tool vet -composites=true ./"
optional_prereqs = [ 'fpm', 'rpmbuild', 'gpg' ]

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
    'influx' : './cmd/influx',
    'influxd' : './cmd/influxd',
    'influx_stress' : './cmd/influx_stress',
    'influx_inspect' : './cmd/influx_inspect',
    'influx_tsm' : './cmd/influx_tsm',
}

supported_builds = {
    'darwin': [ "amd64", "i386" ],
    'windows': [ "amd64", "i386" ],
    'linux': [ "amd64", "i386", "armhf", "arm64", "armel" ]
}

supported_packages = {
    "darwin": [ "tar", "zip" ],
    "linux": [ "deb", "rpm", "tar", "zip"],
    "windows": [ "tar", "zip" ],
}

################
#### InfluxDB Functions
################

def create_package_fs(build_root):
    logging.debug("Creating package filesystem at location: {}".format(build_root))
    # Using [1:] for the path names due to them being absolute
    # (will overwrite previous paths, per 'os.path.join' documentation)
    dirs = [ INSTALL_ROOT_DIR[1:], LOG_DIR[1:], DATA_DIR[1:], SCRIPT_DIR[1:], CONFIG_DIR[1:], LOGROTATE_DIR[1:] ]
    for d in dirs:
        create_dir(os.path.join(build_root, d))
        os.chmod(os.path.join(build_root, d), 0o755)

def package_scripts(build_root, config_only=False):
    if config_only:
        logging.info("Copying configuration to build directory.")
        shutil.copyfile(DEFAULT_CONFIG, os.path.join(build_root, "influxdb.conf"))
        os.chmod(os.path.join(build_root, "influxdb.conf"), 0o644)
    else:
        logging.info("Copying scripts and sample configuration to build directory.")
        shutil.copyfile(INIT_SCRIPT, os.path.join(build_root, SCRIPT_DIR[1:], INIT_SCRIPT.split('/')[1]))
        os.chmod(os.path.join(build_root, SCRIPT_DIR[1:], INIT_SCRIPT.split('/')[1]), 0o644)
        shutil.copyfile(SYSTEMD_SCRIPT, os.path.join(build_root, SCRIPT_DIR[1:], SYSTEMD_SCRIPT.split('/')[1]))
        os.chmod(os.path.join(build_root, SCRIPT_DIR[1:], SYSTEMD_SCRIPT.split('/')[1]), 0o644)
        shutil.copyfile(LOGROTATE_SCRIPT, os.path.join(build_root, LOGROTATE_DIR[1:], "influxdb"))
        os.chmod(os.path.join(build_root, LOGROTATE_DIR[1:], "influxdb"), 0o644)
        shutil.copyfile(DEFAULT_CONFIG, os.path.join(build_root, CONFIG_DIR[1:], "influxdb.conf"))
        os.chmod(os.path.join(build_root, CONFIG_DIR[1:], "influxdb.conf"), 0o644)

def run_generate():
    logging.info("Running 'go generate'...")
    if not check_path_for("statik"):
        run("go install github.com/rakyll/statik")
    orig_path = None
    if os.path.join(os.environ.get("GOPATH"), "bin") not in os.environ["PATH"].split(os.pathsep):
        orig_path = os.environ["PATH"].split(os.pathsep)
        os.environ["PATH"] = os.environ["PATH"].split(os.pathsep).append(os.path.join(os.environ.get("GOPATH"), "bin"))
    run("rm -f ./services/admin/statik/statik.go")
    run("go generate ./services/admin")
    if orig_path is not None:
        os.environ["PATH"] = orig_path
    return True

def go_get(branch, update=False, no_stash=False):
    if not check_path_for("gdm"):
        logging.info("Downloading `gdm`...")
        get_command = "go get github.com/sparrc/gdm"
        run(get_command)
    logging.info("Retrieving dependencies with `gdm`...")
    sys.stdout.flush()
    run("{}/bin/gdm restore -v".format(os.environ.get("GOPATH")))
    return True

################
#### All InfluxDB-specific content above this line
################

def run(command, allow_failure=False, shell=False):
    out = None
    logging.debug("{}".format(command))
    try:
        if shell:
            out = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=shell)
        else:
            out = subprocess.check_output(command.split(), stderr=subprocess.STDOUT)
        out = out.decode('utf-8').strip()
        # logging.debug("Command output: {}".format(out))
    except subprocess.CalledProcessError as e:
        if allow_failure:
            logging.warn("Command '{}' failed with error: {}".format(command, e.output))
            return None
        else:
            logging.error("Command '{}' failed with error: {}".format(command, e.output))
            sys.exit(1)
    except OSError as e:
        if allow_failure:
            logging.warn("Command '{}' failed with error: {}".format(command, e))
            return out
        else:
            logging.error("Command '{}' failed with error: {}".format(command, e))
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

def get_current_version():
    version_tag = get_current_version_tag()
    # Remove leading 'v' and possible '-rc\d+'
    version = re.sub(r'-rc\d+', '', str(version_tag[1:]))
    return version

def get_current_rc():
    rc = None
    version_tag = get_current_version_tag()
    matches = re.match(r'.*-rc(\d+)', str(version_tag))
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
    logging.info("Checking environment...")
    for v in [ "GOPATH", "GOBIN", "GOROOT" ]:
        logging.debug("Using '{}' for {}".format(os.environ.get(v), v))

    cwd = os.getcwd()
    if build_dir is None and os.environ.get("GOPATH") and os.environ.get("GOPATH") not in cwd:
        logging.warn("Your current directory is not under your GOPATH. This may lead to build failures.")
    return True

def check_prereqs():
    logging.info("Checking for dependencies...")
    for req in prereqs:
        if not check_path_for(req):
            logging.error("Could not find dependency: {}".format(req))
            return False
    return True

def upload_packages(packages, bucket_name=None, overwrite=False):
    logging.debug("Uploading files to bucket '{}': {}".format(bucket_name, packages))
    try:
        import boto
        from boto.s3.key import Key
    except ImportError:
        logging.warn("Cannot upload packages without 'boto' Python library! Skipping.")
        return False
    logging.info("Connecting to S3.")
    c = boto.connect_s3()
    if bucket_name is None:
        bucket_name = DEFAULT_BUCKET
    bucket = c.get_bucket(bucket_name.split('/')[0])
    for p in packages:
        if '/' in bucket_name:
            # Allow for nested paths within the bucket name (ex:
            # bucket/folder). Assuming forward-slashes as path
            # delimiter.
            name = os.path.join('/'.join(bucket_name.split('/')[1:]),
                                os.path.basename(p))
        else:
            name = os.path.basename(p)
        if bucket.get_key(name) is None or overwrite:
            logging.info("Uploading file {}".format(name))
            k = Key(bucket)
            k.key = name
            if overwrite:
                n = k.set_contents_from_filename(p, replace=True)
            else:
                n = k.set_contents_from_filename(p, replace=False)
            k.make_public()
        else:
            logging.warn("Not uploading file {}, as it already exists in the target bucket.")
    return True

def run_tests(race, parallel, timeout, no_vet):
    logging.info("Starting tests...")
    if race:
        logging.info("Race is enabled.")
    if parallel is not None:
        logging.info("Using parallel: {}".format(parallel))
    if timeout is not None:
        logging.info("Using timeout: {}".format(timeout))
    out = run("go fmt ./...")
    if len(out) > 0:
        logging.error("Code not formatted. Please use 'go fmt ./...' to fix formatting errors.")
        logging.error("{}".format(out))
        return False
    if not no_vet:
        logging.info("Installing 'go vet' tool...")
        run("go install golang.org/x/tools/cmd/vet")
        out = run(go_vet_command)
        if len(out) > 0:
            logging.error("Go vet failed. Please run 'go vet ./...' and fix any errors.")
            logging.error("{}".format(out))
            return False
    else:
        logging.info("Skipping 'go vet' call...")
    test_command = "go test -v"
    if race:
        test_command += " -race"
    if parallel is not None:
        test_command += " -parallel {}".format(parallel)
    if timeout is not None:
        test_command += " -timeout {}".format(timeout)
    test_command += " ./..."
    logging.info("Running tests...")
    output = run(test_command)
    logging.debug("Test output:\n{}".format(output.encode('ascii', 'ignore')))
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
          tags=[],
          static=False):
    logging.info("Starting build for {}/{}...".format(platform, arch))
    logging.info("Using commit: {}".format(get_current_commit()))
    logging.info("Using branch: {}".format(get_current_branch()))
    if static:
        logging.info("Using statically-compiled output.")
    if race:
        logging.info("Race is enabled.")
    if len(tags) > 0:
        logging.info("Using build tags: {}".format(','.join(tags)))

    logging.info("Sending build output to: {}".format(outdir))
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    elif clean and outdir != '/':
        logging.info("Cleaning build directory.")
        shutil.rmtree(outdir)
        os.makedirs(outdir)

    if rc:
        # If a release candidate, update the version information accordingly
        version = "{}rc{}".format(version, rc)
    logging.info("Using version '{}' for build.".format(version))
    
    tmp_build_dir = create_temp_dir()
    for b, c in targets.items():
        logging.info("Building target: {}".format(b))
        build_command = ""
        if static:
            build_command += "CGO_ENABLED=0 "
        if "arm" in arch:
            build_command += "GOOS={} GOARCH={} ".format(platform, "arm")
        else:
            if arch == 'i386':
                arch = '386'
            elif arch == 'x86_64':
                arch = 'amd64'
            build_command += "GOOS={} GOARCH={} ".format(platform, arch)
        if "arm" in arch:
            if arch == "armel":
                build_command += "GOARM=5 "
            elif arch == "armhf" or arch == "arm":
                build_command += "GOARM=6 "
            elif arch == "arm64":
                build_command += "GOARM=7 "
            else:
                logging.error("Invalid ARM architecture specified: {}".format(arch))
                logging.error("Please specify either 'armel', 'armhf', or 'arm64'.")
                return False
        if platform == 'windows':
            build_command += "go build -o {} ".format(os.path.join(outdir, b + '.exe'))
        else:
            build_command += "go build -o {} ".format(os.path.join(outdir, b))
        if race:
            build_command += "-race "
        if len(tags) > 0:
            build_command += "-tags {} ".format(','.join(tags))
        go_version = get_go_version()
        if "1.4" in go_version:
            if static:
                build_command += "-ldflags=\"-s -X main.version {} -X main.branch {} -X main.commit {}\" ".format(version,
                                                                                                                  get_current_branch(),
                                                                                                                  get_current_commit())
            else:
                build_command += "-ldflags=\"-X main.version {} -X main.branch {} -X main.commit {}\" ".format(version,
                                                                                                               get_current_branch(),
                                                                                                               get_current_commit())

        else:
            # With Go 1.5, the linker flag arguments changed to 'name=value' from 'name value'
            if static:
                build_command += "-ldflags=\"-s -X main.version={} -X main.branch={} -X main.commit={}\" ".format(version,
                                                                                                                  get_current_branch(),
                                                                                                                  get_current_commit())
            else:
                build_command += "-ldflags=\"-X main.version={} -X main.branch={} -X main.commit={}\" ".format(version,
                                                                                                               get_current_branch(),
                                                                                                               get_current_commit())
        if static:
            build_command += "-a -installsuffix cgo "
        build_command += c
        run(build_command, shell=True)
    return True

def create_dir(path):
    os.makedirs(path)

def rename_file(fr, to):
    try:
        os.rename(fr, to)
    except OSError as e:
        # Return the original filename
        return fr
    else:
        # Return the new filename
        return to

def copy_file(fr, to):
    shutil.copy(fr, to)

def generate_md5_from_file(path):
    m = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            m.update(chunk)
    return m.hexdigest()

def generate_sig_from_file(path):
    logging.debug("Generating GPG signature for file: {}".format(path))
    gpg_path = check_path_for('gpg')
    if gpg_path is None:
        logging.warn("gpg binary not found on path! Skipping signature creation.")
        return False
    if os.environ.get("GNUPG_HOME") is not None:
        run('gpg --homedir {} --armor --yes --detach-sign {}'.format(os.environ.get("GNUPG_HOME"), path))
    else:
        run('gpg --armor --detach-sign --yes {}'.format(path))
    return True

def build_packages(build_output, version, nightly=False, rc=None, iteration=1, static=False):
    outfiles = []
    tmp_build_dir = create_temp_dir()
    logging.debug("Packaging for build output: {}".format(build_output))
    logging.debug("Storing temporary build data at location: {}".format(tmp_build_dir))
    try:
        for platform in build_output:
            # Create top-level folder displaying which platform (linux, etc)
            create_dir(os.path.join(tmp_build_dir, platform))
            for arch in build_output[platform]:
                # Create second-level directory displaying the architecture (amd64, etc)
                current_location = build_output[platform][arch]

                # Create directory tree to mimic file system of package
                build_root = os.path.join(tmp_build_dir,
                                          platform,
                                          arch,
                                          '{}-{}-{}'.format(PACKAGE_NAME, version, iteration))
                create_dir(build_root)

                # Copy packaging scripts to build directory
                if platform == 'windows' or static:
                    # For windows and static builds, just copy
                    # binaries to root of package (no other scripts or
                    # directories)
                    package_scripts(build_root, config_only=True)
                else:
                    create_package_fs(build_root)
                    package_scripts(build_root)

                for binary in targets:
                    # Copy newly-built binaries to packaging directory
                    if platform == 'windows':
                        binary = binary + '.exe'
                    if platform == 'windows' or static:
                        # Where the binary should go in the package filesystem
                        to = os.path.join(build_root, binary)
                        # Where the binary currently is located
                        fr = os.path.join(current_location, binary)
                    else:
                        # Where the binary currently is located
                        fr = os.path.join(current_location, binary)
                        # Where the binary should go in the package filesystem
                        to = os.path.join(build_root, INSTALL_ROOT_DIR[1:], binary)
                    copy_file(fr, to)

                for package_type in supported_packages[platform]:
                    # Package the directory structure for each package type for the platform
                    logging.debug("Packaging directory '{}' as '{}'.".format(build_root, package_type))
                    name = PACKAGE_NAME
                    # Reset version, iteration, and current location on each run
                    # since they may be modified below.
                    package_version = version
                    package_iteration = iteration
                    package_build_root = build_root
                    current_location = build_output[platform][arch]

                    if rc is not None:
                        # Set iteration to 0 since it's a release candidate
                        package_iteration = "0.rc{}".format(rc)

                    if package_type in ['zip', 'tar']:
                        # For tars and zips, start the packaging one folder above
                        # the build root (to include the package name)
                        package_build_root = os.path.join('/', '/'.join(build_root.split('/')[:-1]))
                        if nightly:
                            if static:
                                name = '{}-static-nightly_{}_{}'.format(name,
                                                                 platform,
                                                                 arch)
                            else:
                                name = '{}-nightly_{}_{}'.format(name,
                                                                        platform,
                                                                        arch)
                        else:
                            if static:
                                name = '{}-{}-{}-static_{}_{}'.format(name,
                                                                      package_version,
                                                                      package_iteration,
                                                                      platform,
                                                                      arch)
                            else:
                                name = '{}-{}-{}_{}_{}'.format(name,
                                                               package_version,
                                                               package_iteration,
                                                               platform,
                                                               arch)

                        current_location = os.path.join(os.getcwd(), current_location)
                        if package_type == 'tar':
                            tar_command = "cd {} && tar -cvzf {}.tar.gz ./*".format(build_root, name)
                            run(tar_command, shell=True)
                            run("mv {}.tar.gz {}".format(os.path.join(build_root, name), current_location), shell=True)
                            outfile = os.path.join(current_location, name + ".tar.gz")
                            outfiles.append(outfile)
                            print("MD5({}) = {}".format(outfile, generate_md5_from_file(outfile)))
                        elif package_type == 'zip':
                            zip_command = "cd {} && zip -r {}.zip ./*".format(build_root, name)
                            run(zip_command, shell=True)
                            run("mv {}.zip {}".format(os.path.join(build_root, name), current_location), shell=True)
                            outfile = os.path.join(current_location, name + ".zip")
                            outfiles.append(outfile)
                            logging.info("MD5({}) = {}".format(outfile.split(os.pathsep)[-1:],
                                                               generate_md5_from_file(outfile)))
                    elif package_type not in ['zip', 'tar'] and static:
                        logging.info("Skipping package type '{}' for static builds.".format(package_type))
                    else:
                        fpm_command = "fpm {} --name {} -a {} -t {} --version {} --iteration {} -C {} -p {} ".format(
                            fpm_common_args,
                            name,
                            arch,
                            package_type,
                            package_version,
                            package_iteration,
                            package_build_root,
                            current_location)
                        if package_type == "rpm":
                            fpm_command += "--depends coreutils --rpm-posttrans {}".format(POSTINST_SCRIPT)
                        out = run(fpm_command, shell=True)
                        matches = re.search(':path=>"(.*)"', out)
                        outfile = None
                        if matches is not None:
                            outfile = matches.groups()[0]
                        if outfile is None:
                            logging.warn("Could not determine output from packaging output!")
                        else:
                            # Strip nightly version (the unix epoch) from filename
                            if nightly and package_type in [ 'deb', 'rpm' ]:
                                outfile = rename_file(outfile,
                                                      outfile.replace("{}-{}".format(version, iteration), "nightly"))
                            outfiles.append(os.path.join(os.getcwd(), outfile))
                            # Display MD5 hash for generated package
                            logging.info("MD5({}) = {}".format(outfile.split(os.pathsep)[-1:],
                                                               generate_md5_from_file(outfile)))
        logging.debug("Produced package files: {}".format(outfiles))
        return outfiles
    finally:
        # Cleanup
        shutil.rmtree(tmp_build_dir)

def print_usage():
    print("Usage: ./build.py [options]")
    print("")
    print("Options:")
    print("\t --outdir=<path> \n\t\t- Send build output to a specified path. Defaults to ./build.")
    print("\t --arch=<arch> \n\t\t- Build for specified architecture. Acceptable values: x86_64|amd64, 386|i386, arm, or all")
    print("\t --platform=<platform> \n\t\t- Build for specified platform. Acceptable values: linux, windows, darwin, or all")
    print("\t --version=<version> \n\t\t- Version information to apply to build metadata. If not specified, will be pulled from repo tag.")
    print("\t --commit=<commit> \n\t\t- Use specific commit for build (currently a NOOP).")
    print("\t --branch=<branch> \n\t\t- Build from a specific branch (currently a NOOP).")
    print("\t --rc=<rc number> \n\t\t- Whether or not the build is a release candidate (affects version information).")
    print("\t --iteration=<iteration number> \n\t\t- The iteration to display on the package output (defaults to 0 for RC's, and 1 otherwise).")
    print("\t --race \n\t\t- Whether the produced build should have race detection enabled.")
    print("\t --package \n\t\t- Whether the produced builds should be packaged for the target platform(s).")
    print("\t --nightly \n\t\t- Whether the produced build is a nightly (affects version information).")
    print("\t --update \n\t\t- Whether dependencies should be updated prior to building.")
    print("\t --test \n\t\t- Run Go tests. Will not produce a build.")
    print("\t --parallel \n\t\t- Run Go tests in parallel up to the count specified.")
    print("\t --generate \n\t\t- Run `go generate`.")
    print("\t --timeout \n\t\t- Timeout for Go tests. Defaults to 480s.")
    print("\t --clean \n\t\t- Clean the build output directory prior to creating build.")
    print("\t --no-get \n\t\t- Do not run `go get` before building.")
    print("\t --static \n\t\t- Generate statically-linked binaries.")
    print("\t --bucket=<S3 bucket>\n\t\t- Full path of the bucket to upload packages to (must also specify --upload).")
    print("\t --sign \n\t\t- Sign output packages using GPG.")
    print("\t --debug \n\t\t- Use debug output.")
    print("")

def main():
    global PACKAGE_NAME

    # Command-line arguments
    outdir = "build"
    commit = None
    target_platform = None
    target_arch = None
    nightly = False
    race = False
    branch = None
    version = get_current_version()
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
    run_get = True
    upload_bucket = None
    generate = False
    no_stash = False
    static = False
    build_tags = []
    sign_packages = False
    upload_overwrite = False

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
        elif '--rc' in arg:
            # Signifies that this is a release candidate build.
            rc = arg.split("=")[1]
        elif '--race' in arg:
            # Signifies that race detection should be enabled.
            race = True
        elif '--package' in arg:
            # Signifies that packages should be built.
            package = True
            # If packaging do not allow stashing of local changes
            no_stash = True
        elif '--nightly' in arg:
            # Signifies that this is a nightly build.
            nightly = True
        elif '--update' in arg:
            # Signifies that dependencies should be updated.
            update = True
        elif '--upload' in arg:
            # Signifies that the resulting packages should be uploaded to S3
            upload = True
        elif '--overwrite' in arg:
            # Signifies that the resulting packages should be uploaded to S3
            upload_overwrite = True
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
        elif '--bucket' in arg:
            # The bucket to upload the packages to, relies on boto
            upload_bucket = arg.split("=")[1]
        elif '--no-stash' in arg:
            # Do not stash uncommited changes
            # Fail if uncommited changes exist
            no_stash = True
        elif '--generate' in arg:
            generate = True
        elif '--build-tags' in arg:
            for t in arg.split("=")[1].split(","):
                build_tags.append(t)
        elif '--name' in arg:
            # Change the output package name
            PACKAGE_NAME = arg.split("=")[1]
        elif '--static' in arg:
            static = True
        elif '--sign' in arg:
            sign_packages = True
        elif '--debug' in arg:
            # Setting log level is handled elsewhere
            pass
        elif '--help' in arg:
            print_usage()
            return 0
        else:
            print("!! Unknown argument: {}".format(arg))
            print_usage()
            return 1

    if nightly and rc:
        logging.error("Cannot be both a nightly and a release candidate.")
        return 1

    if nightly:
        # In order to cleanly delineate nightly version, we are adding the epoch timestamp
        # to the version so that version numbers are always greater than the previous nightly.
        version = "{}~n{}".format(version,
                                  datetime.utcnow().strftime("%Y%m%d%H%M"))
        iteration = 0
    elif rc:
        iteration = 0

    # Pre-build checks
    check_environ()
    if not check_prereqs():
        return 1

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
            if target_arch == '386':
                target_arch = 'i386'
            elif target_arch == 'x86_64':
                target_arch = 'amd64'
    if target_platform:
        if target_platform not in supported_builds and target_platform != 'all':
            logging.error("Invalid build platform: {}".format(target_platform))
            return 1
    else:
        target_platform = get_system_platform()

    build_output = {}

    if run_get:
        if not go_get(branch, update=update, no_stash=no_stash):
            return 1

    if generate:
        if not run_generate():
            return 1

    if test:
        if not run_tests(race, parallel, timeout, no_vet):
            return 1
        return 0

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
            if not build(version=version,
                         branch=branch,
                         commit=commit,
                         platform=platform,
                         arch=arch,
                         nightly=nightly,
                         rc=rc,
                         race=race,
                         clean=clean,
                         outdir=od,
                         tags=build_tags,
                         static=static):
                return 1
            build_output.get(platform).update( { arch : od } )

    # Build packages
    if package:
        if not check_path_for("fpm"):
            logging.error("FPM ruby gem required for packaging. Stopping.")
            return 1
        packages = build_packages(build_output,
                                  version,
                                  nightly=nightly,
                                  rc=rc,
                                  iteration=iteration,
                                  static=static)
        if sign_packages:
            logging.debug("Generating GPG signatures for packages: {}".format(packages))
            sigs = [] # retain signatures so they can be uploaded with packages
            for p in packages:
                if generate_sig_from_file(p):
                    sigs.append(p + '.asc')
                else:
                    logging.error("Creation of signature for package [{}] failed!".format(p))
                    return 1
            packages += sigs
        if upload:
            logging.debug("Files staged for upload: {}".format(packages))
            if nightly or upload_overwrite:
                upload_packages(packages, bucket_name=upload_bucket, overwrite=True)
            else:
                upload_packages(packages, bucket_name=upload_bucket, overwrite=False)
    return 0

if __name__ == '__main__':
    if '--debug' in sys.argv[1:]:
        LOG_LEVEL = logging.DEBUG
    log_format = '[%(levelname)s] %(funcName)s: %(message)s'
    logging.basicConfig(level=LOG_LEVEL,
                        format=log_format)
    sys.exit(main())
