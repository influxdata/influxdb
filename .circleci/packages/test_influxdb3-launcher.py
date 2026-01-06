#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit tests for influxdb3-launcher

Usage:
    $ python3 test_influxdb3-launcher.py /path/to/influxdb3-launcher

Development (should be black and pyright clean):
    $ python3 -m venv ./.venv
    $ source ./.venv/bin/activate
    $ pip install black pyright
"""

import sys

sys.dont_write_bytecode = True  # don't create __pycache__ files

import argparse
import importlib.machinery
import importlib.util
import io
import os
import signal
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import unittest


# Global to store the influxdb3-launcher script path
LAUNCHER_PATH: str | None = None


def load_launcher_module():
    """
    Load the influxdb3-launcher script as a Python module.
    Returns the loaded module.
    """
    launcher_path = (
        Path(__file__).parent / "influxdb3/fs/usr/lib/influxdb3/influxdb3-launcher"
    )

    # Force Python to treat this as a Python file even without .py extension
    spec = importlib.util.spec_from_file_location(
        "launcher",
        launcher_path,
        loader=importlib.machinery.SourceFileLoader("launcher", str(launcher_path)),
    )

    if spec is None or spec.loader is None:  # pragma: nocover
        raise ImportError(f"Could not load launcher from {launcher_path}")

    module = importlib.util.module_from_spec(spec)

    # Add module to sys.modules before executing to handle imports
    sys.modules["launcher"] = module

    spec.loader.exec_module(module)
    return module


class TestCheckExecutable(unittest.TestCase):
    """Tests for check_executable() function"""

    def setUp(self):
        self.launcher = load_launcher_module()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_mock_executable(self, name="mock_exec"):
        """Helper to create a mock executable script"""
        # Create the Python script
        script_path = os.path.join(self.temp_dir, f"{name}.py")
        with open(script_path, "w") as f:
            f.write("import sys\n")
            f.write("print('mock executable')\n")

        # Create platform-appropriate executable wrapper
        if os.name == "nt":  # Windows
            exec_path = os.path.join(self.temp_dir, f"{name}.bat")  # type: ignore[unreachable]
            with open(exec_path, "w") as f:
                f.write(f'@echo off\n"{sys.executable}" "{script_path}" %*\n')
        else:  # Unix-like (Linux, macOS)
            exec_path = os.path.join(self.temp_dir, name)  # type: ignore[unreachable]
            with open(exec_path, "w") as f:
                f.write(f"#!/bin/sh\n")
                f.write(f'exec "{sys.executable}" "{script_path}" "$@"\n')
            os.chmod(exec_path, 0o755)

        return exec_path

    def test_valid_executable(self):
        """Test with a valid executable"""
        mock_exec = self._create_mock_executable()
        result = self.launcher.check_executable(mock_exec)
        self.assertTrue(os.path.isabs(result))
        self.assertTrue(os.path.isfile(result))
        self.assertEqual(result, mock_exec)

    def test_nonexistent_path(self):
        """Test with non-existent path"""
        with self.assertRaises(SystemExit) as cm:
            self.launcher.check_executable("/nonexistent/path/to/file")
        self.assertEqual(cm.exception.code, 1)

    def test_directory_not_file(self):
        """Test with directory instead of file"""
        with self.assertRaises(SystemExit) as cm:
            self.launcher.check_executable(self.temp_dir)
        self.assertEqual(cm.exception.code, 1)

    @unittest.skipIf(os.name == "nt", "Executable bit test not applicable on Windows")
    def test_non_executable_file(self):
        """Test with non-executable file (Unix only)"""
        non_exec_file = os.path.join(self.temp_dir, "non_executable")
        Path(non_exec_file).touch()
        os.chmod(non_exec_file, 0o644)

        with self.assertRaises(SystemExit) as cm:
            self.launcher.check_executable(non_exec_file)
        self.assertEqual(cm.exception.code, 1)

    @unittest.skipIf(os.name == "nt", "Symlink test not reliable on Windows")
    def test_symlink_to_executable(self):
        """Test with symlink to executable"""
        mock_exec = self._create_mock_executable()
        symlink_path = os.path.join(self.temp_dir, "exec_link")
        os.symlink(mock_exec, symlink_path)

        result = self.launcher.check_executable(symlink_path)
        self.assertTrue(os.path.isabs(result))

    def test_windows_branch_skips_exec_bit(self):
        """Simulate Windows to cover branch where exec bit is not checked"""
        mock_exec = self._create_mock_executable()
        original_os_name = os.name
        try:
            self.launcher.os.name = "nt"  # type: ignore[attr-defined]
            result = self.launcher.check_executable(mock_exec)
            self.assertEqual(result, mock_exec)
        finally:
            self.launcher.os.name = original_os_name  # type: ignore[attr-defined]


class TestReadConfigTOML(unittest.TestCase):
    """Tests for read_config_toml() function"""

    def setUp(self):
        self.launcher = load_launcher_module()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _write_toml_config(self, content):
        """Helper to write TOML config to temp file"""
        config_path = os.path.join(self.temp_dir, "test.toml")
        with open(config_path, "w") as f:
            f.write(content)
        return config_path

    def test_simple_keys(self):
        """Test simple TOML key-value pairs"""
        config_path = self._write_toml_config(
            'object-store = "file"\n' 'node-id = "test-node"\n'
        )
        env_vars = self.launcher.read_config_toml(config_path, "core")
        self.assertEqual(
            env_vars,
            {
                "INFLUXDB3_OBJECT_STORE": "file",
                "INFLUXDB3_NODE_IDENTIFIER_PREFIX": "test-node",
            },
        )

    def test_string_values(self):
        """Test quoted string values in TOML"""
        config_path = self._write_toml_config(
            'object-store = "file"\n'
            'data-dir = "/var/lib/influxdb3/data"\n'
            'plugin-dir = "/var/lib/influxdb3/plugins"\n'
        )
        env_vars = self.launcher.read_config_toml(config_path, "core")
        self.assertEqual(
            env_vars,
            {
                "INFLUXDB3_OBJECT_STORE": "file",
                "INFLUXDB3_DB_DIR": "/var/lib/influxdb3/data",
                "INFLUXDB3_PLUGIN_DIR": "/var/lib/influxdb3/plugins",
            },
        )

    def test_integer_values(self):
        """Test integer values in TOML"""
        config_path = self._write_toml_config(
            'object-store = "file"\n'
            "ram-cache-size-mb = 1024\n"
            "gen1-duration-seconds = 600\n"
        )
        env_vars = self.launcher.read_config_toml(config_path, "core")
        self.assertEqual(
            env_vars,
            {
                "INFLUXDB3_OBJECT_STORE": "file",
                "INFLUXDB3_RAM_CACHE_SIZE_MB": "1024",
                "INFLUXDB3_GEN1_DURATION_SECONDS": "600",
            },
        )

    def test_boolean_values(self):
        """Test boolean values in TOML"""
        config_path = self._write_toml_config(
            'object-store = "file"\n'
            "force-snapshot-mem-threshold = true\n"
            "aws-allow-http = false\n"
        )
        env_vars = self.launcher.read_config_toml(config_path, "core")
        self.assertEqual(
            env_vars,
            {
                "INFLUXDB3_OBJECT_STORE": "file",
                "INFLUXDB3_FORCE_SNAPSHOT_MEM_THRESHOLD": "true",
                "AWS_ALLOW_HTTP": "false",
            },
        )

    def test_comments_ignored(self):
        """Test that TOML comments are ignored"""
        config_path = self._write_toml_config(
            "# This is a comment\n" 'object-store = "file"\n' "# Another comment\n"
        )
        env_vars = self.launcher.read_config_toml(config_path, "core")
        self.assertEqual(env_vars, {"INFLUXDB3_OBJECT_STORE": "file"})

    def test_blank_lines_ignored(self):
        """Test that blank lines are ignored"""
        config_path = self._write_toml_config(
            'object-store = "file"\n\n\nnode-id = "test"\n\n'
        )
        env_vars = self.launcher.read_config_toml(config_path, "core")
        self.assertEqual(
            env_vars,
            {
                "INFLUXDB3_OBJECT_STORE": "file",
                "INFLUXDB3_NODE_IDENTIFIER_PREFIX": "test",
            },
        )

    def test_special_characters_in_value(self):
        """Test special characters in TOML values"""
        config_path = self._write_toml_config(
            'object-store = "file"\n'
            'aws-endpoint = "https://s3.example.com:9000?foo=bar&baz=qux"\n'
        )
        env_vars = self.launcher.read_config_toml(config_path, "core")
        self.assertEqual(
            env_vars,
            {
                "INFLUXDB3_OBJECT_STORE": "file",
                "AWS_ENDPOINT": "https://s3.example.com:9000?foo=bar&baz=qux",
            },
        )

    def test_nonexistent_config(self):
        """Test with non-existent TOML config file"""
        with self.assertRaises(SystemExit) as cm:
            self.launcher.read_config_toml("/nonexistent/config.toml", "core")
        self.assertEqual(cm.exception.code, 1)

    def test_config_is_directory(self):
        """Test with directory instead of file"""
        with self.assertRaises(SystemExit) as cm:
            self.launcher.read_config_toml(self.temp_dir, "core")
        self.assertEqual(cm.exception.code, 1)

    def test_empty_config_file(self):
        """Test with empty TOML config file fails validation"""
        config_path = self._write_toml_config("")
        with self.assertRaises(SystemExit) as cm:
            self.launcher.read_config_toml(config_path, "core")
        self.assertEqual(cm.exception.code, 1)

    def test_invalid_toml(self):
        """Test with invalid TOML syntax"""
        config_path = self._write_toml_config('object-store = "unclosed\n')
        with self.assertRaises(SystemExit) as cm:
            self.launcher.read_config_toml(config_path, "core")
        self.assertEqual(cm.exception.code, 1)

    def test_mismatched_quotes_toml(self):
        """Test that TOML parser rejects mismatched quotes"""
        config_path = self._write_toml_config("object-store = \"file'\n")
        with self.assertRaises(SystemExit) as cm:
            self.launcher.read_config_toml(config_path, "core")
        self.assertEqual(cm.exception.code, 1)

    def test_core_flavor_mappings(self):
        """Test Core flavor-specific key mappings"""
        config_path = self._write_toml_config(
            'object-store = "file"\n'
            'http-bind = "0.0.0.0:8086"\n'
            'log-filter = "info"\n'
        )
        env_vars = self.launcher.read_config_toml(config_path, "core")
        self.assertEqual(
            env_vars,
            {
                "INFLUXDB3_OBJECT_STORE": "file",
                "INFLUXDB3_HTTP_BIND_ADDR": "0.0.0.0:8086",
                "LOG_FILTER": "info",
            },
        )

    def test_enterprise_flavor_mappings(self):
        """Test Enterprise flavor-specific key mappings"""
        config_path = self._write_toml_config(
            'object-store = "file"\n'
            'cluster-id = "my-cluster"\n'
            'mode = "all"\n'
            'license-file = "/path/to/license.jwt"\n'
        )
        env_vars = self.launcher.read_config_toml(config_path, "enterprise")
        self.assertEqual(
            env_vars,
            {
                "INFLUXDB3_OBJECT_STORE": "file",
                "INFLUXDB3_ENTERPRISE_CLUSTER_ID": "my-cluster",
                "INFLUXDB3_ENTERPRISE_MODE": "all",
                "INFLUXDB3_ENTERPRISE_LICENSE_FILE": "/path/to/license.jwt",
            },
        )

    def test_default_key_transformation(self):
        """Test default key transformation (dashes to underscores, uppercase, INFLUXDB3_ prefix)"""
        config_path = self._write_toml_config(
            'object-store = "file"\n'
            'some-custom-key = "value"\n'
            'another-key = "value2"\n'
        )
        env_vars = self.launcher.read_config_toml(config_path, "core")
        self.assertEqual(
            env_vars,
            {
                "INFLUXDB3_OBJECT_STORE": "file",
                "INFLUXDB3_SOME_CUSTOM_KEY": "value",
                "INFLUXDB3_ANOTHER_KEY": "value2",
            },
        )

    def test_mixed_types(self):
        """Test mixing different value types"""
        config_path = self._write_toml_config(
            'object-store = "file"\n'
            'node-id = "test-node"\n'
            "ram-cache-size-mb = 2048\n"
            "force-snapshot-mem-threshold = true\n"
        )
        env_vars = self.launcher.read_config_toml(config_path, "core")
        self.assertEqual(
            env_vars,
            {
                "INFLUXDB3_OBJECT_STORE": "file",
                "INFLUXDB3_NODE_IDENTIFIER_PREFIX": "test-node",
                "INFLUXDB3_RAM_CACHE_SIZE_MB": "2048",
                "INFLUXDB3_FORCE_SNAPSHOT_MEM_THRESHOLD": "true",
            },
        )


class TestValidateRequiredKeys(unittest.TestCase):
    """Tests for _validate_required_keys() function"""

    def setUp(self):
        self.launcher = load_launcher_module()

    def test_no_required_keys(self):
        """Test with no required keys (early return)"""
        config = {"FOO": "bar"}
        # Should not raise
        self.launcher._validate_required_keys(config, [], "test key")

    def test_single_required_key_present(self):
        """Test with single required key present"""
        config = {"object-store": "file"}
        # Should not raise
        self.launcher._validate_required_keys(config, ["object-store"], "TOML key")

    def test_single_required_key_missing(self):
        """Test with single required key missing"""
        config = {"foo": "bar"}
        with self.assertRaises(SystemExit) as cm:
            self.launcher._validate_required_keys(config, ["object-store"], "TOML key")
        self.assertEqual(cm.exception.code, 1)

    def test_comma_separated_all_present(self):
        """Test with comma-separated keys all present"""
        config = {"object-store": "file", "license-file": "/path/to/license.jwt"}
        # Should not raise
        self.launcher._validate_required_keys(
            config, ["object-store,license-file"], "TOML key"
        )

    def test_comma_separated_some_missing(self):
        """Test with comma-separated keys some missing"""
        config = {"object-store": "file"}
        with self.assertRaises(SystemExit) as cm:
            self.launcher._validate_required_keys(
                config, ["object-store,license-file"], "TOML key"
            )
        self.assertEqual(cm.exception.code, 1)

    def test_multiple_groups_one_satisfied(self):
        """Test with multiple groups, one satisfied"""
        config = {"object-store": "file", "license-file": "/path/to/license.jwt"}
        # Should not raise - first group is satisfied
        self.launcher._validate_required_keys(
            config,
            [
                "object-store,license-file",
                "object-store,license-email,license-type",
            ],
            "TOML key",
        )

    def test_multiple_groups_none_satisfied(self):
        """Test with multiple groups, none satisfied"""
        config = {"object-store": "file"}
        with self.assertRaises(SystemExit) as cm:
            self.launcher._validate_required_keys(
                config,
                [
                    "object-store,license-file",
                    "object-store,license-email,license-type",
                ],
                "TOML key",
            )
        self.assertEqual(cm.exception.code, 1)

    def test_empty_value_treated_as_missing(self):
        """Test that empty values are treated as missing"""
        config = {"object-store": "file", "license-file": ""}
        with self.assertRaises(SystemExit) as cm:
            self.launcher._validate_required_keys(
                config, ["object-store,license-file"], "TOML key"
            )
        self.assertEqual(cm.exception.code, 1)

    def test_toml_nested_value_skipped(self):
        """Nested TOML values should be skipped, scalars retained"""
        fd, toml_path = tempfile.mkstemp()
        os.close(fd)
        with open(toml_path, "w") as f:
            f.write('object-store = "file"\n[extra]\nfoo = "bar"\n')
        try:
            result = self.launcher.read_config_toml(toml_path, "core")
            self.assertEqual(result, {"INFLUXDB3_OBJECT_STORE": "file"})
        finally:
            os.remove(toml_path)

    def test_non_scalar_value_raises(self):
        """Non-scalar values should be rejected (flat TOML only)"""
        config = {"object-store": {"nested": "nope"}}
        with self.assertRaises(SystemExit) as cm:
            self.launcher._validate_required_keys(config, ["object-store"], "TOML key")
        self.assertEqual(cm.exception.code, 1)

    def test_boolean_scalar_allowed(self):
        """Booleans are valid scalars for required keys"""
        config = {"object-store": "file", "feature-flag": True}
        # Should not raise
        self.launcher._validate_required_keys(
            config, ["object-store,feature-flag"], "TOML key"
        )

    def test_enterprise_to_core_downgrade_fails(self):
        """Downgrading enterprise -> core should exit with error"""
        temp_dir = tempfile.mkdtemp()
        try:
            stamp = os.path.join(temp_dir, ".influxdb3-launcher")
            with open(stamp, "w") as f:
                f.write("enterprise\n")
            with self.assertRaises(SystemExit) as cm:
                self.launcher.check_flavor_migration(stamp, "core")
            self.assertEqual(cm.exception.code, 1)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


class TestReadConfigTOMLValidation(unittest.TestCase):
    """Tests for read_config_toml() with flavor-based validation"""

    def setUp(self):
        self.launcher = load_launcher_module()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _write_toml(self, content):
        """Helper to write TOML config"""
        path = os.path.join(self.temp_dir, "test.toml")
        with open(path, "w") as f:
            f.write(content)
        return path

    def test_toml_core(self):
        """Test with TOML config for core flavor"""
        toml_path = self._write_toml('object-store = "file"\n')
        result = self.launcher.read_config_toml(toml_path, "core")
        self.assertEqual(result, {"INFLUXDB3_OBJECT_STORE": "file"})

    def test_enterprise_with_license_file(self):
        """Test enterprise flavor with license file"""
        toml_path = self._write_toml(
            'object-store = "file"\nlicense-file = "/path/to/license.jwt"\n'
        )
        result = self.launcher.read_config_toml(toml_path, "enterprise")
        self.assertEqual(
            result,
            {
                "INFLUXDB3_OBJECT_STORE": "file",
                "INFLUXDB3_ENTERPRISE_LICENSE_FILE": "/path/to/license.jwt",
            },
        )

    def test_enterprise_with_license_email_and_type(self):
        """Test enterprise flavor with license email and type"""
        toml_path = self._write_toml(
            'object-store = "file"\n'
            'license-email = "test@example.com"\n'
            'license-type = "trial"\n'
        )
        result = self.launcher.read_config_toml(toml_path, "enterprise")
        self.assertEqual(
            result,
            {
                "INFLUXDB3_OBJECT_STORE": "file",
                "INFLUXDB3_ENTERPRISE_LICENSE_EMAIL": "test@example.com",
                "INFLUXDB3_ENTERPRISE_LICENSE_TYPE": "trial",
            },
        )


class TestWritePidfile(unittest.TestCase):
    """Tests for write_pidfile() function"""

    def setUp(self):
        self.launcher = load_launcher_module()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_write_pidfile_basic(self):
        """Test basic PID file write"""
        pidfile = os.path.join(self.temp_dir, "test.pid")
        self.launcher.write_pidfile(pidfile)
        self.assertTrue(os.path.exists(pidfile))
        with open(pidfile) as f:
            pid = int(f.read().strip())
        self.assertEqual(pid, os.getpid())

    def test_write_pidfile_creates_directory(self):
        """Test that write_pidfile creates parent directory"""
        pidfile = os.path.join(self.temp_dir, "subdir", "test.pid")
        self.launcher.write_pidfile(pidfile)
        self.assertTrue(os.path.exists(pidfile))
        with open(pidfile) as f:
            pid = int(f.read().strip())
        self.assertEqual(pid, os.getpid())

    def test_write_pidfile_permissions(self):
        """Test that PID file has correct permissions"""
        pidfile = os.path.join(self.temp_dir, "test.pid")
        self.launcher.write_pidfile(pidfile)
        stat_info = os.stat(pidfile)
        # Check that it's readable by all (0o644)
        self.assertEqual(stat_info.st_mode & 0o777, 0o644)

    def test_write_pidfile_explicit_pid(self):
        """Test writing a specific PID to the pidfile"""
        pidfile = os.path.join(self.temp_dir, "test.pid")
        explicit_pid = 12345
        self.launcher.write_pidfile(pidfile, explicit_pid)
        self.assertTrue(os.path.exists(pidfile))
        with open(pidfile) as f:
            pid = int(f.read().strip())
        self.assertEqual(pid, explicit_pid)


class TestDaemonize(unittest.TestCase):
    """Tests for daemonize() function"""

    def setUp(self):
        self.launcher = load_launcher_module()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_platform_supports_fork_constant(self):
        """Test that PLATFORM_SUPPORTS_FORK is correctly set"""
        # On Unix-like systems (Linux, macOS), both fork and setsid should exist
        if os.name != "nt":
            self.assertTrue(self.launcher.PLATFORM_SUPPORTS_FORK)
        else:
            # On Windows, fork doesn't exist
            self.assertFalse(self.launcher.PLATFORM_SUPPORTS_FORK)

    @unittest.skipIf(os.name == "nt", "Fork not available on Windows")
    def test_daemonize_forks_and_writes_pidfile(self):
        """Test that daemonize forks and writes correct PID to pidfile"""
        pidfile = os.path.join(self.temp_dir, "daemon.pid")

        # Fork so we can test daemonize in a child process
        pid = os.fork()
        if pid == 0:
            # Child: call daemonize, then exit
            try:
                self.launcher.daemonize(pidfile=pidfile)
                # If we get here, we're the daemon child
                # Write a marker file to prove we continued
                marker = os.path.join(self.temp_dir, "daemon_ran")
                with open(marker, "w") as f:
                    f.write(str(os.getpid()))
                os._exit(0)
            except Exception:
                os._exit(1)
        else:
            # Parent: wait for intermediate child to exit
            _, status = os.waitpid(pid, 0)
            self.assertEqual(os.WEXITSTATUS(status), 0)

            # Give daemon time to write marker file
            time.sleep(0.5)

            # Check pidfile was created
            self.assertTrue(os.path.exists(pidfile))

            # Read the PID from pidfile
            with open(pidfile) as f:
                daemon_pid = int(f.read().strip())

            # The PID in the file should be different from our pid and the intermediate child
            self.assertNotEqual(daemon_pid, os.getpid())
            self.assertNotEqual(daemon_pid, pid)

            # Check the daemon ran (marker file exists)
            marker = os.path.join(self.temp_dir, "daemon_ran")
            # Wait a bit more if needed
            for _ in range(10):
                if os.path.exists(marker):
                    break
                time.sleep(0.1)

            self.assertTrue(os.path.exists(marker))

            # Clean up daemon process if still running
            try:
                os.kill(daemon_pid, 9)
            except ProcessLookupError:
                pass  # Already exited

    @unittest.skipIf(os.name == "nt", "Fork not available on Windows")
    def test_daemonize_creates_new_session(self):
        """Test that daemonize creates a new session (setsid) via double-fork"""
        pidfile = os.path.join(self.temp_dir, "daemon.pid")
        sid_file = os.path.join(self.temp_dir, "daemon_sid")
        original_sid = os.getsid(0)

        pid = os.fork()
        if pid == 0:
            try:
                self.launcher.daemonize(pidfile=pidfile)
                # Write our session ID
                with open(sid_file, "w") as f:
                    f.write(str(os.getsid(0)))
                os._exit(0)
            except Exception:
                os._exit(1)
        else:
            os.waitpid(pid, 0)
            time.sleep(0.5)

            # Check SID file exists
            for _ in range(10):
                if os.path.exists(sid_file):
                    break
                time.sleep(0.1)

            if os.path.exists(sid_file):
                with open(sid_file) as f:
                    daemon_sid = int(f.read().strip())
                with open(pidfile) as f:
                    daemon_pid = int(f.read().strip())

                # With double-fork, daemon is NOT the session leader (that's the point).
                # The SID is the PID of the intermediate child (which called setsid).
                # We verify daemon is in a NEW session, different from ours.
                self.assertNotEqual(daemon_sid, original_sid)
                # The daemon's SID should NOT equal its PID (it's not a session leader)
                self.assertNotEqual(daemon_sid, daemon_pid)

                # Clean up
                try:
                    os.kill(daemon_pid, 9)
                except ProcessLookupError:
                    pass

    @unittest.skipIf(os.name == "nt", "Fork not available on Windows")
    def test_daemonize_redirects_to_log_file(self):
        """Test that daemonize redirects stdout/stderr to log file"""
        pidfile = os.path.join(self.temp_dir, "daemon.pid")
        log_file = os.path.join(self.temp_dir, "daemon.log")

        pid = os.fork()
        if pid == 0:
            try:
                self.launcher.daemonize(pidfile=pidfile, log_file=log_file)
                # Write to stdout - should go to log file
                print("stdout test message")
                sys.stdout.flush()
                # Write to stderr - should also go to log file
                print("stderr test message", file=sys.stderr)
                sys.stderr.flush()
                os._exit(0)
            except Exception:
                os._exit(1)
        else:
            os.waitpid(pid, 0)
            time.sleep(0.5)

            # Check log file was created and has content
            for _ in range(10):
                if os.path.exists(log_file) and os.path.getsize(log_file) > 0:
                    break
                time.sleep(0.1)

            self.assertTrue(os.path.exists(log_file))
            with open(log_file) as f:
                content = f.read()
            self.assertIn("stdout test message", content)
            self.assertIn("stderr test message", content)

            # Clean up daemon
            if os.path.exists(pidfile):
                with open(pidfile) as f:
                    daemon_pid = int(f.read().strip())
                try:
                    os.kill(daemon_pid, 9)
                except ProcessLookupError:
                    pass

    @unittest.skipIf(os.name == "nt", "Fork not available on Windows")
    def test_daemonize_log_file_open_failure(self):
        """If log file open fails, daemonize should fall back without crashing"""
        pidfile = os.path.join(self.temp_dir, "daemon.pid")
        log_file = os.path.join(self.temp_dir, "forbidden.log")

        pid = os.fork()
        if pid == 0:
            try:
                original_open = os.open

                def fake_open(path, flags, mode=0o777):
                    if path == log_file:
                        raise OSError("denied")
                    return original_open(path, flags, mode)

                os.open = fake_open  # type: ignore[assignment]
                self.launcher.daemonize(pidfile=pidfile, log_file=log_file)
                os._exit(0)
            except Exception:
                os._exit(1)
        else:
            os.waitpid(pid, 0)
            time.sleep(0.5)

            # Log file should not have been created
            self.assertFalse(os.path.exists(log_file))

            # Clean up daemon
            if os.path.exists(pidfile):
                with open(pidfile) as f:
                    daemon_pid = int(f.read().strip())
                try:
                    os.kill(daemon_pid, 9)
                except ProcessLookupError:
                    pass

    def test_daemonize_mocked_double_fork(self):
        """Mock forks to cover daemonize path without real forking"""
        calls = []
        fork_sequence = iter([0, 0])  # first fork -> child, second -> grandchild

        def fake_fork():
            return next(fork_sequence)

        def fake_setsid():
            calls.append("setsid")

        def fake_signal(sig, handler):
            calls.append(("signal", sig, handler))

        def fake_write_pidfile(pidfile):
            calls.append(("pidfile", pidfile))

        def fake_open(path, flags, mode=0o777):
            calls.append(("open", path))
            return 3

        def fake_dup2(fd, target):
            calls.append(("dup2", fd, target))

        def fake_close(fd):
            calls.append(("close", fd))

        original_fork = os.fork
        original_setsid = os.setsid
        original_signal = signal.signal
        original_open = os.open
        original_dup2 = os.dup2
        original_close = os.close
        original_write_pidfile = self.launcher.write_pidfile
        original_platform = getattr(self.launcher, "PLATFORM_SUPPORTS_FORK")
        try:
            setattr(self.launcher, "PLATFORM_SUPPORTS_FORK", True)
            os.fork = fake_fork  # type: ignore[assignment]
            os.setsid = fake_setsid  # type: ignore[assignment]
            signal.signal = fake_signal  # type: ignore[assignment]
            os.open = fake_open  # type: ignore[assignment]
            os.dup2 = fake_dup2  # type: ignore[assignment]
            os.close = fake_close  # type: ignore[assignment]
            self.launcher.write_pidfile = fake_write_pidfile  # type: ignore[assignment]

            self.launcher.daemonize(pidfile="/tmp/mock.pid", log_file="/tmp/mock.log")

            # Ensure key steps were called
            self.assertIn("setsid", calls)
            self.assertIn(("signal", signal.SIGHUP, signal.SIG_IGN), calls)
            self.assertIn(("pidfile", "/tmp/mock.pid"), calls)
            self.assertIn(("open", "/tmp/mock.log"), calls)
            self.assertIn(("dup2", 3, sys.stdout.fileno()), calls)
            self.assertIn(("dup2", 3, sys.stderr.fileno()), calls)
            self.assertIn(("close", 3), calls)
        finally:
            os.fork = original_fork  # type: ignore[assignment]
            os.setsid = original_setsid  # type: ignore[assignment]
            signal.signal = original_signal  # type: ignore[assignment]
            os.open = original_open  # type: ignore[assignment]
            os.dup2 = original_dup2  # type: ignore[assignment]
            os.close = original_close  # type: ignore[assignment]
            self.launcher.write_pidfile = original_write_pidfile  # type: ignore[assignment]
            setattr(self.launcher, "PLATFORM_SUPPORTS_FORK", original_platform)

    def test_daemonize_unsupported_platform(self):
        """Test that daemonize fails gracefully on unsupported platforms"""
        # Temporarily mock PLATFORM_SUPPORTS_FORK to False
        original_value = getattr(self.launcher, "PLATFORM_SUPPORTS_FORK")
        try:
            setattr(self.launcher, "PLATFORM_SUPPORTS_FORK", False)

            with self.assertRaises(SystemExit) as cm:
                self.launcher.daemonize()
            self.assertEqual(cm.exception.code, 1)
        finally:
            setattr(self.launcher, "PLATFORM_SUPPORTS_FORK", original_value)


class TestRun(unittest.TestCase):
    """Tests for run() function"""

    def setUp(self):
        self.launcher = load_launcher_module()
        self.temp_dir = tempfile.mkdtemp()
        # Save original os.execve to restore later
        self.original_execve = os.execve
        # Save original environment to restore later
        self.original_environ = os.environ.copy()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        # Restore original execve
        os.execve = self.original_execve
        # Restore original environment
        os.environ.clear()
        os.environ.update(self.original_environ)

    def test_run_environment_precedence(self):
        """Test that parent environment overrides config"""
        # Mock execve to capture what would be passed
        called_with = {}

        def mock_execve(path, args, env):
            called_with["path"] = path
            called_with["args"] = args
            called_with["env"] = env
            # Simulate successful exec by raising SystemExit (exec never returns)
            raise SystemExit(0)

        os.execve = mock_execve

        # Set parent environment
        os.environ["TEST_VAR"] = "from_parent"

        exec_path = "/usr/bin/influxdb3"
        exec_args = ["serve"]
        env_vars = {"TEST_VAR": "from_config", "OTHER_VAR": "value"}

        with self.assertRaises(SystemExit) as cm:
            self.launcher.run(exec_path, exec_args, env_vars)
        self.assertEqual(cm.exception.code, 0)

        # Verify parent env won
        self.assertEqual(called_with["env"]["TEST_VAR"], "from_parent")
        self.assertEqual(called_with["env"]["OTHER_VAR"], "value")

    def test_run_with_pidfile(self):
        """Test run() with pidfile"""
        called_with = {}

        def mock_execve(path, args, env):
            called_with["path"] = path
            called_with["args"] = args
            called_with["env"] = env
            # Simulate successful exec by raising SystemExit (exec never returns)
            raise SystemExit(0)

        os.execve = mock_execve

        exec_path = "/usr/bin/influxdb3"
        exec_args = ["serve"]
        env_vars = {"TEST": "value"}
        pidfile = os.path.join(self.temp_dir, "test.pid")

        with self.assertRaises(SystemExit) as cm:
            self.launcher.run(exec_path, exec_args, env_vars, pidfile)
        self.assertEqual(cm.exception.code, 0)

        # Verify pidfile was written
        self.assertTrue(os.path.exists(pidfile))
        with open(pidfile) as f:
            pid = int(f.read().strip())
        self.assertEqual(pid, os.getpid())

    def test_run_argument_passing(self):
        """Test that arguments are passed correctly"""
        called_with = {}

        def mock_execve(path, args, env):
            called_with["path"] = path
            called_with["args"] = args
            called_with["env"] = env
            # Simulate successful exec by raising SystemExit (exec never returns)
            raise SystemExit(0)

        os.execve = mock_execve

        exec_path = "/usr/bin/influxdb3"
        exec_args = ["serve", "--verbose", "--port=8086"]
        env_vars = {}

        with self.assertRaises(SystemExit) as cm:
            self.launcher.run(exec_path, exec_args, env_vars)
        self.assertEqual(cm.exception.code, 0)

        # Verify full args list (argv[0] should be exec_path)
        self.assertEqual(called_with["args"][0], exec_path)
        self.assertEqual(called_with["args"][1:], exec_args)

    @unittest.skipUnless(
        hasattr(signal, "SIGHUP"), "SIGHUP not available on this platform"
    )
    def test_run_restores_sighup_before_exec(self):
        """run() should reset SIGHUP to default if it was ignored"""
        called_handler: list = [None]

        def mock_execve(path, args, env):
            # Capture handler at exec time
            called_handler[0] = signal.getsignal(signal.SIGHUP)
            raise SystemExit(0)

        original_handler = signal.getsignal(signal.SIGHUP)
        try:
            signal.signal(signal.SIGHUP, signal.SIG_IGN)
            os.execve = mock_execve

            with self.assertRaises(SystemExit) as cm:
                self.launcher.run("/usr/bin/influxdb3", [], {})
            self.assertEqual(cm.exception.code, 0)

            self.assertEqual(called_handler[0], signal.SIG_DFL)
        finally:
            signal.signal(signal.SIGHUP, original_handler)

    @unittest.skipUnless(
        hasattr(signal, "SIGHUP"), "SIGHUP not available on this platform"
    )
    def test_run_keeps_sighup_default(self):
        """If SIGHUP already default, run() should leave it unchanged"""
        called_handler: list = [None]

        def mock_execve(path, args, env):
            called_handler[0] = signal.getsignal(signal.SIGHUP)
            raise SystemExit(0)

        original_handler = signal.getsignal(signal.SIGHUP)
        try:
            signal.signal(signal.SIGHUP, signal.SIG_DFL)
            os.execve = mock_execve

            with self.assertRaises(SystemExit) as cm:
                self.launcher.run("/usr/bin/influxdb3", [], {})
            self.assertEqual(cm.exception.code, 0)

            self.assertEqual(called_handler[0], signal.SIG_DFL)
        finally:
            signal.signal(signal.SIGHUP, original_handler)


class TestMain(unittest.TestCase):
    """Tests for main() function"""

    def setUp(self):
        self.launcher = load_launcher_module()
        self.temp_dir = tempfile.mkdtemp()
        # Save original os.execve to restore later
        self.original_execve = os.execve
        # Save original environment to restore later
        self.original_environ = os.environ.copy()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        # Restore original execve
        os.execve = self.original_execve
        # Restore original environment
        os.environ.clear()
        os.environ.update(self.original_environ)

    def _create_executable(self):
        """Create a mock executable file"""
        exec_path = os.path.join(self.temp_dir, "mock_exec")
        with open(exec_path, "w") as f:
            f.write("#!/bin/sh\necho test\n")
        os.chmod(exec_path, 0o755)
        return exec_path

    def _create_toml_config(self, content):
        """Create a TOML config file and return its path"""
        config_path = os.path.join(self.temp_dir, "test.toml")
        with open(config_path, "w") as f:
            f.write(content)
        return config_path

    def test_missing_required_flavor(self):
        """Test that missing --flavor causes error"""
        exec_path = self._create_executable()
        config_path = self._create_toml_config('object-store = "file"\n')

        argv = [
            "launcher",
            "--exec",
            exec_path,
            "--config-toml",
            config_path,
            "--stamp-dir",
            self.temp_dir,
            "--",
        ]

        # Capture stderr to check error message
        f = io.StringIO()
        old_stderr = sys.stderr
        try:
            sys.stderr = f
            with self.assertRaises(SystemExit) as cm:
                self.launcher.main(argv)
        finally:
            sys.stderr = old_stderr

        self.assertEqual(cm.exception.code, 2)  # argparse error code
        stderr_output = f.getvalue()
        self.assertIn("required", stderr_output.lower())

    def test_missing_required_exec(self):
        """Test that missing --exec causes error"""
        config_path = self._create_toml_config('object-store = "file"\n')

        argv = [
            "launcher",
            "--flavor",
            "core",
            "--config-toml",
            config_path,
            "--stamp-dir",
            self.temp_dir,
            "--",
        ]

        with self.assertRaises(SystemExit) as cm:
            self.launcher.main(argv)
        self.assertEqual(cm.exception.code, 2)  # argparse error code

    def test_missing_required_stamp_dir(self):
        """Test that missing --stamp-dir causes error"""
        exec_path = self._create_executable()
        config_path = self._create_toml_config('object-store = "file"\n')

        argv = [
            "launcher",
            "--flavor",
            "core",
            "--exec",
            exec_path,
            "--config-toml",
            config_path,
            "--",
        ]

        with self.assertRaises(SystemExit) as cm:
            self.launcher.main(argv)
        self.assertEqual(cm.exception.code, 2)  # argparse error code

    def test_invalid_flavor(self):
        """Test that invalid flavor value causes error"""
        exec_path = self._create_executable()
        config_path = self._create_toml_config('object-store = "file"\n')

        argv = [
            "launcher",
            "--flavor",
            "invalid",
            "--exec",
            exec_path,
            "--config-toml",
            config_path,
            "--stamp-dir",
            self.temp_dir,
            "--",
        ]

        # Capture stderr to check error message
        f = io.StringIO()
        old_stderr = sys.stderr
        try:
            sys.stderr = f
            with self.assertRaises(SystemExit) as cm:
                self.launcher.main(argv)
        finally:
            sys.stderr = old_stderr

        self.assertEqual(cm.exception.code, 2)  # argparse error code
        stderr_output = f.getvalue()
        self.assertIn("invalid choice", stderr_output.lower())

    def test_missing_config_toml(self):
        """Test that missing --config-toml causes error"""
        exec_path = self._create_executable()

        argv = [
            "launcher",
            "--flavor",
            "core",
            "--exec",
            exec_path,
            "--stamp-dir",
            self.temp_dir,
            "--",
        ]

        with self.assertRaises(SystemExit) as cm:
            self.launcher.main(argv)
        self.assertEqual(cm.exception.code, 2)  # argparse error code

    def test_separator_splits_arguments(self):
        """Test that -- separator properly splits launcher and exec args"""
        # Mock execve to capture what would be passed
        called_with = {}

        def mock_execve(path, args, env):
            called_with["path"] = path
            called_with["args"] = args
            called_with["env"] = env
            raise SystemExit(0)

        os.execve = mock_execve

        exec_path = self._create_executable()
        config_path = self._create_toml_config('object-store = "file"\n')

        argv = [
            "launcher",
            "--flavor",
            "core",
            "--exec",
            exec_path,
            "--config-toml",
            config_path,
            "--stamp-dir",
            self.temp_dir,
            "--",
            "serve",
            "--verbose",
        ]

        with self.assertRaises(SystemExit) as cm:
            self.launcher.main(argv)
        self.assertEqual(cm.exception.code, 0)

        # Verify exec args were passed correctly
        self.assertEqual(called_with["args"][0], exec_path)
        self.assertEqual(called_with["args"][1:], ["serve", "--verbose"])

    def test_no_separator_empty_exec_args(self):
        """Test that no -- separator results in empty exec args"""
        # Mock execve to capture what would be passed
        called_with = {}

        def mock_execve(path, args, env):
            called_with["path"] = path
            called_with["args"] = args
            called_with["env"] = env
            raise SystemExit(0)

        os.execve = mock_execve

        exec_path = self._create_executable()
        config_path = self._create_toml_config('object-store = "file"\n')

        argv = [
            "launcher",
            "--flavor",
            "core",
            "--exec",
            exec_path,
            "--config-toml",
            config_path,
            "--stamp-dir",
            self.temp_dir,
        ]

        with self.assertRaises(SystemExit) as cm:
            self.launcher.main(argv)
        self.assertEqual(cm.exception.code, 0)

        # Verify exec args are empty
        self.assertEqual(called_with["args"], [exec_path])

    def test_successful_execution_with_pidfile(self):
        """Test successful execution with pidfile option"""
        # Mock execve to capture what would be passed
        called_with = {}

        def mock_execve(path, args, env):
            called_with["path"] = path
            called_with["args"] = args
            called_with["env"] = env
            raise SystemExit(0)

        os.execve = mock_execve

        exec_path = self._create_executable()
        config_path = self._create_toml_config('object-store = "file"\n')
        pidfile = os.path.join(self.temp_dir, "test.pid")

        argv = [
            "launcher",
            "--flavor",
            "core",
            "--exec",
            exec_path,
            "--config-toml",
            config_path,
            "--stamp-dir",
            self.temp_dir,
            "--pidfile",
            pidfile,
            "--",
            "serve",
        ]

        with self.assertRaises(SystemExit) as cm:
            self.launcher.main(argv)
        self.assertEqual(cm.exception.code, 0)

        # Verify pidfile was written
        self.assertTrue(os.path.exists(pidfile))
        with open(pidfile) as f:
            pid = int(f.read().strip())
        self.assertEqual(pid, os.getpid())

    def test_core_missing_object_store(self):
        """Test that Core without object-store fails with error"""
        exec_path = self._create_executable()
        config_path = self._create_toml_config('foo = "bar"\n')

        argv = [
            "launcher",
            "--flavor",
            "core",
            "--exec",
            exec_path,
            "--config-toml",
            config_path,
            "--stamp-dir",
            self.temp_dir,
            "--",
        ]

        # Capture stderr to check error message
        f = io.StringIO()
        old_stderr = sys.stderr
        try:
            sys.stderr = f
            with self.assertRaises(SystemExit) as cm:
                self.launcher.main(argv)
        finally:
            sys.stderr = old_stderr

        self.assertEqual(cm.exception.code, 1)
        stderr_output = f.getvalue()
        self.assertIn("required configuration", stderr_output.lower())

    def test_enterprise_missing_license(self):
        """Test that Enterprise without license fails with error"""
        exec_path = self._create_executable()
        config_path = self._create_toml_config('object-store = "file"\n')

        argv = [
            "launcher",
            "--flavor",
            "enterprise",
            "--exec",
            exec_path,
            "--config-toml",
            config_path,
            "--stamp-dir",
            self.temp_dir,
            "--",
        ]

        # Capture stderr to check error message
        f = io.StringIO()
        old_stderr = sys.stderr
        try:
            sys.stderr = f
            with self.assertRaises(SystemExit) as cm:
                self.launcher.main(argv)
        finally:
            sys.stderr = old_stderr

        self.assertEqual(cm.exception.code, 1)
        stderr_output = f.getvalue()
        self.assertIn("required configuration", stderr_output.lower())

    def test_enterprise_only_email_fails(self):
        """Test that Enterprise with only license-email fails"""
        exec_path = self._create_executable()
        config_path = self._create_toml_config(
            'object-store = "file"\nlicense-email = "user@example.com"\n'
        )

        argv = [
            "launcher",
            "--flavor",
            "enterprise",
            "--exec",
            exec_path,
            "--config-toml",
            config_path,
            "--stamp-dir",
            self.temp_dir,
            "--",
        ]

        # Capture stderr to check error message
        f = io.StringIO()
        old_stderr = sys.stderr
        try:
            sys.stderr = f
            with self.assertRaises(SystemExit) as cm:
                self.launcher.main(argv)
        finally:
            sys.stderr = old_stderr

        self.assertEqual(cm.exception.code, 1)
        stderr_output = f.getvalue()
        self.assertIn("required configuration", stderr_output.lower())

    def test_log_file_without_daemonize_warns(self):
        """Test that --log-file without --daemonize produces a warning"""

        # Mock execve to capture what would be passed
        def mock_execve(path, args, env):
            _ = path
            _ = args
            _ = env
            raise SystemExit(0)

        os.execve = mock_execve

        exec_path = self._create_executable()
        config_path = self._create_toml_config('object-store = "file"\n')
        log_file = os.path.join(self.temp_dir, "test.log")

        argv = [
            "launcher",
            "--flavor",
            "core",
            "--exec",
            exec_path,
            "--config-toml",
            config_path,
            "--stamp-dir",
            self.temp_dir,
            "--log-file",
            log_file,
            "--",
        ]

        # Capture stderr to check warning message
        f = io.StringIO()
        old_stderr = sys.stderr
        try:
            sys.stderr = f
            with self.assertRaises(SystemExit) as cm:
                self.launcher.main(argv)
        finally:
            sys.stderr = old_stderr

        self.assertEqual(cm.exception.code, 0)
        stderr_output = f.getvalue()
        self.assertIn("--log-file has no effect without --daemonize", stderr_output)

    @unittest.skipIf(os.name == "nt", "Daemonize not available on Windows")
    def test_daemonize_with_pidfile(self):
        """Test --daemonize with --pidfile creates background process"""
        exec_path = self._create_executable()
        config_path = self._create_toml_config('object-store = "file"\n')
        pidfile = os.path.join(self.temp_dir, "daemon.pid")
        launcher_path = str(
            Path(__file__).parent / "influxdb3/fs/usr/lib/influxdb3/influxdb3-launcher"
        )

        # Run launcher directly via subprocess since daemonize will fork
        result = subprocess.run(
            [
                sys.executable,
                launcher_path,
                "--flavor",
                "core",
                "--exec",
                exec_path,
                "--config-toml",
                config_path,
                "--stamp-dir",
                self.temp_dir,
                "--daemonize",
                "--pidfile",
                pidfile,
                "--",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )

        # Parent should exit with 0
        self.assertEqual(result.returncode, 0)

        # Give daemon time to write pidfile
        time.sleep(0.5)

        # Check pidfile was created
        self.assertTrue(os.path.exists(pidfile))

        # Read the PID and verify it's a valid process
        with open(pidfile) as f:
            daemon_pid = int(f.read().strip())

        # Clean up daemon
        try:
            os.kill(daemon_pid, 9)
        except ProcessLookupError:
            pass  # Already exited

    def test_daemonize_unsupported_platform_via_main(self):
        """Test that --daemonize fails on unsupported platforms via main()"""
        exec_path = self._create_executable()
        config_path = self._create_toml_config('object-store = "file"\n')

        # Temporarily mock PLATFORM_SUPPORTS_FORK to False
        original_value = getattr(self.launcher, "PLATFORM_SUPPORTS_FORK")
        try:
            setattr(self.launcher, "PLATFORM_SUPPORTS_FORK", False)

            argv = [
                "launcher",
                "--flavor",
                "core",
                "--exec",
                exec_path,
                "--config-toml",
                config_path,
                "--stamp-dir",
                self.temp_dir,
                "--daemonize",
                "--",
            ]

            # Capture stderr
            f = io.StringIO()
            old_stderr = sys.stderr
            try:
                sys.stderr = f
                with self.assertRaises(SystemExit) as cm:
                    self.launcher.main(argv)
            finally:
                sys.stderr = old_stderr

            self.assertEqual(cm.exception.code, 1)
            stderr_output = f.getvalue()
            self.assertIn("not supported on this platform", stderr_output)
        finally:
            setattr(self.launcher, "PLATFORM_SUPPORTS_FORK", original_value)


class TestFlavorStamp(unittest.TestCase):
    """Tests for stamp file functions (read_stamp, write_stamp, check_flavor_migration)"""

    def setUp(self):
        self.launcher = load_launcher_module()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_read_stamp_nonexistent(self):
        """Test read_stamp with non-existent file"""
        stamp_path = os.path.join(self.temp_dir, ".influxdb3-launcher")
        result = self.launcher.read_stamp(stamp_path)
        self.assertIsNone(result)

    def test_write_and_read_stamp(self):
        """Test write_stamp and read_stamp round-trip"""
        stamp_path = os.path.join(self.temp_dir, ".influxdb3-launcher")
        self.launcher.write_stamp(stamp_path, "core")
        result = self.launcher.read_stamp(stamp_path)
        self.assertEqual(result, "core")

    def test_write_stamp_enterprise(self):
        """Test write_stamp with enterprise flavor"""
        stamp_path = os.path.join(self.temp_dir, ".influxdb3-launcher")
        self.launcher.write_stamp(stamp_path, "enterprise")
        result = self.launcher.read_stamp(stamp_path)
        self.assertEqual(result, "enterprise")

    def test_write_stamp_overwrites(self):
        """Test that write_stamp overwrites existing stamp"""
        stamp_path = os.path.join(self.temp_dir, ".influxdb3-launcher")
        self.launcher.write_stamp(stamp_path, "core")
        self.launcher.write_stamp(stamp_path, "enterprise")
        result = self.launcher.read_stamp(stamp_path)
        self.assertEqual(result, "enterprise")

    def test_check_flavor_migration_fresh_install_core(self):
        """Test fresh install with core flavor writes stamp"""
        stamp_path = os.path.join(self.temp_dir, ".influxdb3-launcher")
        self.launcher.check_flavor_migration(stamp_path, "core")
        result = self.launcher.read_stamp(stamp_path)
        self.assertEqual(result, "core")

    def test_check_flavor_migration_fresh_install_enterprise(self):
        """Test fresh install with enterprise flavor writes stamp"""
        stamp_path = os.path.join(self.temp_dir, ".influxdb3-launcher")
        self.launcher.check_flavor_migration(stamp_path, "enterprise")
        result = self.launcher.read_stamp(stamp_path)
        self.assertEqual(result, "enterprise")

    def test_check_flavor_migration_same_flavor_core(self):
        """Test same flavor (core) does not modify stamp"""
        stamp_path = os.path.join(self.temp_dir, ".influxdb3-launcher")
        self.launcher.write_stamp(stamp_path, "core")
        original_mtime = os.path.getmtime(stamp_path)

        # Small delay to ensure mtime would change if file was modified
        time.sleep(0.1)

        self.launcher.check_flavor_migration(stamp_path, "core")
        new_mtime = os.path.getmtime(stamp_path)

        # Stamp should not have been modified
        self.assertEqual(original_mtime, new_mtime)

    def test_check_flavor_migration_same_flavor_enterprise(self):
        """Test same flavor (enterprise) does not modify stamp"""
        stamp_path = os.path.join(self.temp_dir, ".influxdb3-launcher")
        self.launcher.write_stamp(stamp_path, "enterprise")
        original_mtime = os.path.getmtime(stamp_path)

        # Small delay to ensure mtime would change if file was modified
        time.sleep(0.1)

        self.launcher.check_flavor_migration(stamp_path, "enterprise")
        new_mtime = os.path.getmtime(stamp_path)

        self.assertEqual(original_mtime, new_mtime)

    def test_check_flavor_migration_core_to_enterprise(self):
        """Test upgrade from core to enterprise prints message and doesn't update stamp"""
        stamp_path = os.path.join(self.temp_dir, ".influxdb3-launcher")
        self.launcher.write_stamp(stamp_path, "core")

        f = io.StringIO()

        old_stdout = sys.stdout
        try:
            sys.stdout = f
            self.launcher.check_flavor_migration(stamp_path, "enterprise")
        finally:
            sys.stdout = old_stdout

        output = f.getvalue()
        # Check for exact message format
        self.assertIn("I: Detected previous run of InfluxDB 3 Core", output)
        self.assertIn(stamp_path, output)

        # Stamp should NOT be updated (user must manually remove it)
        result = self.launcher.read_stamp(stamp_path)
        self.assertEqual(result, "core")

    def test_check_flavor_migration_enterprise_to_core_fails(self):
        """Test downgrade from enterprise to core fails with error"""
        stamp_path = os.path.join(self.temp_dir, ".influxdb3-launcher")
        self.launcher.write_stamp(stamp_path, "enterprise")

        # Capture stderr to check error message
        f = io.StringIO()
        old_stderr = sys.stderr
        try:
            sys.stderr = f
            with self.assertRaises(SystemExit) as cm:
                self.launcher.check_flavor_migration(stamp_path, "core")
        finally:
            sys.stderr = old_stderr

        self.assertEqual(cm.exception.code, 1)
        stderr_output = f.getvalue()
        self.assertIn(
            "E: Cannot downgrade from InfluxDB 3 Enterprise to Core", stderr_output
        )


class TestGetPlatformId(unittest.TestCase):
    """Tests for get_platform_id() function"""

    def setUp(self):
        self.launcher = load_launcher_module()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _write_os_release(self, content: str) -> str:
        """Helper to write a mock os-release file"""
        os_release_path = os.path.join(self.temp_dir, "os-release")
        with open(os_release_path, "w") as f:
            f.write(content)
        return os_release_path

    def test_file_not_exists(self):
        """Test returns None when os-release file doesn't exist"""
        result = self.launcher.get_platform_id("/nonexistent/os-release")
        self.assertIsNone(result)

    def test_no_platform_id(self):
        """Test returns None when PLATFORM_ID line is missing"""
        os_release = self._write_os_release(
            'NAME="Red Hat Enterprise Linux"\n' 'VERSION_ID="8.9"\n'
        )
        result = self.launcher.get_platform_id(os_release)
        self.assertIsNone(result)

    def test_platform_id_double_quotes(self):
        """Test parses PLATFORM_ID with double quotes"""
        os_release = self._write_os_release(
            'NAME="Red Hat Enterprise Linux"\n' 'PLATFORM_ID="platform:el8"\n'
        )
        result = self.launcher.get_platform_id(os_release)
        self.assertEqual(result, "platform:el8")

    def test_platform_id_single_quotes(self):
        """Test parses PLATFORM_ID with single quotes"""
        os_release = self._write_os_release(
            "NAME='Red Hat Enterprise Linux'\n" "PLATFORM_ID='platform:el8'\n"
        )
        result = self.launcher.get_platform_id(os_release)
        self.assertEqual(result, "platform:el8")

    def test_platform_id_no_quotes(self):
        """Test parses PLATFORM_ID without quotes"""
        os_release = self._write_os_release("NAME=RHEL\n" "PLATFORM_ID=platform:el8\n")
        result = self.launcher.get_platform_id(os_release)
        self.assertEqual(result, "platform:el8")

    def test_rhel7(self):
        """Test detects RHEL7 platform"""
        os_release = self._write_os_release('PLATFORM_ID="platform:el7"\n')
        result = self.launcher.get_platform_id(os_release)
        self.assertEqual(result, "platform:el7")

    def test_oracle_linux_7(self):
        """Test detects Oracle Linux 7 platform"""
        os_release = self._write_os_release('PLATFORM_ID="platform:ol7"\n')
        result = self.launcher.get_platform_id(os_release)
        self.assertEqual(result, "platform:ol7")

    def test_oracle_linux_8(self):
        """Test detects Oracle Linux 8 platform"""
        os_release = self._write_os_release('PLATFORM_ID="platform:ol8"\n')
        result = self.launcher.get_platform_id(os_release)
        self.assertEqual(result, "platform:ol8")

    def test_rhel9_not_affected(self):
        """Test detects RHEL9 platform (not in affected list)"""
        os_release = self._write_os_release('PLATFORM_ID="platform:el9"\n')
        result = self.launcher.get_platform_id(os_release)
        self.assertEqual(result, "platform:el9")

    def test_unreadable_file(self):
        """Test returns None on file read error"""
        os_release = self._write_os_release('PLATFORM_ID="platform:el8"\n')
        os.chmod(os_release, 0o000)
        try:
            result = self.launcher.get_platform_id(os_release)
            # Depending on whether we're root, this may or may not raise
            # Just ensure we don't crash
            self.assertTrue(result is None or result == "platform:el8")
        finally:
            os.chmod(os_release, 0o644)


class TestGetSslCertEnv(unittest.TestCase):
    """Tests for get_ssl_cert_env() function"""

    def setUp(self):
        self.launcher = load_launcher_module()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _write_os_release(self, content: str) -> str:
        """Helper to write a mock os-release file"""
        os_release_path = os.path.join(self.temp_dir, "os-release")
        with open(os_release_path, "w") as f:
            f.write(content)
        return os_release_path

    def _create_cert_bundle(self) -> str:
        """Helper to create a mock certificate bundle file"""
        cert_path = os.path.join(self.temp_dir, "ca-bundle.crt")
        with open(cert_path, "w") as f:
            f.write("# Mock certificate bundle\n")
        return cert_path

    def test_returns_ssl_cert_file_on_rhel8(self):
        """Test returns SSL_CERT_FILE on RHEL8 when conditions are met"""
        os_release = self._write_os_release('PLATFORM_ID="platform:el8"\n')
        cert_bundle = self._create_cert_bundle()

        result = self.launcher.get_ssl_cert_env(os_release, cert_bundle)

        self.assertEqual(result, {"SSL_CERT_FILE": cert_bundle})

    def test_returns_ssl_cert_file_on_rhel7(self):
        """Test returns SSL_CERT_FILE on RHEL7"""
        os_release = self._write_os_release('PLATFORM_ID="platform:el7"\n')
        cert_bundle = self._create_cert_bundle()

        result = self.launcher.get_ssl_cert_env(os_release, cert_bundle)

        self.assertEqual(result, {"SSL_CERT_FILE": cert_bundle})

    def test_returns_ssl_cert_file_on_oracle_linux_8(self):
        """Test returns SSL_CERT_FILE on Oracle Linux 8"""
        os_release = self._write_os_release('PLATFORM_ID="platform:ol8"\n')
        cert_bundle = self._create_cert_bundle()

        result = self.launcher.get_ssl_cert_env(os_release, cert_bundle)

        self.assertEqual(result, {"SSL_CERT_FILE": cert_bundle})

    def test_returns_ssl_cert_file_on_oracle_linux_7(self):
        """Test returns SSL_CERT_FILE on Oracle Linux 7"""
        os_release = self._write_os_release('PLATFORM_ID="platform:ol7"\n')
        cert_bundle = self._create_cert_bundle()

        result = self.launcher.get_ssl_cert_env(os_release, cert_bundle)

        self.assertEqual(result, {"SSL_CERT_FILE": cert_bundle})

    def test_returns_empty_on_rhel9(self):
        """Test returns empty dict on RHEL9 (not affected)"""
        os_release = self._write_os_release('PLATFORM_ID="platform:el9"\n')
        cert_bundle = self._create_cert_bundle()

        result = self.launcher.get_ssl_cert_env(os_release, cert_bundle)

        self.assertEqual(result, {})

    def test_returns_empty_on_fedora(self):
        """Test returns empty dict on Fedora"""
        os_release = self._write_os_release('PLATFORM_ID="platform:f39"\n')
        cert_bundle = self._create_cert_bundle()

        result = self.launcher.get_ssl_cert_env(os_release, cert_bundle)

        self.assertEqual(result, {})

    def test_returns_empty_when_no_os_release(self):
        """Test returns empty dict when os-release doesn't exist"""
        cert_bundle = self._create_cert_bundle()

        result = self.launcher.get_ssl_cert_env("/nonexistent/os-release", cert_bundle)

        self.assertEqual(result, {})

    def test_returns_empty_when_cert_bundle_missing(self):
        """Test returns empty dict when cert bundle doesn't exist"""
        os_release = self._write_os_release('PLATFORM_ID="platform:el8"\n')

        result = self.launcher.get_ssl_cert_env(
            os_release, "/nonexistent/ca-bundle.crt"
        )

        self.assertEqual(result, {})

    def test_returns_empty_when_ssl_cert_file_already_set(self):
        """Test returns empty dict when SSL_CERT_FILE is already set"""
        os_release = self._write_os_release('PLATFORM_ID="platform:el8"\n')
        cert_bundle = self._create_cert_bundle()

        original_value = os.environ.get("SSL_CERT_FILE")
        try:
            os.environ["SSL_CERT_FILE"] = "/custom/path/to/certs.pem"
            result = self.launcher.get_ssl_cert_env(os_release, cert_bundle)
            self.assertEqual(result, {})
        finally:
            if original_value is None:
                os.environ.pop("SSL_CERT_FILE", None)
            else:
                os.environ["SSL_CERT_FILE"] = original_value

    def test_affected_platforms_constant(self):
        """Test that SSL_CERT_AFFECTED_PLATFORMS contains expected values"""
        expected = frozenset(
            ["platform:el7", "platform:el8", "platform:ol7", "platform:ol8"]
        )
        self.assertEqual(self.launcher.SSL_CERT_AFFECTED_PLATFORMS, expected)

    def test_cert_bundle_path_constant(self):
        """Test that SSL_CERT_BUNDLE_PATH is set correctly"""
        self.assertEqual(
            self.launcher.SSL_CERT_BUNDLE_PATH, "/etc/pki/tls/certs/ca-bundle.crt"
        )


class TestLauncherIntegration(unittest.TestCase):
    """Integration tests that run the launcher with mock executables"""

    def setUp(self):
        assert LAUNCHER_PATH is not None, "LAUNCHER_PATH not set"
        self.launcher_path: str = LAUNCHER_PATH
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _write_toml_config(self, content):
        """Helper to write a TOML config file and return its path"""
        config_path = os.path.join(self.temp_dir, "test.toml")
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(content)
        return config_path

    def _create_mock_executable(self, script_content):
        """Helper to create a mock executable script with custom content"""
        # Create the Python script
        script_name = f"mock_{id(script_content)}"
        script_path = os.path.join(self.temp_dir, f"{script_name}.py")
        with open(script_path, "w") as f:
            f.write(script_content)

        # Create platform-appropriate executable wrapper
        if os.name == "nt":  # Windows
            exec_path = os.path.join(self.temp_dir, f"{script_name}.bat")  # type: ignore[unreachable]
            with open(exec_path, "w") as f:
                f.write(f'@echo off\n"{sys.executable}" "{script_path}" %*\n')
        else:  # Unix-like (Linux, macOS)
            exec_path = os.path.join(self.temp_dir, script_name)  # type: ignore[unreachable]
            with open(exec_path, "w") as f:
                f.write(f"#!/bin/sh\n")
                f.write(f'exec "{sys.executable}" "{script_path}" "$@"\n')
            os.chmod(exec_path, 0o755)

        return exec_path

    def test_environment_from_config(self):
        """Test that environment variables from TOML config are passed to executed process"""
        config_path = self._write_toml_config(
            'object-store = "file"\n' 'node-id = "test-node-123"\n'
        )

        # Create a mock executable that prints an env var
        mock_exec = self._create_mock_executable(
            "import os\n"
            'print(f\'NODE_ID={os.environ.get("INFLUXDB3_NODE_IDENTIFIER_PREFIX", "NOT_SET")}\')\n'
        )

        result = subprocess.run(
            [
                sys.executable,
                str(self.launcher_path),
                "--flavor",
                "core",
                "--exec",
                mock_exec,
                "--config-toml",
                config_path,
                "--stamp-dir",
                self.temp_dir,
                "--",
            ],
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 0)
        self.assertIn("NODE_ID=test-node-123", result.stdout)

    def test_enterprise_license_file_valid(self):
        """Test that Enterprise with object-store and license-file works"""
        config_path = self._write_toml_config(
            'object-store = "file"\n' 'license-file = "/path/to/license.jwt"\n'
        )

        # Create a simple mock executable
        mock_exec = self._create_mock_executable("print('Enterprise running')\n")

        result = subprocess.run(
            [
                sys.executable,
                str(self.launcher_path),
                "--flavor",
                "enterprise",
                "--exec",
                mock_exec,
                "--config-toml",
                config_path,
                "--stamp-dir",
                self.temp_dir,
                "--",
            ],
            capture_output=True,
            text=True,
        )

        # Should succeed with license file
        self.assertEqual(result.returncode, 0)
        self.assertIn("Enterprise running", result.stdout)

    def test_core_with_object_store(self):
        """Test that Core works with just object-store"""
        config_path = self._write_toml_config('object-store = "file"\n')

        # Create a simple mock executable
        mock_exec = self._create_mock_executable("print('Core running')\n")

        result = subprocess.run(
            [
                sys.executable,
                str(self.launcher_path),
                "--flavor",
                "core",
                "--exec",
                mock_exec,
                "--config-toml",
                config_path,
                "--stamp-dir",
                self.temp_dir,
                "--",
            ],
            capture_output=True,
            text=True,
        )

        # Should succeed with just object-store for Core
        self.assertEqual(result.returncode, 0)
        self.assertIn("Core running", result.stdout)

    @unittest.skipIf(os.name == "nt", "Daemonize not available on Windows")
    def test_daemonize_integration(self):
        """Integration test for --daemonize with actual forking"""
        config_path = self._write_toml_config('object-store = "file"\n')
        pidfile = os.path.join(self.temp_dir, "daemon.pid")
        log_file = os.path.join(self.temp_dir, "daemon.log")
        marker_file = os.path.join(self.temp_dir, "daemon.marker")

        # Create a mock executable that writes to log and marker file
        mock_exec = self._create_mock_executable(
            f"import time\n"
            f"print('Daemon started')\n"
            f"with open('{marker_file}', 'w') as f:\n"
            f"    f.write('daemon_ran')\n"
        )

        result = subprocess.run(
            [
                sys.executable,
                str(self.launcher_path),
                "--flavor",
                "core",
                "--exec",
                mock_exec,
                "--config-toml",
                config_path,
                "--stamp-dir",
                self.temp_dir,
                "--daemonize",
                "--pidfile",
                pidfile,
                "--log-file",
                log_file,
                "--",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )

        # Parent should exit successfully
        self.assertEqual(result.returncode, 0)

        # Wait for daemon to run
        for _ in range(20):
            if os.path.exists(marker_file):
                break
            time.sleep(0.1)

        # Check pidfile was created
        self.assertTrue(os.path.exists(pidfile))

        # Check marker file was created (daemon ran)
        self.assertTrue(os.path.exists(marker_file))

        # Check log file has output
        if os.path.exists(log_file):
            with open(log_file) as f:
                log_content = f.read()
            self.assertIn("Daemon started", log_content)

        # Clean up daemon if still running
        if os.path.exists(pidfile):
            with open(pidfile) as f:
                daemon_pid = int(f.read().strip())
            try:
                os.kill(daemon_pid, 9)
            except ProcessLookupError:
                pass  # Already exited


def main():
    """Main test runner"""
    parser = argparse.ArgumentParser(description="Test influxdb3-launcher")
    parser.add_argument(
        "launcher_path", help="Path to the influxdb3-launcher script to test"
    )
    args = parser.parse_args()

    global LAUNCHER_PATH
    LAUNCHER_PATH = os.path.abspath(args.launcher_path)

    if not os.path.isfile(LAUNCHER_PATH):  # pragma: nocover
        print(f"Error: {LAUNCHER_PATH} is not a file", file=sys.stderr)
        sys.exit(1)

    # Run the tests
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
