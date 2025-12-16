"""
InfluxDB 3 Sandbox Verification Request Plugin

This plugin verifies that systemd sandbox controls are active and expected
plugin functionality works correctly. It returns a JSON object with test
results and HTTP 200 if all pass, or HTTP 500 if any fail.

Result types:
- PASS: Test passed as expected
- EXFAIL: Expected failure (security control working)
- EXPASS: Extra pass (security stricter than required)
- FAIL: Unexpected result
- SKIP: Test skipped (wrong platform, module not installed)

Installation:
  1. Copy this file to the plugin directory (e.g., /var/lib/influxdb3/plugins/)

  2. Install optional dependencies in the processing engine venv:
     $ curl -X POST "http://localhost:8181/api/v3/configure/plugin_environment/install_packages" \
         -H "Content-Type: application/json" \
         -d '{"packages": ["requests"]}'

  3. Create a database (if not exists):
     $ curl -X POST "http://localhost:8181/api/v3/configure/database" \
         -H "Content-Type: application/json" \
         -d '{"db": "mydb"}'

  4. Enable the request trigger:
     $ curl -X POST "http://localhost:8181/api/v3/configure/processing_engine_trigger" \
         -H "Content-Type: application/json" \
         -d '{
           "db": "mydb",
           "plugin_filename": "sandbox_verify.py",
           "trigger_name": "sandbox_verify",
           "trigger_specification": "request:sandbox_verify",
           "trigger_settings": {"run_async": false, "error_behavior": "log"},
           "trigger_arguments": null,
           "disabled": false
         }'

  5. Invoke the plugin:
     $ curl -s "http://localhost:8181/api/v3/engine/sandbox_verify" | jq .

     Or with pretty output and status check:
     $ curl -sw '\nHTTP %{http_code}\n' "http://localhost:8181/api/v3/engine/sandbox_verify" | jq .
"""

from __future__ import annotations

import ctypes
import errno
import json
import mmap
import os
import socket
import ssl
import stat
import sys
from typing import Any, Optional

# Platform detection
IS_LINUX = sys.platform.startswith("linux")
IS_MACOS = sys.platform == "darwin"
IS_UNIX = IS_LINUX or IS_MACOS


def _syscall_nr(name: str) -> Optional[int]:
    """Return architecture-appropriate syscall numbers for common calls."""
    arch = os.uname().machine
    # x86_64 numbers differ from asm-generic; aarch64/arm64 use asm-generic.
    x86_64 = {
        "bpf": 321,
        "perf_event_open": 298,
        "userfaultfd": 323,
        "io_uring_setup": 425,
        "keyctl": 250,
    }
    generic = {
        "bpf": 280,
        "perf_event_open": 241,
        "userfaultfd": 282,
        "io_uring_setup": 425,
        "keyctl": 219,
    }
    if arch in ("x86_64", "amd64"):
        return x86_64.get(name)
    # Treat everything else as asm-generic (covers aarch64/arm64).
    return generic.get(name)

# Result constants
PASS = "PASS"
EXFAIL = "EXFAIL"
EXPASS = "EXPASS"
FAIL = "FAIL"
SKIP = "SKIP"


# =============================================================================
# Platform skip helpers
# =============================================================================


def _skip_if_not_linux(test_name: str, results: dict[str, str]) -> bool:
    """Return True if test should be skipped (not Linux)."""
    if not IS_LINUX:
        results[test_name] = SKIP
        return True
    return False


def _skip_if_not_unix(test_name: str, results: dict[str, str]) -> bool:
    """Return True if test should be skipped (not UNIX)."""
    if not IS_UNIX:
        results[test_name] = SKIP
        return True
    return False


# =============================================================================
# Sandbox Protection Tests
# =============================================================================


def test_User_identity(results: dict[str, str]) -> None:
    """Verify running as influxdb3 user."""
    if _skip_if_not_unix("User_identity", results):
        return
    try:
        import pwd

        username = pwd.getpwuid(os.geteuid()).pw_name
        if username == "influxdb3":
            results["User_identity"] = PASS
        else:
            results["User_identity"] = FAIL
    except Exception:
        results["User_identity"] = FAIL


def test_Group_identity(results: dict[str, str]) -> None:
    """Verify running as influxdb3 group."""
    if _skip_if_not_unix("Group_identity", results):
        return
    try:
        import grp

        groupname = grp.getgrgid(os.getegid()).gr_name
        if groupname == "influxdb3":
            results["Group_identity"] = PASS
        else:
            results["Group_identity"] = FAIL
    except Exception:
        results["Group_identity"] = FAIL


def test_SupplementaryGroups_identity(results: dict[str, str]) -> None:
    """Verify only influxdb3 in groups list."""
    if _skip_if_not_unix("SupplementaryGroups_identity", results):
        return
    try:
        import grp

        groups = [grp.getgrgid(g).gr_name for g in os.getgroups()]
        # Should only contain influxdb3 (or be empty with just primary group)
        non_influxdb3 = [g for g in groups if g != "influxdb3"]
        if len(non_influxdb3) == 0:
            results["SupplementaryGroups_identity"] = PASS
        else:
            results["SupplementaryGroups_identity"] = FAIL
    except Exception:
        results["SupplementaryGroups_identity"] = FAIL


def test_Umask_filePerms(results: dict[str, str]) -> None:
    """Verify files created with 0640 (UMask=0027)."""
    if _skip_if_not_unix("Umask_filePerms", results):
        return
    test_file = f"/tmp/sandbox_umask_test_{os.getpid()}"
    try:
        # Create file with mode 0o666, umask should reduce to 0o640
        fd = os.open(test_file, os.O_CREAT | os.O_WRONLY, 0o666)
        st = os.fstat(fd)
        os.close(fd)
        os.unlink(test_file)
        actual_mode = stat.S_IMODE(st.st_mode)
        if actual_mode == 0o640:
            results["Umask_filePerms"] = PASS
        else:
            results["Umask_filePerms"] = FAIL
    except Exception:
        results["Umask_filePerms"] = FAIL
        try:
            os.unlink(test_file)
        except OSError:
            pass


def test_ProtectHome_home_neg(results: dict[str, str]) -> None:
    """Verify cannot access /home (ProtectHome=true)."""
    if _skip_if_not_linux("ProtectHome_home_neg", results):
        return
    try:
        os.listdir("/home")
        results["ProtectHome_home_neg"] = FAIL
    except PermissionError:
        results["ProtectHome_home_neg"] = EXFAIL
    except FileNotFoundError:
        # /home doesn't exist - also acceptable
        results["ProtectHome_home_neg"] = EXFAIL
    except Exception:
        results["ProtectHome_home_neg"] = FAIL


def test_ProtectSystem_etc_influxdb3_read(results: dict[str, str]) -> None:
    """Verify can read /etc/influxdb3."""
    if _skip_if_not_linux("ProtectSystem_etc_influxdb3_read", results):
        return
    try:
        if os.path.isdir("/etc/influxdb3") and os.access("/etc/influxdb3", os.R_OK):
            results["ProtectSystem_etc_influxdb3_read"] = PASS
        elif not os.path.exists("/etc/influxdb3"):
            # Directory doesn't exist yet - SKIP
            results["ProtectSystem_etc_influxdb3_read"] = SKIP
        else:
            results["ProtectSystem_etc_influxdb3_read"] = FAIL
    except Exception:
        results["ProtectSystem_etc_influxdb3_read"] = FAIL


def test_ProtectSystem_etc_influxdb3_write_neg(results: dict[str, str]) -> None:
    """Verify cannot write to /etc/influxdb3."""
    if _skip_if_not_linux("ProtectSystem_etc_influxdb3_write_neg", results):
        return
    if not os.path.exists("/etc/influxdb3"):
        results["ProtectSystem_etc_influxdb3_write_neg"] = SKIP
        return
    test_file = f"/etc/influxdb3/sandbox_test_{os.getpid()}"
    try:
        with open(test_file, "w") as f:
            f.write("test")
        os.unlink(test_file)
        results["ProtectSystem_etc_influxdb3_write_neg"] = FAIL
    except (PermissionError, OSError):
        results["ProtectSystem_etc_influxdb3_write_neg"] = EXFAIL
    except Exception:
        results["ProtectSystem_etc_influxdb3_write_neg"] = FAIL


def test_ProtectKernelLogs_neg(results: dict[str, str]) -> None:
    """Verify cannot read /dev/kmsg or /proc/kmsg."""
    if _skip_if_not_linux("ProtectKernelLogs_neg", results):
        return
    blocked = 0
    klogs_paths = ["/dev/kmsg", "/proc/kmsg"]
    for path in klogs_paths:
        try:
            with open(path, "r") as f:
                f.read(1)
            # If we get here, it's not blocked
        except OSError:
            # Catches PermissionError, FileNotFoundError, and other OS errors
            blocked += 1
    if blocked == len(klogs_paths):
        results["ProtectKernelLogs_neg"] = EXFAIL
    else:
        results["ProtectKernelLogs_neg"] = FAIL


def test_ProtectProc_invisible(results: dict[str, str]) -> None:
    """Verify other users' processes hidden in /proc."""
    if _skip_if_not_linux("ProtectProc_invisible", results):
        return
    try:
        euid = os.geteuid()
        found_other = False
        for entry in os.listdir("/proc"):
            if entry.isdigit():
                try:
                    with open(f"/proc/{entry}/status") as f:
                        for line in f:
                            if line.startswith("Uid:"):
                                uid = int(line.split()[1])
                                if uid != euid:
                                    found_other = True
                                    break
                except (PermissionError, FileNotFoundError, OSError):
                    pass  # Expected - can't read other processes
            if found_other:
                break
        if found_other:
            results["ProtectProc_invisible"] = FAIL
        else:
            results["ProtectProc_invisible"] = PASS
    except Exception:
        results["ProtectProc_invisible"] = FAIL


def test_InaccessiblePaths_dbus_neg(results: dict[str, str]) -> None:
    """Verify /run/dbus/system_bus_socket blocked."""
    if _skip_if_not_linux("InaccessiblePaths_dbus_neg", results):
        return
    path = "/run/dbus/system_bus_socket"
    try:
        # Try to connect to the socket - this tests actual access, not just existence
        # InaccessiblePaths may chmod 0000 instead of hiding the path
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(path)
        s.close()
        results["InaccessiblePaths_dbus_neg"] = FAIL  # Should not be able to connect
    except PermissionError:
        results["InaccessiblePaths_dbus_neg"] = EXFAIL  # Expected - blocked
    except FileNotFoundError:
        results["InaccessiblePaths_dbus_neg"] = EXFAIL  # Also acceptable - path hidden
    except OSError:
        results["InaccessiblePaths_dbus_neg"] = (
            EXFAIL  # Other OS errors (connection refused, etc)
        )
    except Exception:
        results["InaccessiblePaths_dbus_neg"] = FAIL


def test_InaccessiblePaths_runUser_neg(results: dict[str, str]) -> None:
    """Verify /run/user blocked."""
    if _skip_if_not_linux("InaccessiblePaths_runUser_neg", results):
        return
    path = "/run/user"
    try:
        # Try to list the directory - this tests actual access, not just existence
        # InaccessiblePaths may chmod 0000 instead of hiding the path
        os.listdir(path)
        results["InaccessiblePaths_runUser_neg"] = FAIL  # Should not be able to list
    except PermissionError:
        results["InaccessiblePaths_runUser_neg"] = EXFAIL  # Expected - blocked
    except FileNotFoundError:
        results["InaccessiblePaths_runUser_neg"] = (
            EXFAIL  # Also acceptable - path hidden
        )
    except OSError:
        results["InaccessiblePaths_runUser_neg"] = EXFAIL  # Other OS errors
    except Exception:
        results["InaccessiblePaths_runUser_neg"] = FAIL


def test_RestrictAddressFamilies_netlink_neg(results: dict[str, str]) -> None:
    """Verify AF_NETLINK blocked."""
    if _skip_if_not_linux("RestrictAddressFamilies_netlink_neg", results):
        return
    try:
        AF_NETLINK = 16  # Linux-specific
        s = socket.socket(AF_NETLINK, socket.SOCK_RAW)
        s.close()
        results["RestrictAddressFamilies_netlink_neg"] = FAIL
    except (PermissionError, OSError) as e:
        if isinstance(e, OSError) and e.errno in (errno.EAFNOSUPPORT, errno.EPERM):
            results["RestrictAddressFamilies_netlink_neg"] = EXFAIL
        elif isinstance(e, PermissionError):
            results["RestrictAddressFamilies_netlink_neg"] = EXFAIL
        else:
            results["RestrictAddressFamilies_netlink_neg"] = FAIL
    except Exception:
        results["RestrictAddressFamilies_netlink_neg"] = FAIL


def test_RestrictAddressFamilies_packet_neg(results: dict[str, str]) -> None:
    """Verify AF_PACKET blocked."""
    if _skip_if_not_linux("RestrictAddressFamilies_packet_neg", results):
        return
    try:
        AF_PACKET = 17  # Linux-specific
        s = socket.socket(AF_PACKET, socket.SOCK_RAW)
        s.close()
        results["RestrictAddressFamilies_packet_neg"] = FAIL
    except (PermissionError, OSError) as e:
        if isinstance(e, OSError) and e.errno in (errno.EAFNOSUPPORT, errno.EPERM):
            results["RestrictAddressFamilies_packet_neg"] = EXFAIL
        elif isinstance(e, PermissionError):
            results["RestrictAddressFamilies_packet_neg"] = EXFAIL
        else:
            results["RestrictAddressFamilies_packet_neg"] = FAIL
    except Exception:
        results["RestrictAddressFamilies_packet_neg"] = FAIL


def test_UserFaultFd_neg(results: dict[str, str]) -> None:
    """Verify userfaultfd syscall blocked."""
    if _skip_if_not_linux("UserFaultFd_neg", results):
        return
    try:
        libc = ctypes.CDLL(None)
        SYS_userfaultfd = _syscall_nr("userfaultfd")
        if SYS_userfaultfd is None:
            results["UserFaultFd_neg"] = SKIP
            return
        # Use flags=0 so kernel will deny when vm.unprivileged_userfaultfd=0 or seccomp blocks.
        res = libc.syscall(SYS_userfaultfd, 0)
        if res == -1:
            # Any failure is acceptable here: EPERM/ENOSYS when blocked by seccomp/sysctl,
            # EINVAL on older kernels that don't support this flag, etc.
            results["UserFaultFd_neg"] = EXFAIL
        else:
            os.close(res)
            results["UserFaultFd_neg"] = FAIL
    except Exception:
        results["UserFaultFd_neg"] = EXFAIL


def test_IoUringSetup_neg(results: dict[str, str]) -> None:
    """Verify io_uring_setup syscall blocked."""
    if _skip_if_not_linux("IoUringSetup_neg", results):
        return
    try:
        libc = ctypes.CDLL(None)
        SYS_io_uring_setup = _syscall_nr("io_uring_setup")
        if SYS_io_uring_setup is None:
            results["IoUringSetup_neg"] = SKIP
            return

        class IoUringParams(ctypes.Structure):
            _fields_ = [
                ("sq_entries", ctypes.c_uint32),
                ("cq_entries", ctypes.c_uint32),
                ("flags", ctypes.c_uint32),
                ("sq_thread_cpu", ctypes.c_uint32),
                ("sq_thread_idle", ctypes.c_uint32),
                ("features", ctypes.c_uint32),
                ("wq_fd", ctypes.c_uint32),
                ("reserved", ctypes.c_uint32 * 3),
                ("sq_off", ctypes.c_uint64 * 6),
                ("cq_off", ctypes.c_uint64 * 6),
            ]

        params = IoUringParams()
        params.sq_entries = 2
        params.cq_entries = 2
        res = libc.syscall(SYS_io_uring_setup, params.sq_entries, ctypes.byref(params))
        if res == -1:
            # Any failure indicates the syscall was blocked (seccomp/ENOSYS/EPERM/etc.)
            results["IoUringSetup_neg"] = EXFAIL
        else:
            os.close(res)
            results["IoUringSetup_neg"] = FAIL
    except Exception:
        results["IoUringSetup_neg"] = EXFAIL


def test_Bpf_neg(results: dict[str, str]) -> None:
    """Verify bpf syscall blocked."""
    if _skip_if_not_linux("Bpf_neg", results):
        return
    try:
        # If unprivileged BPF is globally disabled, this will always fail; skip in that case.
        sysctl_path = "/proc/sys/kernel/unprivileged_bpf_disabled"
        if os.path.exists(sysctl_path):
            with open(sysctl_path) as f:
                val = f.read().strip()
            if val and val != "0":
                results["Bpf_neg"] = SKIP
                return

        libc = ctypes.CDLL(None, use_errno=True)
        SYS_bpf = _syscall_nr("bpf")
        if SYS_bpf is None:
            results["Bpf_neg"] = SKIP
            return
        BPF_MAP_GET_NEXT_ID = 5
        attr = ctypes.create_string_buffer(
            8
        )  # struct { __u32 start_id; __u32 next_id; }
        res = libc.syscall(SYS_bpf, BPF_MAP_GET_NEXT_ID, ctypes.byref(attr), len(attr))
        if res == -1:
            err = ctypes.get_errno()
            if err in (errno.EPERM, errno.EACCES, errno.ENOSYS):
                results["Bpf_neg"] = EXFAIL  # blocked by seccomp
            else:
                # syscall reached kernel; treat as not blocked
                results["Bpf_neg"] = FAIL
        else:
            results["Bpf_neg"] = FAIL  # succeeded => not blocked
    except Exception:
        results["Bpf_neg"] = EXFAIL


def test_PerfEventOpen_neg(results: dict[str, str]) -> None:
    """Verify perf_event_open blocked."""
    if _skip_if_not_linux("PerfEventOpen_neg", results):
        return
    try:
        # If the kernel has unprivileged perf disabled (perf_event_paranoid > 2), unprivileged
        # calls will fail even without seccomp. Detect and skip in that case.
        paranoid_path = "/proc/sys/kernel/perf_event_paranoid"
        if os.path.exists(paranoid_path):
            with open(paranoid_path) as f:
                val = f.read().strip()
            try:
                if int(val) > 2:
                    results["PerfEventOpen_neg"] = SKIP
                    return
            except ValueError:
                pass

        libc = ctypes.CDLL(None, use_errno=True)
        SYS_perf_event_open = _syscall_nr("perf_event_open")
        if SYS_perf_event_open is None:
            results["PerfEventOpen_neg"] = SKIP
            return
        # Build a minimal valid perf_event_attr for a software counter (task clock)
        PERF_TYPE_SOFTWARE = 1
        PERF_COUNT_SW_TASK_CLOCK = 1
        attr_size = 112  # sizeof(struct perf_event_attr) on current kernels
        buf = ctypes.create_string_buffer(attr_size)
        ctypes.c_uint32.from_buffer(buf, 0).value = PERF_TYPE_SOFTWARE  # type
        ctypes.c_uint32.from_buffer(buf, 4).value = attr_size  # size
        ctypes.c_uint64.from_buffer(buf, 8).value = PERF_COUNT_SW_TASK_CLOCK  # config
        res = libc.syscall(SYS_perf_event_open, ctypes.byref(buf), 0, -1, -1, 0)
        if res == -1:
            err = ctypes.get_errno()
            if err in (errno.EPERM, errno.EACCES, errno.ENOSYS):
                results["PerfEventOpen_neg"] = PASS  # blocked as intended by seccomp
            else:
                results["PerfEventOpen_neg"] = FAIL  # other error: treat as not blocked
        else:
            os.close(res)
            results["PerfEventOpen_neg"] = FAIL  # succeeded => not blocked
    except Exception:
        results["PerfEventOpen_neg"] = EXFAIL


def test_Keyctl_neg(results: dict[str, str]) -> None:
    """Verify keyctl blocked."""
    if _skip_if_not_linux("Keyctl_neg", results):
        return
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        SYS_keyctl = _syscall_nr("keyctl")
        if SYS_keyctl is None:
            results["Keyctl_neg"] = SKIP
            return
        KEYCTL_GET_KEYRING_ID = 0
        KEY_SPEC_THREAD_KEYRING = ctypes.c_long(-1)  # request thread keyring
        # Ask for the thread keyring ID without creating; should succeed if allowed
        res = libc.syscall(
            SYS_keyctl, KEYCTL_GET_KEYRING_ID, KEY_SPEC_THREAD_KEYRING, 0, 0, 0
        )
        if res == -1 and ctypes.get_errno() in (
            errno.EPERM,
            errno.EACCES,
            errno.ENOSYS,
        ):
            results["Keyctl_neg"] = EXFAIL  # blocked as expected
        elif res >= 0:
            # Syscall succeeded -> not blocked
            results["Keyctl_neg"] = FAIL
        else:
            # Other errors (e.g., EINVAL) mean the call reached the kernel; treat as not blocked
            results["Keyctl_neg"] = FAIL
    except Exception:
        results["Keyctl_neg"] = EXFAIL


def test_CapBoundingSet(results: dict[str, str]) -> None:
    """Verify common caps are absent from bounding set."""
    if _skip_if_not_linux("CapBoundingSet", results):
        return
    try:
        libc = ctypes.CDLL(None)
        libc_errno = ctypes.get_errno
        PR_CAPBSET_READ = 23
        libc.prctl.restype = ctypes.c_int
        libc.prctl.argtypes = [
            ctypes.c_int,
            ctypes.c_ulong,
            ctypes.c_ulong,
            ctypes.c_ulong,
            ctypes.c_ulong,
        ]
        for cap in (CAP_NET_ADMIN := 12, CAP_SYS_ADMIN := 21, CAP_SYS_PTRACE := 19):
            res = libc.prctl(PR_CAPBSET_READ, cap, 0, 0, 0)
            if res == 1:
                results["CapBoundingSet"] = FAIL
                return
            if res == -1 and libc_errno() not in (errno.EINVAL, errno.EPERM):
                results["CapBoundingSet"] = FAIL
                return
        results["CapBoundingSet"] = PASS
    except Exception:
        results["CapBoundingSet"] = FAIL


def test_AmbientCapabilities(results: dict[str, str]) -> None:
    """Verify no capabilities are present in the ambient set."""
    if _skip_if_not_linux("AmbientCapabilities", results):
        return
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        PR_CAP_AMBIENT = 47
        PR_CAP_AMBIENT_IS_SET = 1

        cap_last_path = "/proc/sys/kernel/cap_last_cap"
        try:
            with open(cap_last_path) as f:
                cap_last = int(f.read().strip())
        except Exception:
            cap_last = 63  # reasonable upper bound if proc not readable

        first_err = None
        for cap in range(cap_last + 1):
            res = libc.prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_IS_SET, cap, 0, 0)
            if res == 1:
                results["AmbientCapabilities"] = FAIL
                return
            if res == -1 and first_err is None:
                first_err = ctypes.get_errno()
            if res == -1:
                err = ctypes.get_errno()
                if err in (errno.EINVAL, errno.ENOSYS):
                    # If the first capability reports not supported, assume kernel lacks ambient caps.
                    if cap == 0:
                        results["AmbientCapabilities"] = SKIP
                        return
                    # Otherwise treat as failure for that specific cap.
                    results["AmbientCapabilities"] = FAIL
                    return
                else:
                    results["AmbientCapabilities"] = FAIL
                    return
        results["AmbientCapabilities"] = PASS
    except Exception:
        results["AmbientCapabilities"] = FAIL


def test_IPAddressDeny_localhost(results: dict[str, str]) -> None:
    """Test if localhost (127.0.0.1:8181) is blocked by IPAddressDeny."""
    if _skip_if_not_linux("IPAddressDeny_localhost", results):
        return
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect(("127.0.0.1", 8181))
        s.close()
        results["IPAddressDeny_localhost"] = EXPASS  # Allowed (acceptable)
    except PermissionError:
        results["IPAddressDeny_localhost"] = PASS  # Blocked by IPAddressDeny
    except OSError as e:
        if e.errno == errno.EPERM:
            results["IPAddressDeny_localhost"] = PASS
        else:
            results["IPAddressDeny_localhost"] = EXPASS  # Other error, likely allowed
    except Exception:
        results["IPAddressDeny_localhost"] = EXPASS


def test_IPAddressDeny_localhost6(results: dict[str, str]) -> None:
    """Test if localhost ([::1]:8181) is blocked by IPAddressDeny."""
    if _skip_if_not_linux("IPAddressDeny_localhost6", results):
        return
    try:
        s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect(("::1", 8181))
        s.close()
        results["IPAddressDeny_localhost6"] = EXPASS
    except PermissionError:
        results["IPAddressDeny_localhost6"] = PASS
    except OSError as e:
        if e.errno == errno.EPERM:
            results["IPAddressDeny_localhost6"] = PASS
        else:
            results["IPAddressDeny_localhost6"] = EXPASS
    except Exception:
        results["IPAddressDeny_localhost6"] = EXPASS


def test_SystemCallFilter_ptrace_neg(results: dict[str, str]) -> None:
    """Verify ptrace syscall blocked."""
    if _skip_if_not_linux("SystemCallFilter_ptrace_neg", results):
        return
    try:
        libc = ctypes.CDLL(None)
        PTRACE_TRACEME = 0
        # Set return type and argument types
        libc.ptrace.restype = ctypes.c_long
        libc.ptrace.argtypes = [
            ctypes.c_long,
            ctypes.c_long,
            ctypes.c_void_p,
            ctypes.c_void_p,
        ]
        result = libc.ptrace(PTRACE_TRACEME, 0, None, None)
        if result == -1:
            # Check errno
            err = ctypes.get_errno()
            if err == errno.EPERM:
                results["SystemCallFilter_ptrace_neg"] = EXFAIL
            else:
                results["SystemCallFilter_ptrace_neg"] = EXFAIL  # Any error is fine
        else:
            results["SystemCallFilter_ptrace_neg"] = FAIL
    except Exception:
        results["SystemCallFilter_ptrace_neg"] = EXFAIL


def test_RestrictNamespaces_unshare_neg(results: dict[str, str]) -> None:
    """Verify unshare syscall blocked."""
    if _skip_if_not_linux("RestrictNamespaces_unshare_neg", results):
        return
    try:
        libc = ctypes.CDLL(None)
        CLONE_NEWUSER = 0x10000000
        libc.unshare.restype = ctypes.c_int
        libc.unshare.argtypes = [ctypes.c_int]
        result = libc.unshare(CLONE_NEWUSER)
        if result == -1:
            results["RestrictNamespaces_unshare_neg"] = EXFAIL
        else:
            results["RestrictNamespaces_unshare_neg"] = FAIL
    except Exception:
        results["RestrictNamespaces_unshare_neg"] = EXFAIL


def test_PrivateDevices_devMem_neg(results: dict[str, str]) -> None:
    """Verify /dev/mem inaccessible."""
    if _skip_if_not_linux("PrivateDevices_devMem_neg", results):
        return
    try:
        with open("/dev/mem", "rb") as f:
            f.read(1)
        results["PrivateDevices_devMem_neg"] = FAIL
    except (PermissionError, FileNotFoundError, OSError):
        results["PrivateDevices_devMem_neg"] = EXFAIL
    except Exception:
        results["PrivateDevices_devMem_neg"] = FAIL


def test_NoNewPrivileges(results: dict[str, str]) -> None:
    """Verify PR_GET_NO_NEW_PRIVS = 1."""
    if _skip_if_not_linux("NoNewPrivileges", results):
        return
    try:
        libc = ctypes.CDLL(None)
        PR_GET_NO_NEW_PRIVS = 39
        libc.prctl.restype = ctypes.c_int
        libc.prctl.argtypes = [
            ctypes.c_int,
            ctypes.c_ulong,
            ctypes.c_ulong,
            ctypes.c_ulong,
            ctypes.c_ulong,
        ]
        result = libc.prctl(PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0)
        if result == 1:
            results["NoNewPrivileges"] = PASS
        else:
            results["NoNewPrivileges"] = FAIL
    except Exception:
        results["NoNewPrivileges"] = FAIL


def test_LockPersonality_neg(results: dict[str, str]) -> None:
    """Verify cannot change personality."""
    if _skip_if_not_linux("LockPersonality_neg", results):
        return
    try:
        libc = ctypes.CDLL(None)
        # Try to set a different personality
        PER_LINUX32 = 0x0008  # Different from default
        libc.personality.restype = ctypes.c_int
        libc.personality.argtypes = [ctypes.c_ulong]
        # First get current
        current = libc.personality(0xFFFFFFFF)
        # Try to change
        result = libc.personality(PER_LINUX32)
        if result == -1:
            results["LockPersonality_neg"] = EXFAIL
        else:
            # Restore and check if it actually changed
            libc.personality(current)
            results["LockPersonality_neg"] = FAIL
    except Exception:
        results["LockPersonality_neg"] = EXFAIL


def test_MemoryDenyWriteExecute(results: dict[str, str]) -> None:
    """Test RWX mmap: PASS if blocked, EXPASS if allowed."""
    if _skip_if_not_linux("MemoryDenyWriteExecute", results):
        return
    try:
        PROT_READ = mmap.PROT_READ
        PROT_WRITE = mmap.PROT_WRITE
        PROT_EXEC = mmap.PROT_EXEC
        m = mmap.mmap(-1, 4096, prot=PROT_READ | PROT_WRITE | PROT_EXEC)
        m.close()
        results["MemoryDenyWriteExecute"] = EXPASS  # RWX allowed (MDWE=false)
    except PermissionError:
        results["MemoryDenyWriteExecute"] = PASS  # RWX blocked (MDWE=true)
    except OSError as e:
        if e.errno == errno.EPERM:
            results["MemoryDenyWriteExecute"] = PASS
        else:
            results["MemoryDenyWriteExecute"] = EXPASS
    except Exception:
        results["MemoryDenyWriteExecute"] = EXPASS


def test_SeccompFilter(results: dict[str, str]) -> None:
    """Verify seccomp filter is active (Seccomp: 2 in /proc/self/status)."""
    if _skip_if_not_linux("SeccompFilter", results):
        return
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("Seccomp:"):
                    mode = int(line.split()[1])
                    # 0 = disabled, 1 = strict, 2 = filter (systemd uses filter mode)
                    if mode == 2:
                        results["SeccompFilter"] = PASS
                    else:
                        results["SeccompFilter"] = FAIL
                    return
        # Seccomp line not found
        results["SeccompFilter"] = FAIL
    except Exception:
        results["SeccompFilter"] = FAIL


# =============================================================================
# Expected Functionality Tests
# =============================================================================


def test_Func_dns(results: dict[str, str]) -> None:
    """Test DNS resolution: reverse on 1.1.1.1, then forward on result."""
    try:
        # Reverse DNS
        hostname, _, _ = socket.gethostbyaddr("1.1.1.1")
        # Forward DNS
        socket.gethostbyname(hostname)
        results["Func_dns"] = PASS
    except Exception:
        results["Func_dns"] = FAIL


def test_Func_tls(results: dict[str, str]) -> None:
    """Test TLS/SSL module functional."""
    try:
        _ = ssl.create_default_context()
        # Just creating the context is enough
        results["Func_tls"] = PASS
    except Exception:
        results["Func_tls"] = FAIL


def test_Func_https(results: dict[str, str]) -> None:
    """Test HTTPS connection to www.python.org."""
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection(("www.python.org", 443), timeout=10) as sock:
            with ctx.wrap_socket(sock, server_hostname="www.python.org") as _:
                # Connection established
                results["Func_https"] = PASS
    except Exception:
        results["Func_https"] = FAIL


def test_Func_stateDir(results: dict[str, str]) -> None:
    """Test can write to /var/lib/influxdb3."""
    if _skip_if_not_linux("Func_stateDir", results):
        return
    path = "/var/lib/influxdb3"
    if not os.path.isdir(path):
        results["Func_stateDir"] = SKIP
        return
    test_file = f"{path}/sandbox_test_{os.getpid()}"
    try:
        with open(test_file, "w") as f:
            f.write("test")
        os.unlink(test_file)
        results["Func_stateDir"] = PASS
    except Exception:
        results["Func_stateDir"] = FAIL


def test_Func_logsDir(results: dict[str, str]) -> None:
    """Test can write to /var/log/influxdb3."""
    if _skip_if_not_linux("Func_logsDir", results):
        return
    path = "/var/log/influxdb3"
    if not os.path.isdir(path):
        results["Func_logsDir"] = SKIP
        return
    test_file = f"{path}/sandbox_test_{os.getpid()}"
    try:
        with open(test_file, "w") as f:
            f.write("test")
        os.unlink(test_file)
        results["Func_logsDir"] = PASS
    except Exception:
        results["Func_logsDir"] = FAIL


def _check_isolated_mount(path: str) -> bool:
    """Check if path has an isolated mount in /proc/self/mountinfo."""
    try:
        with open("/proc/self/mountinfo") as f:
            for line in f:
                fields = line.split()
                if len(fields) >= 5:
                    mount_point = fields[4]
                    if mount_point == path:
                        return True
    except Exception:
        pass
    return False


def test_Func_tmp(results: dict[str, str]) -> None:
    """Test write to /tmp + isolated mount check."""
    if _skip_if_not_linux("Func_tmp", results):
        return
    test_file = f"/tmp/sandbox_test_{os.getpid()}"
    try:
        with open(test_file, "w") as f:
            f.write("test")
        os.unlink(test_file)
        # Check for isolated mount
        if _check_isolated_mount("/tmp"):
            results["Func_tmp"] = PASS
        else:
            results["Func_tmp"] = FAIL
    except Exception:
        results["Func_tmp"] = FAIL


def test_Func_varTmp(results: dict[str, str]) -> None:
    """Test write to /var/tmp + isolated mount check."""
    if _skip_if_not_linux("Func_varTmp", results):
        return
    test_file = f"/var/tmp/sandbox_test_{os.getpid()}"
    try:
        with open(test_file, "w") as f:
            f.write("test")
        os.unlink(test_file)
        # Check for isolated mount
        if _check_isolated_mount("/var/tmp"):
            results["Func_varTmp"] = PASS
        else:
            results["Func_varTmp"] = FAIL
    except Exception:
        results["Func_varTmp"] = FAIL


def test_Func_devShm(results: dict[str, str]) -> None:
    """Test write to /dev/shm + isolated mount check."""
    if _skip_if_not_linux("Func_devShm", results):
        return
    test_file = f"/dev/shm/sandbox_test_{os.getpid()}"
    try:
        with open(test_file, "w") as f:
            f.write("test")
        os.unlink(test_file)
        # Check for isolated mount
        if _check_isolated_mount("/dev/shm"):
            results["Func_devShm"] = PASS
        else:
            results["Func_devShm"] = FAIL
    except Exception:
        results["Func_devShm"] = FAIL


def test_Func_imports(results: dict[str, str]) -> None:
    """Test core Python modules import."""
    try:
        import os as _

        results["Func_imports"] = PASS
    except Exception:
        results["Func_imports"] = FAIL

    try:
        import socket as _

        results["Func_imports"] = PASS
    except Exception:
        results["Func_imports"] = FAIL

    try:
        import sys as _

        results["Func_imports"] = PASS
    except Exception:
        results["Func_imports"] = FAIL


def test_Func_datetime(results: dict[str, str]) -> None:
    """Test datetime module works."""
    try:
        import datetime

        now = datetime.datetime.now()
        _ = now.strftime("%Y%m%d%H%M%S")
        results["Func_datetime"] = PASS
    except Exception:
        results["Func_datetime"] = FAIL


def test_Func_requests(results: dict[str, str]) -> None:
    """Test requests module works (SKIP if not installed)."""
    try:
        import requests

        _ = requests.__version__
        results["Func_requests"] = PASS
    except ImportError:
        results["Func_requests"] = SKIP
    except Exception:
        results["Func_requests"] = FAIL


def test_Func_sys(results: dict[str, str]) -> None:
    """Test sys.prefix/path accessible."""
    try:
        _ = sys.prefix
        _ = sys.path
        results["Func_sys"] = PASS
    except Exception:
        results["Func_sys"] = FAIL


def test_Func_lineBuilder(
    results: dict[str, str], influxdb3_local: Optional[Any]
) -> None:
    """Test LineBuilder API works."""
    _ = influxdb3_local  # for pyright
    try:
        # LineBuilder is provided by InfluxDB3's Python runtime
        line = LineBuilder("test_table")  # type: ignore[name-defined]
        line.tag("t1", "v1")
        line.int64_field("f1", 1)
        results["Func_lineBuilder"] = PASS
    except NameError:
        # LineBuilder not available in this context
        results["Func_lineBuilder"] = SKIP
    except Exception:
        results["Func_lineBuilder"] = FAIL


def test_Func_logging(results: dict[str, str], influxdb3_local: Optional[Any]) -> None:
    """Test influxdb3_local.info() works."""
    if influxdb3_local is None:
        results["Func_logging"] = SKIP
        return
    try:
        influxdb3_local.info("sandbox_verify: logging test")
        results["Func_logging"] = PASS
    except Exception:
        results["Func_logging"] = FAIL


def test_Func_unixSocket(results: dict[str, str]) -> None:
    """Test AF_UNIX sockets work (needed for DNS)."""
    if _skip_if_not_unix("Func_unixSocket", results):
        return
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.close()
        results["Func_unixSocket"] = PASS
    except Exception:
        results["Func_unixSocket"] = FAIL


def test_Func_dbWrite(results: dict[str, str], influxdb3_local: Optional[Any]) -> None:
    """Test writing to the database via influxdb3_local.write()."""
    if influxdb3_local is None:
        results["Func_dbWrite"] = SKIP
        return
    try:
        import time

        # LineBuilder is provided by InfluxDB3's Python runtime
        line = LineBuilder("sandbox_verify_test")  # type: ignore[name-defined]
        line.tag("test", "write")
        line.int64_field("value", 1)
        line.time_ns(int(time.time() * 1e9))
        influxdb3_local.write(line)
        results["Func_dbWrite"] = PASS
    except NameError:
        # LineBuilder not available
        results["Func_dbWrite"] = SKIP
    except Exception:
        results["Func_dbWrite"] = FAIL


def test_Func_dbQuery(results: dict[str, str], influxdb3_local: Optional[Any]) -> None:
    """Test querying the database via influxdb3_local.query()."""
    if influxdb3_local is None:
        results["Func_dbQuery"] = SKIP
        return
    try:
        # Simple query that should work on any database
        result = influxdb3_local.query("SHOW TABLES")
        # Result should be a list (even if empty)
        if isinstance(result, list):
            results["Func_dbQuery"] = PASS
        else:
            results["Func_dbQuery"] = FAIL
    except Exception:
        results["Func_dbQuery"] = FAIL


def test_Func_authHeader_neg(
    results: dict[str, str], request_headers: dict[str, str]
) -> None:
    """Verify Authorization header is NOT passed to plugins (security check)."""
    # Normalize header names to lowercase for case-insensitive lookup
    normalized = {k.lower(): v for k, v in request_headers.items()}
    if "authorization" in normalized:
        results["Func_authHeader_neg"] = FAIL  # Should not be present
    else:
        results["Func_authHeader_neg"] = PASS  # Expected - header filtered out


def dumpEnvAndHeaders(results: dict[str, Any], request_headers: dict[str, str]) -> None:
    """Dump os.environ and request headers for debugging."""
    results["dump_environ"] = dict(os.environ)
    results["dump_headers"] = dict(request_headers)


# =============================================================================
# Main plugin entry point
# =============================================================================


def process_request(
    influxdb3_local: Any,
    query_parameters: dict[str, str],
    request_headers: dict[str, str],
    request_body: bytes,
    args: Optional[dict[str, str]] = None,
) -> tuple[str, int, dict[str, str]]:
    """
    InfluxDB 3 request plugin entry point.

    Returns (body, status_code, headers).
    """
    _ = query_parameters  # for pyright
    _ = request_body  # for pyright
    _ = args  # for pyright
    results: dict[str, Any] = {}

    # Run all sandbox protection tests
    test_User_identity(results)
    test_Group_identity(results)
    test_SupplementaryGroups_identity(results)
    test_Umask_filePerms(results)
    test_ProtectHome_home_neg(results)
    test_ProtectSystem_etc_influxdb3_read(results)
    test_ProtectSystem_etc_influxdb3_write_neg(results)
    test_ProtectKernelLogs_neg(results)
    test_ProtectProc_invisible(results)
    test_InaccessiblePaths_dbus_neg(results)
    test_InaccessiblePaths_runUser_neg(results)
    test_RestrictAddressFamilies_netlink_neg(results)
    test_RestrictAddressFamilies_packet_neg(results)
    test_IPAddressDeny_localhost(results)
    test_IPAddressDeny_localhost6(results)
    test_SystemCallFilter_ptrace_neg(results)
    test_PrivateDevices_devMem_neg(results)
    test_NoNewPrivileges(results)
    test_LockPersonality_neg(results)
    test_MemoryDenyWriteExecute(results)
    test_SeccompFilter(results)
    test_RestrictNamespaces_unshare_neg(results)
    test_UserFaultFd_neg(results)
    test_IoUringSetup_neg(results)
    test_Bpf_neg(results)
    test_PerfEventOpen_neg(results)
    test_Keyctl_neg(results)
    test_CapBoundingSet(results)
    test_AmbientCapabilities(results)

    # Run all functionality tests
    test_Func_dns(results)
    test_Func_tls(results)
    test_Func_https(results)
    test_Func_stateDir(results)
    test_Func_logsDir(results)
    test_Func_tmp(results)
    test_Func_varTmp(results)
    test_Func_devShm(results)
    test_Func_imports(results)
    test_Func_datetime(results)
    test_Func_requests(results)
    test_Func_sys(results)
    test_Func_lineBuilder(results, influxdb3_local)
    test_Func_logging(results, influxdb3_local)
    test_Func_unixSocket(results)
    test_Func_dbWrite(results, influxdb3_local)
    test_Func_dbQuery(results, influxdb3_local)
    test_Func_authHeader_neg(results, request_headers)

    # Dump environment and headers for debugging
    dumpEnvAndHeaders(results, request_headers)

    # Determine status code
    has_fail = FAIL in results.values()
    status_code = 500 if has_fail else 200

    # Build response
    headers = {"Content-Type": "application/json"}
    body = json.dumps(results, indent=2)

    return (body, status_code, headers)
