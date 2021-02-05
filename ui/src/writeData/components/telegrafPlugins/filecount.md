# Filecount Input Plugin

Reports the number and total size of files in specified directories.

### Configuration:

```toml
[[inputs.filecount]]
  ## Directory to gather stats about.
  ##   deprecated in 1.9; use the directories option
  # directory = "/var/cache/apt/archives"

  ## Directories to gather stats about.
  ## This accept standard unit glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   /var/log/**    -> recursively find all directories in /var/log and count files in each directories
  ##   /var/log/*/*   -> find all directories with a parent dir in /var/log and count files in each directories
  ##   /var/log       -> count all files in /var/log and all of its subdirectories
  directories = ["/var/cache/apt", "/tmp"]

  ## Only count files that match the name pattern. Defaults to "*".
  name = "*"

  ## Count files in subdirectories. Defaults to true.
  recursive = true

  ## Only count regular files. Defaults to true.
  regular_only = true

  ## Follow all symlinks while walking the directory tree. Defaults to false.
  follow_symlinks = false

  ## Only count files that are at least this size. If size is
  ## a negative number, only count files that are smaller than the
  ## absolute value of size. Acceptable units are B, KiB, MiB, KB, ...
  ## Without quotes and units, interpreted as size in bytes.
  size = "0B"

  ## Only count files that have not been touched for at least this
  ## duration. If mtime is negative, only count files that have been
  ## touched in this duration. Defaults to "0s".
  mtime = "0s"
```

### Metrics

- filecount
  - tags:
    - directory (the directory path)
  - fields:
    - count (integer)
    - size_bytes (integer)

### Example Output:

```
filecount,directory=/var/cache/apt count=7i,size_bytes=7438336i 1530034445000000000
filecount,directory=/tmp count=17i,size_bytes=28934786i 1530034445000000000
```
