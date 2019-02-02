`influx-tools import`
=====================

The import tool consumes binary data produced by `influx-tools export -format
binary` to write data directly to disk possibly under a new retention policy.
This tool handles the binary format only - exports of line protocol data should
be handled using the existing endpoints. Influx should be offline while this
tool is run.

If the target retention policy already exists, the tool will error out if you
attempt to change the retention policy settings. However, it is possible to
replace on disk shards with the `-replace` option.