`influx-tools import`
=====================

The import tool consumes binary data produced by `influx-tools export -format
binary` to write data directly to disk possibly under a new retention policy.
This tool handles the binary format only - exports of line protocol data should
be handled using the existing endpoints. Influx should be offline while this
tool is run.


Replacing existing shards
--------------------

The default behavior errors out when encountering an existing retention policy
with a different duration or shard duration or when attempting to import a
shard conflicting with an existing shard. Using the `-replace` option enables
replacing existing retention policies as well as replacing existing shards.
Existing shards will be deleted from both the metadata store as well as from
disk.

