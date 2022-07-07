After a partition of a table has not received any writes for some amount of time, the compactor will ensure it is stored in object store as N parquet files which:
* have non overlapping time ranges
* each does not exceed a size specified by config param max_desired_file_size_bytes.