This directory contains a JDBC client program that is used for testing InfluxBD IOx using the FlightSQL JDBC driver

# Local Execution:

## Run IOx

```shell
influxdb_iox -v
```

## Run the JDBC test

To run the JDBC test program, specify the target database in the JDBC URL:

```shell
# run the jdbc client driver program, downloading the JDBC driver if needed
./jdbc_client "jdbc:arrow-flight-sql://localhost:8082?useEncryption=false&database=26f7e5a4b7be365b_917b97a92e883afc" query 'select * from cpu'
```

# Cleanup:

Clean up any intermediate files (like JDBC driver)

```shell
make clean
```
