# Flight SQL

InfluxDB IOx supports running SQL queries via [Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)

You can use either a native FlightSQL client as well as JDBC / ODBC Flight SQL drivers

## JDBC:

To use the JDBC driver with IOx:

1. Download the driver by following the link from [Maven](https://mvnrepository.com/artifact/org.apache.arrow/flight-sql/10.0.1) or [Dremio](https://www.dremio.com/drivers/jdbc/)
2. Use a jdbc conection of the format: `jdbc:arrow-flight-sql://hostname:port?useEncryption=false&database=NAME`

`hostname:port` is the host / port on which the IOx query gRPC API is running (default port is 8082), and `NAME` is the database name (for example, `26f7e5a4b7be365b_917b97a92e883afc`)

An example JDBC URL is:

```
jdbc:arrow-flight-sql://localhost:8082?useEncryption=false&database=26f7e5a4b7be365b_917b97a92e883afc
```
