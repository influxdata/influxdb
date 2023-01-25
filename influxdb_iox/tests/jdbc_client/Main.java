/**
 * Command line program that interacts with IOx via JDBC. See usage
 * details below.
 */
import java.sql.*;
import java.util.Properties;

public class Main {
    static void print_usage_and_exit(String[] args) {
        System.err.println("Usage:");
        System.err.println("jdbc_client <url> <command>");
        System.err.println("");
        System.err.println("Example:");
        System.err.println("jdbc_client 'jdbc:arrow-flight-sql://localhost:8082?useEncryption=false' query 'select * from foo'");
        System.err.println("");
        System.err.println("Commands:");
        System.err.println("# Run specified ad-hoc query");
        System.err.println("jdbc_client <url> query <sql>");
        System.err.println("# Run specified prepared query without parameters");
        System.err.println("jdbc_client <url> prepared_query <sql>");
        System.err.println("# Run metadata tests");
        System.err.println("jdbc_client <url> props");
        System.exit(1);
    }

    public static void main(String[] args) {
        System.out.println("InfluxDB IOx JDCB Test Driver");
        if (args.length < 1) {
            System.err.println("No url specified");
            print_usage_and_exit(args);
        }
        String url = args[0];

        if (args.length < 2) {
            System.err.println("No command specified");
            print_usage_and_exit(args);
        }
        String command = args[1];

        try {
            switch(command) {
            case "query":
                {
                    if (args.length < 3) {
                        System.err.println("No query specified");
                        print_usage_and_exit(args);
                    }
                    String query = args[2];

                    run_query(url, query);
                }
                break;

            case "prepared_query":
                {
                    if (args.length < 3) {
                        System.err.println("No query specified");
                        print_usage_and_exit(args);
                    }
                    String query = args[2];

                    run_prepared_query(url, query);
                }
                break;

            case "props":
                {
                    run_props(url);
                }
                break;

            default:
                {
                    System.err.println("Unknown command: " + command);
                    print_usage_and_exit(args);
                }
                break;
            }
	} catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
	}

        // if we got here, all good
	System.exit(0);
    }

    // Connect using the specified JBDC url
    static Connection connect(String url) throws SQLException {
        System.out.println("----- Connecting -------");
        System.out.println("URL: " + url);
        Properties props = new Properties();
        props.put("user", "test");
        props.put("password", "**token**");
        Connection conn = DriverManager.getConnection(url, props);
        return conn;
    }

    static void run_query(String url, String query) throws SQLException {
        System.out.println("----- Running SQL Query -------");
        System.out.println("query: " + query);
        Connection con = connect(url);

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        print_result_set(rs);
    }

    static void run_prepared_query(String url, String query) throws SQLException {
        System.out.println("----- Running Prepared SQL Query -------");
        System.out.println("query: " + query);
        Connection con = connect(url);
        PreparedStatement stmt = con.prepareStatement(query);
        ResultSet rs = stmt.executeQuery();
        print_result_set(rs);
    }

    static void run_props(String url) throws SQLException {
        Connection conn = connect(url);
        System.out.println(conn.getCatalog());
        DatabaseMetaData md = conn.getMetaData();
        System.out.println("isReadOnly: " + md.isReadOnly());
        System.out.println("getSearchStringEscape: " + md.getSearchStringEscape());
        System.out.println("getDriverVersion: " + md.getDriverVersion());
        System.out.println("getDatabaseProductVersion: " + md.getDatabaseProductVersion());
        System.out.println("getJDBCMajorVersion: " + md.getJDBCMajorVersion());
        System.out.println("getJDBCMinorVersion: " + md.getJDBCMinorVersion());
        System.out.println("getDriverName: " + md.getDriverName());
    }

    // Print out the ResultSet in a whitespace delimited form
    static void print_result_set(ResultSet rs) throws SQLException {
        // https://stackoverflow.com/questions/24229442/print-the-data-in-resultset-along-with-column-names
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) System.out.print(",  ");
            System.out.print(rsmd.getColumnName(i));
        }
        System.out.println("");
        System.out.println("------------");
        while (rs.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.print(",  ");
                System.out.print(rs.getString(i));
            }
            System.out.println("");
        }
    }
}
