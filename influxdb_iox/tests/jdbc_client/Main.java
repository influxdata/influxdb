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
        System.err.println("jdbc_client <url> metadata");
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

            case "metadata":
                {
                    run_metadata(url);
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

    static void run_metadata(String url) throws SQLException {
        Connection conn = connect(url);
        System.out.println(conn.getCatalog());
        DatabaseMetaData md = conn.getMetaData();
        // Note yet implemented
        // (see https://github.com/influxdata/influxdb_iox/issues/7210 )

        System.out.println("**************");
        System.out.println("Catalogs:");
        System.out.println("**************");
        print_result_set(md.getCatalogs());

        System.out.println("**************");
        System.out.println("CrossReference");
        System.out.println("**************");
        print_result_set(md.getCrossReference(null, null, "system", null, null, "iox"));

        System.out.println("**************");
        System.out.println("Schemas:");
        System.out.println("**************");
        print_result_set(md.getSchemas());

        System.out.println("**************");
        System.out.println("ExportedKeys");
        System.out.println("**************");
        print_result_set(md.getExportedKeys(null, null, "system"));


        System.out.println("**************");
        System.out.println("ImportedKeys");
        System.out.println("**************");
        print_result_set(md.getImportedKeys(null, null, "system"));

        System.out.println("**************");
        System.out.println("PrimaryKeys:");
        System.out.println("**************");
        print_result_set(md.getPrimaryKeys(null, null, "system"));

        System.out.println("**************");
        System.out.println("Tables:");
        System.out.println("**************");
        // null means no filtering
        print_result_set(md.getTables(null, null, null, null));

        System.out.println("**************");
        System.out.println("Tables (system table filter):");
        System.out.println("**************");
        print_result_set(md.getTables("public", "system", null, null));
       
        System.out.println("**************");
        System.out.println("Table Types:");
        System.out.println("**************");
        print_result_set(md.getTableTypes());

        System.out.println("**************");
        System.out.println("getColumns:");
        System.out.println("**************");
        print_result_set(md.getColumns(null, null, null, null));

        System.out.println("**************");
        System.out.println("Type Info:");
        System.out.println("**************");
        print_result_set(md.getTypeInfo());

        System.out.println("**************");
        System.out.println("getFunctions:");
        System.out.println("**************");
        print_result_set(md.getFunctions(null, null, null));


        // List from https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html
        System.out.println("allProceduresAreCallable: " + md.allProceduresAreCallable());
        System.out.println("allTablesAreSelectable: " + md.allTablesAreSelectable());
        System.out.println("autoCommitFailureClosesAllResultSets: " + md.autoCommitFailureClosesAllResultSets());
        System.out.println("dataDefinitionCausesTransactionCommit: " + md.dataDefinitionCausesTransactionCommit());
        System.out.println("dataDefinitionIgnoredInTransactions: " + md.dataDefinitionIgnoredInTransactions());
        System.out.println("doesMaxRowSizeIncludeBlobs: " + md.doesMaxRowSizeIncludeBlobs());
        System.out.println("generatedKeyAlwaysReturned: " + md.generatedKeyAlwaysReturned());
        System.out.println("getCatalogSeparator: " + md.getCatalogSeparator());
        System.out.println("getCatalogTerm: " + md.getCatalogTerm());
        System.out.println("getDatabaseMajorVersion: " + md.getDatabaseMajorVersion());
        System.out.println("getDatabaseMinorVersion: " + md.getDatabaseMinorVersion());
        System.out.println("getDatabaseProductName: " + md.getDatabaseProductName());
        System.out.println("getDatabaseProductVersion: " + md.getDatabaseProductVersion());
        System.out.println("getDefaultTransactionIsolation: " + md.getDefaultTransactionIsolation());
        System.out.println("getDriverMajorVersion: " + md.getDriverMajorVersion());
        System.out.println("getDriverMinorVersion: " + md.getDriverMinorVersion());
        System.out.println("getDriverName: " + md.getDriverName());
        System.out.println("getDriverVersion: " + md.getDriverVersion());
        System.out.println("getExtraNameCharacters: " + md.getExtraNameCharacters());
        System.out.println("getIdentifierQuoteString: " + md.getIdentifierQuoteString());
        System.out.println("getJDBCMajorVersion: " + md.getJDBCMajorVersion());
        System.out.println("getJDBCMinorVersion: " + md.getJDBCMinorVersion());
        System.out.println("getMaxBinaryLiteralLength: " + md.getMaxBinaryLiteralLength());
        System.out.println("getMaxCatalogNameLength: " + md.getMaxCatalogNameLength());
        System.out.println("getMaxCharLiteralLength: " + md.getMaxCharLiteralLength());
        System.out.println("getMaxColumnNameLength: " + md.getMaxColumnNameLength());
        System.out.println("getMaxColumnsInGroupBy: " + md.getMaxColumnsInGroupBy());
        System.out.println("getMaxColumnsInIndex: " + md.getMaxColumnsInIndex());
        System.out.println("getMaxColumnsInOrderBy: " + md.getMaxColumnsInOrderBy());
        System.out.println("getMaxColumnsInSelect: " + md.getMaxColumnsInSelect());
        System.out.println("getMaxColumnsInTable: " + md.getMaxColumnsInTable());
        System.out.println("getMaxConnections: " + md.getMaxConnections());
        System.out.println("getMaxCursorNameLength: " + md.getMaxCursorNameLength());
        System.out.println("getMaxIndexLength: " + md.getMaxIndexLength());
        System.out.println("getMaxLogicalLobSize: " + md.getMaxLogicalLobSize());
        System.out.println("getMaxProcedureNameLength: " + md.getMaxProcedureNameLength());
        System.out.println("getMaxRowSize: " + md.getMaxRowSize());
        System.out.println("getMaxSchemaNameLength: " + md.getMaxSchemaNameLength());
        System.out.println("getMaxStatementLength: " + md.getMaxStatementLength());
        System.out.println("getMaxStatements: " + md.getMaxStatements());
        System.out.println("getMaxTableNameLength: " + md.getMaxTableNameLength());
        System.out.println("getMaxTablesInSelect: " + md.getMaxTablesInSelect());
        System.out.println("getMaxUserNameLength: " + md.getMaxUserNameLength());
        System.out.println("getNumericFunctions: " + md.getNumericFunctions());
        System.out.println("getProcedureTerm: " + md.getProcedureTerm());
        System.out.println("getResultSetHoldability: " + md.getResultSetHoldability());
        System.out.println("getSchemaTerm: " + md.getSchemaTerm());
        System.out.println("getSearchStringEscape: " + md.getSearchStringEscape());
        System.out.println("getSQLKeywords: " + md.getSQLKeywords());
        System.out.println("getSQLStateType: " + md.getSQLStateType());
        System.out.println("getStringFunctions: " + md.getStringFunctions());
        System.out.println("getSystemFunctions: " + md.getSystemFunctions());
        System.out.println("getTimeDateFunctions: " + md.getTimeDateFunctions());
        System.out.println("getURL: " + md.getURL());
        System.out.println("isCatalogAtStart: " + md.isCatalogAtStart());
        System.out.println("isReadOnly: " + md.isReadOnly());
        System.out.println("locatorsUpdateCopy: " + md.locatorsUpdateCopy());
        System.out.println("nullPlusNonNullIsNull: " + md.nullPlusNonNullIsNull());
        System.out.println("nullsAreSortedAtEnd: " + md.nullsAreSortedAtEnd());
        System.out.println("nullsAreSortedAtStart: " + md.nullsAreSortedAtStart());
        System.out.println("nullsAreSortedHigh: " + md.nullsAreSortedHigh());
        System.out.println("nullsAreSortedLow: " + md.nullsAreSortedLow());
        System.out.println("storesLowerCaseIdentifiers: " + md.storesLowerCaseIdentifiers());
        System.out.println("storesLowerCaseQuotedIdentifiers: " + md.storesLowerCaseQuotedIdentifiers());
        System.out.println("storesMixedCaseIdentifiers: " + md.storesMixedCaseIdentifiers());
        System.out.println("storesMixedCaseQuotedIdentifiers: " + md.storesMixedCaseQuotedIdentifiers());
        System.out.println("storesUpperCaseIdentifiers: " + md.storesUpperCaseIdentifiers());
        System.out.println("storesUpperCaseQuotedIdentifiers: " + md.storesUpperCaseQuotedIdentifiers());
        System.out.println("supportsAlterTableWithAddColumn: " + md.supportsAlterTableWithAddColumn());
        System.out.println("supportsAlterTableWithDropColumn: " + md.supportsAlterTableWithDropColumn());
        System.out.println("supportsANSI92EntryLevelSQL: " + md.supportsANSI92EntryLevelSQL());
        System.out.println("supportsANSI92FullSQL: " + md.supportsANSI92FullSQL());
        System.out.println("supportsANSI92IntermediateSQL: " + md.supportsANSI92IntermediateSQL());
        System.out.println("supportsBatchUpdates: " + md.supportsBatchUpdates());
        System.out.println("supportsCatalogsInDataManipulation: " + md.supportsCatalogsInDataManipulation());
        System.out.println("supportsCatalogsInIndexDefinitions: " + md.supportsCatalogsInIndexDefinitions());
        System.out.println("supportsCatalogsInPrivilegeDefinitions: " + md.supportsCatalogsInPrivilegeDefinitions());
        System.out.println("supportsCatalogsInProcedureCalls: " + md.supportsCatalogsInProcedureCalls());
        System.out.println("supportsCatalogsInTableDefinitions: " + md.supportsCatalogsInTableDefinitions());
        System.out.println("supportsColumnAliasing: " + md.supportsColumnAliasing());
        // Convert not yet supported
        // https://github.com/influxdata/influxdb_iox/issues/7253
        //System.out.println("supportsConvert: " + md.supportsConvert());
        System.out.println("supportsCoreSQLGrammar: " + md.supportsCoreSQLGrammar());
        System.out.println("supportsCorrelatedSubqueries: " + md.supportsCorrelatedSubqueries());
        System.out.println("supportsDataDefinitionAndDataManipulationTransactions: " + md.supportsDataDefinitionAndDataManipulationTransactions());
        System.out.println("supportsDataManipulationTransactionsOnly: " + md.supportsDataManipulationTransactionsOnly());
        System.out.println("supportsDifferentTableCorrelationNames: " + md.supportsDifferentTableCorrelationNames());
        System.out.println("supportsExpressionsInOrderBy: " + md.supportsExpressionsInOrderBy());
        System.out.println("supportsExtendedSQLGrammar: " + md.supportsExtendedSQLGrammar());
        System.out.println("supportsFullOuterJoins: " + md.supportsFullOuterJoins());
        System.out.println("supportsGetGeneratedKeys: " + md.supportsGetGeneratedKeys());
        System.out.println("supportsGroupBy: " + md.supportsGroupBy());
        System.out.println("supportsGroupByBeyondSelect: " + md.supportsGroupByBeyondSelect());
        System.out.println("supportsGroupByUnrelated: " + md.supportsGroupByUnrelated());
        System.out.println("supportsIntegrityEnhancementFacility: " + md.supportsIntegrityEnhancementFacility());
        System.out.println("supportsLikeEscapeClause: " + md.supportsLikeEscapeClause());
        System.out.println("supportsLimitedOuterJoins: " + md.supportsLimitedOuterJoins());
        System.out.println("supportsMinimumSQLGrammar: " + md.supportsMinimumSQLGrammar());
        System.out.println("supportsMixedCaseIdentifiers: " + md.supportsMixedCaseIdentifiers());
        System.out.println("supportsMixedCaseQuotedIdentifiers: " + md.supportsMixedCaseQuotedIdentifiers());
        System.out.println("supportsMultipleOpenResults: " + md.supportsMultipleOpenResults());
        System.out.println("supportsMultipleResultSets: " + md.supportsMultipleResultSets());
        System.out.println("supportsMultipleTransactions: " + md.supportsMultipleTransactions());
        System.out.println("supportsNamedParameters: " + md.supportsNamedParameters());
        System.out.println("supportsNonNullableColumns: " + md.supportsNonNullableColumns());
        System.out.println("supportsOpenCursorsAcrossCommit: " + md.supportsOpenCursorsAcrossCommit());
        System.out.println("supportsOpenCursorsAcrossRollback: " + md.supportsOpenCursorsAcrossRollback());
        System.out.println("supportsOpenStatementsAcrossCommit: " + md.supportsOpenStatementsAcrossCommit());
        System.out.println("supportsOpenStatementsAcrossRollback: " + md.supportsOpenStatementsAcrossRollback());
        System.out.println("supportsOrderByUnrelated: " + md.supportsOrderByUnrelated());
        System.out.println("supportsOuterJoins: " + md.supportsOuterJoins());
        System.out.println("supportsPositionedDelete: " + md.supportsPositionedDelete());
        System.out.println("supportsPositionedUpdate: " + md.supportsPositionedUpdate());
        System.out.println("supportsRefCursors: " + md.supportsRefCursors());
        System.out.println("supportsSavepoints: " + md.supportsSavepoints());
        System.out.println("supportsSchemasInDataManipulation: " + md.supportsSchemasInDataManipulation());
        System.out.println("supportsSchemasInIndexDefinitions: " + md.supportsSchemasInIndexDefinitions());
        System.out.println("supportsSchemasInPrivilegeDefinitions: " + md.supportsSchemasInPrivilegeDefinitions());
        System.out.println("supportsSchemasInProcedureCalls: " + md.supportsSchemasInProcedureCalls());
        System.out.println("supportsSchemasInTableDefinitions: " + md.supportsSchemasInTableDefinitions());
        System.out.println("supportsSelectForUpdate: " + md.supportsSelectForUpdate());
        System.out.println("supportsStatementPooling: " + md.supportsStatementPooling());
        System.out.println("supportsStoredFunctionsUsingCallSyntax: " + md.supportsStoredFunctionsUsingCallSyntax());
        System.out.println("supportsStoredProcedures: " + md.supportsStoredProcedures());
        System.out.println("supportsSubqueriesInComparisons: " + md.supportsSubqueriesInComparisons());
        System.out.println("supportsSubqueriesInExists: " + md.supportsSubqueriesInExists());
        System.out.println("supportsSubqueriesInIns: " + md.supportsSubqueriesInIns());
        System.out.println("supportsSubqueriesInQuantifieds: " + md.supportsSubqueriesInQuantifieds());
        System.out.println("supportsTableCorrelationNames: " + md.supportsTableCorrelationNames());
        System.out.println("supportsTransactionIsolationLevel: " + md.supportsTransactionIsolationLevel(0));
        System.out.println("supportsTransactions: " + md.supportsTransactions());
        System.out.println("supportsUnion: " + md.supportsUnion());
        System.out.println("supportsUnionAll: " + md.supportsUnionAll());
        System.out.println("usesLocalFilePerTable: " + md.usesLocalFilePerTable());
        System.out.println("usesLocalFiles: " + md.usesLocalFiles());

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
