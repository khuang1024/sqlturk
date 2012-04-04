package sqlturk.util.query;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;

public class QueryExecutor {

    private static int queryId = Parameters.QUERY_RESULT_TABLE_INDEX_START;
    private static int __rowId = Parameters.ROWID_QUERY_RESULT_START;

    private QueryExecutor() {
	throw new AssertionError();
    }

    private static void dropOldResultTables(Connection dbConn)
	    throws SQLException {
	Statement stmt = dbConn.createStatement();
	Statement stmt2 = dbConn.createStatement();
	ResultSet rs = stmt.executeQuery("SHOW TABLES");

	while (rs.next()) {
	    String tableName = rs.getString(1);
	    if (tableName.startsWith(Parameters.QUERY_RESULT_PREFIX)
		    && tableName.endsWith(Parameters.QUERY_RESULT_SUFFIX)) {
		stmt2.executeUpdate("DROP TABLE " + tableName);
	    }
	}
	if (rs != null) {
	    rs.close();
	}
	if (stmt != null) {
	    stmt.close();
	}
	if (stmt2 != null) {
	    stmt2.close();
	}
    }
    
    public static String getCurrentLastQueryResultTableName (Connection dbConn) throws SQLException {
	Statement stmt = dbConn.createStatement();
	ResultSet rs = stmt.executeQuery("SHOW TABLES");
	String lastResultTable = null;
	while (rs.next()) {
	    String tableName = rs.getString(1);
	    if (tableName.startsWith(Parameters.QUERY_RESULT_PREFIX)
		    && tableName.endsWith(Parameters.QUERY_RESULT_SUFFIX)) {
		lastResultTable = tableName;
	    }
	}
	return lastResultTable;
//	return Parameters.QUERY_RESULT_PREFIX + (queryId-1) + Parameters.QUERY_RESULT_SUFFIX;
    }
    
//    public static String getCurrentLastQueryResultTableName () {
//	
//	return Parameters.QUERY_RESULT_PREFIX + (queryId-1) + Parameters.QUERY_RESULT_SUFFIX;
//    }

    public static void executeQueries(ArrayList<String> queries,
	    Connection dbConn) throws SQLException {

//	dropOldResultTables(dbConn);

	for (String query : queries) {
	    executeQuery(query, dbConn);
	}
	
	queryId = Parameters.QUERY_RESULT_TABLE_INDEX_START;
    }

    private static void executeQuery(String query, Connection dbConn)
	    throws SQLException {
	System.out.println("Start executing query No." + queryId + " ...");
	System.out.println("debug:\t" + query);
	Statement stmt = dbConn.createStatement();
	
	// create the result table
	String resultTableName = Parameters.QUERY_RESULT_PREFIX + queryId
		+ Parameters.QUERY_RESULT_SUFFIX;
	
	// if this table exists, keep it
	if (isExist(resultTableName, dbConn)) {
	    System.out.println("\nTable "+ resultTableName +" has already existed, skip creating it. --\n");
	    queryId ++;
	    return;
	}

	// create a temporary table SQL_TURK_TMP
	stmt.executeUpdate("DROP TABLE IF EXISTS TEMP");
	String q = "";
	q += "CREATE TABLE TEMP\n";
	q += "AS\n";
	q += query;
	stmt.executeUpdate(q);

	
	
	
	stmt.executeUpdate("DROP TABLE IF EXISTS " + resultTableName);
	stmt.executeUpdate("CREATE TABLE " + resultTableName + " LIKE TEMP");

	// modify the result table
	// will throw out exceptions? since the original table may also have
	// ROWID, though it is hardly to be so?
	stmt.executeUpdate("ALTER TABLE " + resultTableName + " ADD COLUMN "
		+ Parameters.ROWID_ATT_NAME + " INT(11) UNIQUE AUTO_INCREMENT;");
	stmt.executeUpdate("ALTER TABLE " + resultTableName
		+ " AUTO_INCREMENT=" + __rowId);

	// insert the query results into the result table.
	ArrayList<String> attNames = new ArrayList<String>();
	ResultSet rs = stmt.executeQuery("DESCRIBE TEMP");
	String att = "";
	while (rs.next()) {
	    attNames.add(rs.getString(1));
	}
	for (int j = 0; j < attNames.size(); j++) {
	    if (j < attNames.size() - 1) {
		att += attNames.get(j) + ", ";
	    } else {
		att += attNames.get(j);
	    }
	}
	stmt.executeUpdate("INSERT INTO " + resultTableName + " (" + att + ") "
		+ " (SELECT * FROM TEMP)");

	// update the __rowId
	ResultSet rsRowIdCount = stmt.executeQuery(" (SELECT * FROM TEMP)");
	while (rsRowIdCount.next()) {
	    __rowId += Parameters.ROWID_TUPLE_INCREMENT;
	}

	stmt.executeUpdate("DROP TABLE TEMP");

	rs.close();
	stmt.close();
	System.out.println("End executing query No." + queryId);

	queryId++;
    }
    
    private static boolean isExist(String tableName, Connection dbConn) {
	Statement stmt = null;
	ResultSet rs = null;
	try {
	    stmt = dbConn.createStatement();
	    rs = stmt.executeQuery("SHOW TABLES");
	    while (rs.next()) {
		if (rs.getString(1).equals(tableName)) {
		    return true;
		}
	    }
	    return false;
	} catch (SQLException e) {
	    e.printStackTrace();
	    throw new RuntimeException("SQL Exception.");
	} finally {
	    if (rs != null) {
		try {
		    rs.close();
		} catch (SQLException e) {
		    e.printStackTrace();
		}
	    }
	    if (stmt != null) {
		try {
		    stmt.close();
		} catch (SQLException e) {
		    e.printStackTrace();
		}
	    }
	}
    }

}
