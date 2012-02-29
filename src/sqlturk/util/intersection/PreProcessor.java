package sqlturk.util.intersection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import sqlturk.configuration.Parameters;
import sqlturk.util.map.ColumnInfo;

/**
 * This class mainly pre-processes the columns of result tables by rewriting the
 * column name with it is provenance name. For example, lExpectancy ->
 * Country_LifeExpectancy.
 * 
 * @author kerui
 * 
 */
public class PreProcessor {
    private static ArrayList<String> rewriteResultTables = new ArrayList<String>();
//    private static boolean hasRewriteResultTable = false;

    PreProcessor() {
    }

    static ArrayList<String> getRewriteResultTables(Connection dbConn)
	    throws SQLException {
	rewriteAllResultTable(dbConn);
	return rewriteResultTables;
    }

    static void generateRewriteResultTable(Connection dbConn)
	    throws SQLException {
//	if (!hasRewriteResultTable) {
	    rewriteAllResultTable(dbConn);
//	}
    }

    private static void rewriteAllResultTable(Connection dbConn)
	    throws SQLException {
	// clear the previous content
	rewriteResultTables.clear();
	
//	hasRewriteResultTable = true;
	Statement stmt = dbConn.createStatement();
	ResultSet rs = stmt.executeQuery("SHOW TABLES");

	while (rs.next()) {
	    String tableName = rs.getString(1);
	    if (tableName.startsWith(Parameters.QUERY_RESULT_PREFIX)
		    && tableName.endsWith(Parameters.QUERY_RESULT_SUFFIX)) {
		rewriteResultTables
			.add(getRewriteResultTable(tableName, dbConn));
	    }
	}
	rs.close();
	stmt.close();
    }

    /**
     * 
     * @param relationName
     * @param dbConn
     * @return the rewritten result table name
     * @throws SQLException
     */
    private static String getRewriteResultTable(String relationName,
	    Connection dbConn) throws SQLException {
	System.out.println("debug:\tStart rewriting table " + relationName);
	Statement stmt = dbConn.createStatement();
	String rewriteTableName = Parameters.QUERY_RESULT_REWRITE_PREFIX
		+ relationName + Parameters.QUERY_RESULT_REWRITE_SUFFIX;
	stmt.executeUpdate("DROP TABLE IF EXISTS " + rewriteTableName);
	stmt.executeUpdate("CREATE TABLE " + rewriteTableName
		+ " AS SELECT * FROM " + relationName);
	stmt.executeUpdate("ALTER TABLE " + rewriteTableName + " DROP COLUMN "
		+ Parameters.ROWID_ATT_NAME); // DON'T forget to drop __ROWID

	HashMap<String, String> columnTypes = getType(relationName, dbConn);
	Set<String> columns = columnTypes.keySet();
	for (String column : columns) {
	    if (!column.equals(Parameters.ROWID_ATT_NAME)) { // ignore __ROWID
							     // column
		String newType = columnTypes.get(column);
		System.out
			.println("debug:\tRenaming column ["
				+ column
				+ "] to ["
				+ ColumnInfo.getOriginalTableColumnName(
					relationName, column, dbConn)
				+ "] in type (" + newType + ")");
		stmt.executeUpdate("ALTER TABLE "
			+ rewriteTableName
			+ " CHANGE COLUMN "
			+ column
			+ " "
			+ ColumnInfo.getOriginalTableColumnName(relationName,
				column, dbConn) + " " + newType);
	    }
	}
	stmt.close();
	System.out.println("debug:\tFinish rewriting table " + relationName
		+ ", generated new table " + rewriteTableName);
	System.out.println();
	return rewriteTableName;
    }

    private static HashMap<String, String> getType(String relationName,
	    Connection dbConn) throws SQLException {
	HashMap<String, String> columnTypes = new HashMap<String, String>();

	Statement stmt = dbConn.createStatement();
	ResultSet rs = stmt.executeQuery("DESC " + relationName);
	while (rs.next()) {
	    columnTypes.put(rs.getString("Field"), rs.getString("Type"));
	}
	rs.close();
	stmt.close();
	return columnTypes;
    }

    /**
     * @param args
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws SQLException
     */
    public static void main(String[] args) throws InstantiationException,
	    IllegalAccessException, ClassNotFoundException, SQLException {
	Class.forName("com.mysql.jdbc.Driver").newInstance();
	Connection dbConn;
	/*
	 * use soe server or test locally
	 */
	if (Parameters.USE_SERVER) {
	    dbConn = DriverManager
		    .getConnection(Parameters.MYSQL_CONNECTION_STRING);
	    System.out.println("Using server.");
	} else {
	    dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL,
		    Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
	    System.out.println("Using local.");
	}
	System.out.println(getRewriteResultTable("Q2_RES", dbConn));
    }

}
