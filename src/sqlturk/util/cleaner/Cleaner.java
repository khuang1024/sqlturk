package sqlturk.util.cleaner;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DriverManager;
import java.sql.SQLException;

import sqlturk.configuration.Parameters;

public class Cleaner {
    private Cleaner() {
	throw new AssertionError();
    }

    public static void dropWhyConnectedTable(Connection dbConn)
	    throws SQLException {
	Statement stmt = dbConn.createStatement();
	stmt.executeUpdate("DROP TABLE IF EXISTS " + Parameters.CONN_REL_NAME);
	System.out.println("DROP TABLE IF EXISTS " + Parameters.CONN_REL_NAME);
	stmt.close();
    }

    public static void dropWhyProvenanceTable(Connection dbConn)
	    throws SQLException {
	Statement stmt = dbConn.createStatement();
	stmt.executeUpdate("DROP TABLE IF EXISTS " + Parameters.PROV_REL_NAME);
	System.out.println("DROP TABLE IF EXISTS " + Parameters.PROV_REL_NAME);
	stmt.close();
    }

    public static void dropFDTable(Connection dbConn) throws SQLException {
	Statement stmt = dbConn.createStatement();
	stmt.executeUpdate("DROP TABLE IF EXISTS " + Parameters.FD_REL_NAME);
	System.out.println("DROP TABLE IF EXISTS " + Parameters.FD_REL_NAME);
	stmt.close();
    }

    public static void dropFDPlusTable(Connection dbConn) throws SQLException {
	Statement stmt = dbConn.createStatement();
	stmt.executeUpdate("DROP TABLE IF EXISTS "
		+ Parameters.FD_PLUS_REL_NAME);
	System.out.println("DROP TABLE IF EXISTS "
		+ Parameters.FD_PLUS_REL_NAME);
	stmt.close();
    }

    public static void dropSelectAliasTable(Connection dbConn)
	    throws SQLException {
	Statement stmt = dbConn.createStatement();
	stmt.executeUpdate("DROP TABLE IF EXISTS "
		+ Parameters.SELECT_ALIAS_REL_NAME);
	System.out.println("DROP TABLE IF EXISTS "
		+ Parameters.SELECT_ALIAS_REL_NAME);
	stmt.close();
    }

    public static void dropFromAliasTable(Connection dbConn)
	    throws SQLException {
	Statement stmt = dbConn.createStatement();
	stmt.executeUpdate("DROP TABLE IF EXISTS "
		+ Parameters.FROM_ALIAS_REL_NAME);
	System.out.println("DROP TABLE IF EXISTS "
		+ Parameters.FROM_ALIAS_REL_NAME);
	stmt.close();
    }

    public static void dropSourceMapTable(Connection dbConn)
	    throws SQLException {
	Statement stmt = dbConn.createStatement();
	stmt.executeUpdate("DROP TABLE IF EXISTS "
		+ Parameters.SOURCE_MAP_REL_NAME);
	System.out.println("DROP TABLE IF EXISTS "
		+ Parameters.SOURCE_MAP_REL_NAME);
	stmt.close();
    }

    public static void dropColumnInfoTable(Connection dbConn)
	    throws SQLException {
	Statement stmt = dbConn.createStatement();
	stmt.executeUpdate("DROP TABLE IF EXISTS "
		+ Parameters.COLUMN_INFO_REL_NAME);
	System.out.println("DROP TABLE IF EXISTS "
		+ Parameters.COLUMN_INFO_REL_NAME);
	stmt.close();
    }

    public static void dropIntersectionTable(Connection dbConn)
	    throws SQLException {
	Statement stmt = dbConn.createStatement();
	stmt.executeUpdate("DROP TABLE IF EXISTS "
		+ Parameters.INTERSECTION_REL_NAME);
	System.out.println("DROP TABLE IF EXISTS "
		+ Parameters.INTERSECTION_REL_NAME);
	stmt.close();
    }

    public static void dropUnionTable(Connection dbConn) throws SQLException {
	Statement stmt = dbConn.createStatement();
	stmt.executeUpdate("DROP TABLE IF EXISTS " + Parameters.UNION_REL_NAME);
	System.out.println("DROP TABLE IF EXISTS " + Parameters.UNION_REL_NAME);
	stmt.close();
    }

    public static void dropForeignKeyConstraintTables(Connection dbConn)
	    throws SQLException {
	Statement showTableStatement = dbConn.createStatement();
	Statement dropTableStatment = dbConn.createStatement();
	ResultSet rs = showTableStatement.executeQuery("SHOW TABLES");
	while (rs.next()) {
	    String tableName = rs.getString(1);
	    if (tableName.startsWith(Parameters.FK_CONSTRAINT_TABLE_PREFIX)
		    && tableName
			    .endsWith(Parameters.FK_CONSTRAINT_TABLE_SUFFIX)) {
		dropTableStatment.executeUpdate("DROP TABLE " + tableName);
		System.out.println("DROP TABLE " + tableName);
	    }
	}
	rs.close();
	showTableStatement.close();
	dropTableStatment.close();
    }

    public static void dropResultTables(Connection dbConn) throws SQLException {
	Statement showTableStatement = dbConn.createStatement();
	Statement dropTableStatment = dbConn.createStatement();
	ResultSet rs = showTableStatement.executeQuery("SHOW TABLES");
	while (rs.next()) {
	    String tableName = rs.getString(1);
	    if (tableName.startsWith(Parameters.QUERY_RESULT_PREFIX)
		    && tableName.endsWith(Parameters.QUERY_RESULT_SUFFIX)) {
		dropTableStatment.executeUpdate("DROP TABLE " + tableName);
		System.out.println("DROP TABLE " + tableName);
	    }
	}
	rs.close();
	showTableStatement.close();
	dropTableStatment.close();
    }

    public static void dropResultRewriteTables(Connection dbConn)
	    throws SQLException {
	Statement showTableStatement = dbConn.createStatement();
	Statement dropTableStatment = dbConn.createStatement();
	ResultSet rs = showTableStatement.executeQuery("SHOW TABLES");
	while (rs.next()) {
	    String tableName = rs.getString(1);
	    if (tableName.startsWith(Parameters.QUERY_RESULT_REWRITE_PREFIX)
		    && tableName
			    .endsWith(Parameters.QUERY_RESULT_REWRITE_SUFFIX)) {
		dropTableStatment.executeUpdate("DROP TABLE " + tableName);
		System.out.println("DROP TABLE " + tableName);
	    }
	}
	rs.close();
	showTableStatement.close();
	dropTableStatment.close();
    }

    public static void dropFDTempTables(Connection dbConn) throws SQLException {
	Statement showTableStatement = dbConn.createStatement();
	Statement dropTableStatment = dbConn.createStatement();
	ResultSet rs = showTableStatement.executeQuery("SHOW TABLES");
	while (rs.next()) {
	    String tableName = rs.getString(1);
	    if (tableName.startsWith(Parameters.FD_TEMP_TABLE_PREFIX)
		    && tableName.endsWith(Parameters.FD_TEMP_TABLE_SUFFIX)) {
		dropTableStatment.executeUpdate("DROP TABLE " + tableName);
		System.out.println("DROP TABLE " + tableName);
	    }
	}
	rs.close();
	showTableStatement.close();
	dropTableStatment.close();
    }

    public static void dropFDPlusTempTables(Connection dbConn)
	    throws SQLException {
	Statement showTableStatement = dbConn.createStatement();
	Statement dropTableStatment = dbConn.createStatement();
	ResultSet rs = showTableStatement.executeQuery("SHOW TABLES");
	while (rs.next()) {
	    String tableName = rs.getString(1);
	    if (tableName.startsWith(Parameters.FD_PLUS_TEMP_TABLE_PREFIX)
		    && tableName.endsWith(Parameters.FD_PLUS_TEMP_TABLE_SUFFIX)) {
		dropTableStatment.executeUpdate("DROP TABLE " + tableName);
		System.out.println("DROP TABLE " + tableName);
	    }
	}
	rs.close();
	showTableStatement.close();
	dropTableStatment.close();
    }

    public static void dropAll(Connection dbConn) throws SQLException {
	dropWhyConnectedTable(dbConn);
	dropWhyProvenanceTable(dbConn);
	dropFDTable(dbConn);
	dropFDPlusTable(dbConn);
	dropSelectAliasTable(dbConn);
	dropFromAliasTable(dbConn);
	dropSourceMapTable(dbConn);
	dropColumnInfoTable(dbConn);
	dropIntersectionTable(dbConn);
	dropUnionTable(dbConn);
	dropForeignKeyConstraintTables(dbConn);
	dropResultTables(dbConn);
	dropResultRewriteTables(dbConn);
	dropFDTempTables(dbConn);
	dropFDPlusTempTables(dbConn);
    }

    public static void dropIntermediateTables(Connection dbConn)
	    throws SQLException {
	// dropWhyConnectedTable(dbConn);
	// dropWhyProvenanceTable(dbConn);
	dropSelectAliasTable(dbConn);
	dropFromAliasTable(dbConn);
	dropSourceMapTable(dbConn);
	dropColumnInfoTable(dbConn);
	dropForeignKeyConstraintTables(dbConn);
	dropResultRewriteTables(dbConn);
	dropFDTempTables(dbConn);
	dropFDPlusTempTables(dbConn);
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
	// TODO Auto-generated method stub
	Class.forName("com.mysql.jdbc.Driver").newInstance();
	Connection dbConn;

	if (Parameters.USE_SERVER) {
	    dbConn = DriverManager
		    .getConnection(Parameters.MYSQL_CONNECTION_STRING);
	    System.out.println("Using server.");
	} else {
	    dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL,
		    Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
	    System.out.println("Using local.");
	}
	System.out.println("Start dropping all tables...");
	dropAll(dbConn);
	System.out.println("Finish dropping all tables");
    }

}
