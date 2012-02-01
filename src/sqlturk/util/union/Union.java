package sqlturk.util.union;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;

public class Union {

    Union() {
	throw new AssertionError();
    }

    public static void createUionRelation(Connection dbConn)
	    throws SQLException {
	Statement stmt = dbConn.createStatement();
	ArrayList<String> allProjectedRewriteResultTables = Projection
		.getAllProjectedRewriteResultTables(dbConn);

	// when there is no common attribute for resulting tuples
	if (allProjectedRewriteResultTables.isEmpty()) {
	    stmt.executeUpdate("CREATE TABLE " + Parameters.UNION_REL_NAME
		    + " (INFO VARCHAR (30)) ");
	    stmt.executeUpdate("INSERT INTO " + Parameters.UNION_REL_NAME
		    + " VALUES ('No common attributes')");
	} else {
	    System.out.println("debug:\t" + "DROP TABLE IF EXISTS "
		    + Parameters.UNION_REL_NAME);
	    stmt.executeUpdate("DROP TABLE IF EXISTS "
		    + Parameters.UNION_REL_NAME);

	    String unionClause = "";
	    for (int i = 0; i < allProjectedRewriteResultTables.size(); i++) {
		if (i < allProjectedRewriteResultTables.size() - 1) {
		    unionClause += "SELECT * FROM "
			    + allProjectedRewriteResultTables.get(i)
			    + " UNION ";
		} else {
		    unionClause += "SELECT * FROM "
			    + allProjectedRewriteResultTables.get(i);
		}
	    }

	    String query = "CREATE TABLE " + Parameters.UNION_REL_NAME + " AS "
		    + unionClause;
	    System.out.println("debug:\tThe query creating union: " + query);
	    stmt.executeUpdate(query);

	    PostProcessor.dropAll(dbConn);
	}
	stmt.close();
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
	createUionRelation(dbConn);
    }

}
