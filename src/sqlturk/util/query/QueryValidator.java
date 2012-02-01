package sqlturk.util.query;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class QueryValidator {

    private QueryValidator() {
	throw new AssertionError();
    }

    // public static boolean isValidQuery(String query, Connection dbConn) {
    //
    // try {
    // Statement stmt = dbConn.createStatement();
    // stmt.executeQuery(query);
    // } catch (SQLException e) {
    // return false;
    // }
    // return true;
    // }

    public static boolean isValidQuery(String query, Connection dbConn) {
	/*
	 * first clean the query, and add the "limit 0" to prevent lengthy
	 * executions
	 */
	String trimmedQuery = query.trim();
	if (trimmedQuery.length() < 6) {
	    return false;
	}
	String prefix = trimmedQuery.substring(0, 6).toLowerCase();
	if (!prefix.equals("select")) {
	    return false;
	}
	if (trimmedQuery.endsWith(";")) {
	    trimmedQuery = trimmedQuery.substring(0, trimmedQuery.length() - 1);
	}
	String queryToTest = trimmedQuery + " limit 0";
	// System.out.println(queryToTest);
	Statement stmt = null;
	ResultSet rs = null;
	try {
	    stmt = dbConn.createStatement();
	    rs = stmt.executeQuery(queryToTest);
	    // System.out.println("Returning ok");
	    return true;
	} catch (SQLException ex) {
	    // System.out.println("Returning error");
	    System.out.println("SQLException: " + ex.getMessage());
	    return false;
	} finally {
	    // System.out.println("Cleaning up");
	    if (rs != null) {
		try {
		    rs.close();
		} catch (SQLException sqlEx) {
		    sqlEx.printStackTrace();
		}
		rs = null;
	    }
	    if (stmt != null) {
		try {
		    stmt.close();
		} catch (SQLException sqlEx) {
		    sqlEx.printStackTrace();
		}
		stmt = null;
	    }
	}
    }
}
