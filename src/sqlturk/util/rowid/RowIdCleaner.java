package sqlturk.util.rowid;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import sqlturk.configuration.Parameters;

public class RowIdCleaner {

    private RowIdCleaner() {
	throw new AssertionError();
    }

    public static void removeRowIDFromRelation(Connection dbConn, String relName) {
	System.out.println("Cleaning relation: " + relName);
	String cleaningQuery = "alter table " + relName + " drop column "
		+ Parameters.ROWID_ATT_NAME;
	Statement stmt = null;
	try {
	    stmt = dbConn.createStatement();
	    stmt.executeUpdate(cleaningQuery);
	} catch (SQLException ex) {
	    System.out.println("SQLException: " + ex.getMessage());
	} finally {
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
