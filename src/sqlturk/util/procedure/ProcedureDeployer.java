package sqlturk.util.procedure;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class ProcedureDeployer {

    private ProcedureDeployer() {
	throw new AssertionError();
    }

    /*
     * deploys the procedure with the given body to the database (first, it
     * checks if a procedure with the same name already exists, and if yes,
     * drops it)
     */
    public static void deployProcedure(Connection dbConn, String procedureName,
	    String procedureText) {
	String dropExistingProcedureQuery = "DROP PROCEDURE IF EXISTS "
		+ procedureName;
	Statement stmt = null;
	try {
	    stmt = dbConn.createStatement();
	    stmt.executeUpdate(dropExistingProcedureQuery);
	    stmt.executeUpdate(procedureText);
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
