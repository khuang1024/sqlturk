package sqlturk.util.procedure;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class ProcedureExecuter {
    private ProcedureExecuter() {
	throw new AssertionError();
    }

    public static void executeProcedure(Connection dbConn, String procedureName) {
	Statement stmt = null;
	try {
	    stmt = dbConn.createStatement();
	    stmt.executeUpdate("call " + procedureName);
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
