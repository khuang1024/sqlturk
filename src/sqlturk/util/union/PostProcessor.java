package sqlturk.util.union;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class PostProcessor {

    PostProcessor() {
	throw new AssertionError();
    }

    static void dropAllRewriteResultTables(Connection dbConn)
	    throws SQLException {
	Statement stmt = dbConn.createStatement();
	ArrayList<String> allRewriteResultTables = PreProcessor
		.getRewriteResultTables(dbConn);
	for (String rewriteResultTable : allRewriteResultTables) {
	    stmt.executeUpdate("DROP TABLE IF EXISTS " + rewriteResultTable);
	}
	allRewriteResultTables.clear();
	stmt.close();
    }

    static void dropAllProjectedRewriteResultTables(Connection dbConn)
	    throws SQLException {
	Statement stmt = dbConn.createStatement();
	ArrayList<String> allProjectedRewriteResultTables = Projection
		.getAllProjectedRewriteResultTables(dbConn);
	for (String projectedRewriteResultTable : allProjectedRewriteResultTables) {
	    stmt.executeUpdate("DROP TABLE IF EXISTS "
		    + projectedRewriteResultTable);
	}
	allProjectedRewriteResultTables.clear();
	stmt.close();
    }

    static void dropAll(Connection dbConn) throws SQLException {
	dropAllRewriteResultTables(dbConn);
	dropAllProjectedRewriteResultTables(dbConn);
    }
}
