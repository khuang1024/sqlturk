package sqlturk.util.rowid;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import sqlturk.configuration.Parameters;

public class RowIdAugmenter2 {

    // private static int currentRowId = Parameters.ROWID_BASE_RELATION_START;
    // private static int firstId = currentRowId;

    private RowIdAugmenter2() {
	throw new AssertionError();
    }

    /*
     * augments a relation with the ROWID attribute returns the next ROWID to be
     * used
     */
    public static int augmentRelationRowId(Connection dbConn, String relName,
	    int firstId) {
	System.out.println("Start augmenting relation: " + relName);

	/*
	 * Need to send 3 queries: set the initial auto increment value add the
	 * ROWID attribute count the tuples, to get the next ROWID, and return
	 * it
	 */
	/*
	 * debug: BUG: here, the setInitialValueQuery doesn't work, since we use
	 * "alter table ... add column ... in auto_increment unique first".
	 * "alter table ... auto_increment=..." doesn't work for this scenario.
	 */
	// String setInitialValueQuery =
	// "alter table "+relName+" auto_increment="+firstId;
	String augmentQuery = "alter table " + relName + " add column "
		+ Parameters.ROWID_ATT_NAME
		+ " int auto_increment unique first";
	String countQuery = "select count(*) from " + relName;

	Statement stmt = null;
	ResultSet rs = null;

	try {
	    stmt = dbConn.createStatement();
	    // stmt.executeUpdate(setInitialValueQuery);
	    stmt.executeUpdate(augmentQuery);
	    rs = stmt.executeQuery(countQuery);
	    rs.next();
	    int nTuples = rs.getInt(1);
	    int nextRowId = firstId + Parameters.ROWID_TUPLE_INCREMENT
		    * nTuples;
	    System.out.println("Finish augmenting relation: " + relName
		    + ". Number of updated tuples: " + nTuples + ". Next "
		    + Parameters.ROWID_ATT_NAME + ": " + nextRowId + "\n");
	    return nextRowId;
	} catch (SQLException ex) {
	    // System.out.println("SQLException: " + ex.getMessage());
	    ex.printStackTrace();
	    return -1;
	} finally {
	    if (rs != null) {
		try {
		    rs.close();
		} catch (SQLException sqlEx) {
		    sqlEx.printStackTrace();
		}
	    }
	    if (stmt != null) {
		try {
		    stmt.close();
		} catch (SQLException sqlEx) {
		    sqlEx.printStackTrace();
		}
	    }
	}
    }

}
