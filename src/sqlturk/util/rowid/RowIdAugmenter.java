package sqlturk.util.rowid;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;

public class RowIdAugmenter {

    private static int currentRowId = Parameters.ROWID_SRC_RELATION_START;

    // private static int firstId = currentRowId;

    private RowIdAugmenter() {
	throw new AssertionError();
    }

    /*
     * augments a relation with the ROWID attribute returns the next ROWID to be
     * used
     */
    public static void augmentRelationRowId(Connection dbConn, String relName) {
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

	// String countQuery = "select count(*) from "+relName;

	Statement stmt = null;
	ResultSet rs = null;
	try {
	    stmt = dbConn.createStatement();
	    stmt.executeUpdate("DROP TABLE IF EXISTS TEMP");// use the TEMP
							    // table
	    stmt.executeUpdate("CREATE TABLE TEMP LIKE " + relName);
	    stmt.executeUpdate("ALTER TABLE TEMP ADD COLUMN "
		    + Parameters.ROWID_ATT_NAME
		    + " INT AUTO_INCREMENT UNIQUE FIRST");
	    stmt.executeUpdate("ALTER TABLE TEMP AUTO_INCREMENT="
		    + currentRowId);

	    // insert the query results into the result table.
	    ArrayList<String> attNames = new ArrayList<String>();
	    rs = stmt.executeQuery("DESCRIBE " + relName);
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
	    stmt.executeUpdate("INSERT INTO TEMP (" + att + ") "
		    + " (SELECT * FROM " + relName + ")");

	    rs = stmt.executeQuery("SELECT COUNT(*) FROM TEMP");
	    rs.next();
	    int nTuples = rs.getInt(1);
	    currentRowId = currentRowId + Parameters.ROWID_TUPLE_INCREMENT
		    * nTuples;
	    System.out.println("Finish augmenting relation: " + relName
		    + ". Number of updated tuples: " + nTuples + ". Next "
		    + Parameters.ROWID_ATT_NAME + ": " + currentRowId + "\n");
	    // return nextRowId;

	    // rename the table
	    // stmt.executeUpdate("DROP TABLE " + relName);
	    // stmt.executeUpdate("RENAME TABLE TEMP TO " + relName);

	} catch (SQLException ex) {
	    // System.out.println("SQLException: " + ex.getMessage());
	    ex.printStackTrace();
	    // return -1;
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
