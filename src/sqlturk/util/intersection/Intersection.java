package sqlturk.util.intersection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;

public class Intersection {
    private static ArrayList<String> allIntersectionTables = new ArrayList<String>();

    Intersection() {
	throw new AssertionError();
    }
    
    static ArrayList<String> getAllIntersectionTables() {
	return allIntersectionTables;
    }

    public static void createIntersectionRelation(Connection dbConn)
	    throws SQLException {
	Statement stmt = dbConn.createStatement();
	ArrayList<String> allCommonAttributes = Projection
		.getAllCommonAttributes(dbConn);
	ArrayList<String> allProjectedRewriteResultTables = Projection
		.getAllProjectedRewriteResultTables(dbConn);

	// when there is no common attribute for resulting tuples
	if (allProjectedRewriteResultTables.isEmpty()) {
	    stmt.executeUpdate("CREATE TABLE "
		    + Parameters.INTERSECTION_REL_NAME
		    + " (INFO VARCHAR (30)) ");
	    stmt.executeUpdate("INSERT INTO "
		    + Parameters.INTERSECTION_REL_NAME
		    + " VALUES ('No common attributes')");
	} else {
	    int tempTableId = 0;
	    String tempTable = "XXXXX" + new java.util.Random().nextInt(100)
		    + "_";
	    stmt.executeUpdate("DROP TABLE IF EXISTS "
		    + Parameters.INTERSECTION_REL_NAME);
	    String selectClause = "";
	    String usingClause = "";
	    for (int i = 0; i < allCommonAttributes.size(); i++) {
		if (i < allCommonAttributes.size() - 1) {
		    selectClause += allCommonAttributes.get(i) + ", ";
		    usingClause += allCommonAttributes.get(i) + ", ";
		} else {
		    selectClause += allCommonAttributes.get(i) + " ";
		    usingClause += allCommonAttributes.get(i);
		}
	    }

	    String firstTempTable = tempTable + Integer.toString(tempTableId++);
	    allIntersectionTables.add(firstTempTable);
	    String query = "CREATE TABLE " + firstTempTable + " AS SELECT "
		    + selectClause + "FROM "
		    + allProjectedRewriteResultTables.get(0) + " INNER JOIN "
		    + allProjectedRewriteResultTables.get(1) + " USING ("
		    + usingClause + ")";
	    System.out.println("debug:\tThe query creating intersection: "
		    + query);
	    stmt.executeUpdate(query);

	    String oldTable = new String(firstTempTable);
	    for (int i = 2; i < allProjectedRewriteResultTables.size(); i++) {
		String newTempTable = tempTable
			+ Integer.toString(tempTableId++);
		String q = "CREATE TABLE " + newTempTable + " AS SELECT "
			+ selectClause + "FROM " + oldTable + " INNER JOIN "
			+ allProjectedRewriteResultTables.get(i) + " USING ("
			+ usingClause + ")";
		System.out.println("debug:\tThe query creating intersection: "
			+ q);
		stmt.executeUpdate(q);
		allIntersectionTables.add(newTempTable);
		oldTable = newTempTable;
	    }
	    stmt.executeUpdate("ALTER TABLE " + oldTable + " RENAME TO "
		    + Parameters.INTERSECTION_REL_NAME);
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

	// use soe server or test locally
	if (Parameters.USE_SERVER) {
	    dbConn = DriverManager
		    .getConnection(Parameters.MYSQL_CONNECTION_STRING);
	    System.out.println("Using server.");
	} else {
	    dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL,
		    Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
	    System.out.println("Using local.");
	}
	createIntersectionRelation(dbConn);
    }

}
