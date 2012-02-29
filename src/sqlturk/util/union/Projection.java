package sqlturk.util.union;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;

class Projection {

    private static ArrayList<String> allCommonAttributes = 
	    new ArrayList<String>();
    private static ArrayList<String> allProjectedRewriteResultTables = 
	    new ArrayList<String>();
    private static boolean isCreatedProjectedRewriteResultTables = false;

    private Projection() {
	throw new AssertionError();
    }

    static ArrayList<String> getAllProjectedRewriteResultTables(
	    Connection dbConn) throws SQLException {
	if (!isCreatedProjectedRewriteResultTables) {
	    createProjectedRewriteResultTables(dbConn);
	}
	return allProjectedRewriteResultTables;
    }

    private static void createProjectedRewriteResultTables(Connection dbConn)
	    throws SQLException {
	Statement stmt = dbConn.createStatement();
	isCreatedProjectedRewriteResultTables = true;

	ArrayList<String> allCommonAttributes = Projection
		.getAllCommonAttributes(dbConn);
	ArrayList<String> allRewriteResultTables = PreProcessor
		.getRewriteResultTables(dbConn);
	
	// debug:
	if (!isSizeMatch(allCommonAttributes, allRewriteResultTables, dbConn)) {
	    allProjectedRewriteResultTables = null;
	    return;
	}
	

	// TODO only consider common attribute, rather than equivalent
	// * attirbute.
	String selectClause = "";
	for (int i = 0; i < allCommonAttributes.size(); i++) {

	    // debug
	    // System.out.println("debug:\t" + allCommonAttributes.get(i));

	    if (i < allCommonAttributes.size() - 1) {
		selectClause += allCommonAttributes.get(i) + ", ";
	    } else {
		selectClause += allCommonAttributes.get(i) + " ";
	    }
	}
	
	// solve the confict, record what projected tables
	// have been created.
	ArrayList<String> projectedTables = new ArrayList<String>();
	ResultSet rs = stmt.executeQuery("SHOW TABLES");
	while(rs.next()) {
	    if (rs.getString(1).startsWith("projected")) {
		projectedTables.add(rs.getString(1));
	    }
	}
	rs.close();
	

	// when there is no common attribute among these resulting tuples
	if (!allCommonAttributes.isEmpty()) {
	    String projectedTablePrefix = "projected"
		    + new java.util.Random().nextInt(100);

	    int projectedId = 0;
	    for (String rewriteTableName : allRewriteResultTables) {
		String projectedTableName = projectedTablePrefix + "_"
			+ Integer.toString(projectedId++);
		
		// create a new one if exists
		while (projectedTables.contains(projectedTableName)) {
		    projectedTableName = "projected"
			    + new java.util.Random().nextInt(100)
			    + "_" + Integer.toString(projectedId-1);
		}
		
		String query = "CREATE TABLE " + projectedTableName
			+ " AS SELECT " + selectClause + " FROM "
			+ rewriteTableName;
		stmt.executeUpdate(query);
		allProjectedRewriteResultTables.add(projectedTableName);
	    }
	}
	stmt.close();
    }

    static ArrayList<String> getAllCommonAttributes(Connection dbConn)
	    throws SQLException {
	PreProcessor.generateRewriteResultTable(dbConn);
	ArrayList<String> rewriteResultTableNames = PreProcessor
		.getRewriteResultTables(dbConn);
	return getAllCommonAttributes(rewriteResultTableNames, dbConn);
    }

    private static ArrayList<String> getAllCommonAttributes(
	    ArrayList<String> rewriteResultTableNames, Connection dbConn)
	    throws SQLException {
	PreProcessor.generateRewriteResultTable(dbConn);

	Statement stmt = dbConn.createStatement();
	ResultSet rs;

	// get all the column names of all rewritten result tables
	ArrayList<ArrayList<String>> allAttributes = 
		new ArrayList<ArrayList<String>>();
	for (String rewriteResultTableName : rewriteResultTableNames) {
	    ArrayList<String> attributes = new ArrayList<String>();
	    rs = stmt.executeQuery("DESC " + rewriteResultTableName);
	    while (rs.next()) {
		attributes.add(rs.getString(1));
	    }
	    allAttributes.add(attributes);
	    rs.close();
	}
	stmt.close();
	

	// here, only concern about the common attributes, not equivalent
	// attributes
	for (String attribute : allAttributes.get(0)) {
	    boolean isCommonAtttribute = true;
	    for (int i = 1; i < allAttributes.size(); i++) {
		if (!allAttributes.get(i).contains(attribute)) {
		    isCommonAtttribute = false;
		    break;
		}
	    }
	    if (isCommonAtttribute 
		    && !allCommonAttributes.contains(attribute)) {
		// debug
		System.out.println("debug:\tCommon attribute for projection: "
			+ attribute);
		allCommonAttributes.add(attribute);
	    }
	}

	return allCommonAttributes;
    }
    
    private static boolean isSizeMatch(ArrayList<String> allCommonAttributes, 
	    ArrayList<String> allRewriteResultTables, Connection dbConn) throws SQLException {
	int relationColumnSize = 0;
	Statement stmt = dbConn.createStatement();
	for (String relationName : allRewriteResultTables) {
	    int count = 0;
	    ResultSet rs = stmt.executeQuery("DESC " + relationName);
	    while (rs.next()) {
		count++;
	    }
	    rs.close();
	    if (relationColumnSize == 0) {
		relationColumnSize = count;
	    } else {
		if (relationColumnSize != count) {
		    return false;
		}
	    }
	}
	stmt.close();
	if(allCommonAttributes.size() != relationColumnSize) {
	    return false;
	} else {
	    return true;
	}
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
	getAllCommonAttributes(dbConn);
    }

}
