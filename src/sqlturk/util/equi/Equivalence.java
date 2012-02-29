package sqlturk.util.equi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;

public class Equivalence {
    
    
    private static ArrayList<ArrayList<String>> equivalentRewriteTables = new ArrayList<ArrayList<String>>();
    private static boolean isInitialized = false;
    
    private Equivalence() {
	throw new AssertionError();
    }
    
    
    
    public static ArrayList<String> getInequivalentResultTables(Connection dbConn) throws SQLException {
	ArrayList<String> inequivalentResultTables = new ArrayList<String>();
	if (!isInitialized) {
	    throw new RuntimeException("Equivalence is not initialized.");
	}
	for (int i = 0; i < equivalentRewriteTables.size(); i++) {
	    String rewriteName = equivalentRewriteTables.get(i).get(0);
	    inequivalentResultTables.add(rewriteName);
	}
	return inequivalentResultTables;
    }
    
    public static void init(Connection dbConn) throws SQLException {
	equivalentRewriteTables = new ArrayList<ArrayList<String>>();
	 
	isInitialized = true;
	
	ArrayList<String> allProjectedRewriteResultTables = PreProcessor.getRewriteResultTables(dbConn);
	for (String table : allProjectedRewriteResultTables) {
	    boolean isFound = false;
	    for (int i = 0 ; i < equivalentRewriteTables.size(); i++) {
		if (isEqual(table, equivalentRewriteTables.get(i).get(0), dbConn)) {
		    equivalentRewriteTables.get(i).add(table);
		    isFound = true;
		    break;
		}
	    }
	    if (!isFound) {
		ArrayList<String> newEquivalence = new ArrayList<String>();
		newEquivalence.add(table);
		equivalentRewriteTables.add(newEquivalence);
	    }
	}
	
	// transform the name and put them into equivalentResultTables
	for (int i = 0; i < equivalentRewriteTables.size(); i++) {
	    for (int j = 0; j < equivalentRewriteTables.get(i).size(); j++) {
		String old = equivalentRewriteTables.get(i).get(j);
		equivalentRewriteTables.get(i).set(j, old.substring(Parameters.QUERY_RESULT_REWRITE_PREFIX.length(), old.length() - Parameters.QUERY_RESULT_REWRITE_SUFFIX.length()));
	    }
	}
	
	for (int i = 0; i < equivalentRewriteTables.size(); i++) {
	    System.out.println("Equivalent Set #" + i + "  :---------------");
	    for (int j = 0; j < equivalentRewriteTables.get(i).size(); j++) {
		System.out.println(equivalentRewriteTables.get(i).get(j) + ", ");
	    }
	    System.out.println("-------------------------------------------");
	}
	PostProcessor.dropAll(dbConn);
    }
    
    private static boolean isEqual(String relation1, String relation2, Connection dbConn) throws SQLException {
	Statement stmt = null;
	ResultSet rs1 = null;
	ResultSet rs2 = null;
	ResultSetMetaData rsmd1 = null;
	ResultSetMetaData rsmd2 = null;
	try {
	    
	    ArrayList<String> attributeNames = new ArrayList<String>();
	    
	    stmt = dbConn.createStatement();
	    
	    // number of rows
	    rs1 = stmt.executeQuery("SELECT COUNT(*) FROM " + relation1);
	    rs1.next();
	    int nRows1 = rs1.getInt(1);
	    rs2 = stmt.executeQuery("SELECT COUNT(*) FROM " + relation2);
	    rs2.next();
	    int nRows2 = rs2.getInt(1);
	    if (nRows1 != nRows2) {
		return false;
	    }
	    
	    // number of columns
	    rs1 = stmt.executeQuery("SELECT * FROM " + relation1);
	    rsmd1 = rs1.getMetaData();
	    rs2 = stmt.executeQuery("SELECT * FROM " + relation2);
	    rsmd2 = rs2.getMetaData();
	    if (rsmd1.getColumnCount() != rsmd2.getColumnCount()) {
		return false;
	    }
	    
	    // name and type of columns
	    int nColumns = rsmd1.getColumnCount();
	    for (int i = 1; i <= nColumns; i++) {
		String label = rsmd1.getColumnLabel(i);
		String type = rsmd1.getColumnTypeName(i);
		for (int j = 1; j <= nColumns; j++) {
		    if (rsmd2.getColumnLabel(j).equals(label)) {
			if (!rsmd2.getColumnTypeName(j).equals(type)) {
			    return false;
			} else {
			    attributeNames.add(label);
			    break; // when find the same column
			}
		    }
		    if (j == nColumns) {
			return false;
		    }
		}
	    }
	    
	    
	    if (attributeNames.size() != nColumns) {
		System.out.println(attributeNames.size());
		System.out.println(nColumns);
		throw new RuntimeException("Dismatch.");
	    }
	    
	    // values of columns
	    String attributes = "";
	    for (int i = 0; i < nColumns; i++) {
		if (i < nColumns - 1) {
		    attributes += attributeNames.get(i) + ", ";
		} else {
		    attributes += attributeNames.get(i);
		}
	    }
	    rs1 = stmt.executeQuery("SELECT COUNT(*) FROM "+relation1+" INNER JOIN "+relation2+" USING(" + attributes + ")");
	    rs1.next();
	    if (rs1.getInt(1) != nRows1) {
		return false;
	    } else {
		return true;
	    }
	} catch (SQLException e) {
	    e.printStackTrace();
	    return false;
	} finally {
	    if (rsmd1 != null) {
		rsmd1 = null;
	    }
	    if (rsmd2 != null) {
		rsmd2 = null;
	    }
	    if (rs1 != null) {
		try {
		    rs1.close();
		} catch (SQLException e) {
		    e.printStackTrace();
		}
	    }
	    if (rs2 != null) {
		try {
		    rs2.close();
		} catch (SQLException e) {
		    e.printStackTrace();
		}
	    }
	    if (stmt != null) {
		try {
		    stmt.close();
		} catch (SQLException e) {
		    e.printStackTrace();
		}
	    }
	    System.out.println("Closed all.");
	}
    }
    
    public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
	Class.forName("com.mysql.jdbc.Driver").newInstance();
	Connection dbConn;

	// use soe server or test locally
	if (Parameters.USE_SERVER) {
	    dbConn = DriverManager.getConnection(Parameters.MYSQL_CONNECTION_STRING);
	    System.out.println("Using server.");
	} else {
	    dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL, Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
	    System.out.println("Using local.");
	}
	
	while (true) {
	    ArrayList<String> test = getInequivalentResultTables(dbConn);
	    for (String t: test) {
		System.out.println(t);
	    }
	    try {
		Thread.sleep(5000);
	    } catch (InterruptedException e) {
		e.printStackTrace();
	    }
	}
//	System.out.println("t = " + isEqual("t1", "t8", dbConn));
//	System.out.println("t = " + isEqual("t3", "t4", dbConn));
//	System.out.println("t = " + isEqual("t7", "t10", dbConn));
//	System.out.println("f = " + isEqual("t1", "t3", dbConn));
//	System.out.println("f = " + isEqual("t7", "t3", dbConn));
//	System.out.println("f = " + isEqual("t9", "t3", dbConn));
//	System.out.println("f = " + isEqual("t9", "t7", dbConn));
//	System.out.println("f = " + isEqual("t1", "t2", dbConn));
//	System.out.println("f = " + isEqual("t1", "t7", dbConn));
//	System.out.println("f = " + isEqual("t4", "t2", dbConn));
//	System.out.println("f = " + isEqual("t7", "t9", dbConn));
//	
//	init(dbConn);

//	dbConn.close();
    }

}
