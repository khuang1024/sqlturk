package sqlturk.util.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;

public class XCommon {
    
    private XCommon() {
	throw new AssertionError();
    }
    
    public static String flattenByComma(ArrayList<String> array) {
	String string = "";
	for (String e : array) {
	    string += e + ", ";
	}
	string = string.substring(0, string.length() - 2);
	return string;
    }
    
    public static ArrayList<String> getAllCols(ArrayList<String> rels, Connection dbConn) throws SQLException {
	
	ArrayList<String> allColNames = new ArrayList<String>();
	
	Statement stmt = dbConn.createStatement();
	ResultSet rs = null;
	
	for (int i = 0; i < rels.size(); i++) {
	    rs = stmt.executeQuery("DESC " + rels.get(i));
	    while (rs.next()) {
		String colName = rs.getString(1);
		if (!allColNames.contains(colName)) {
		    allColNames.add(colName);
		}
	    }
	}
	
	// remove __ROWID
	while (allColNames.contains(Parameters.ROWID_ATT_NAME)) {
	    allColNames.remove(Parameters.ROWID_ATT_NAME);
	}
	
	rs.close();
	stmt.close();
	
	return allColNames;
	
    }
    
    
    public static ArrayList<String> getCommonCols(ArrayList<XTuple> tuples) {
	if (tuples.size() == 1) {
	    throw new RuntimeException("Error: Only on tuple.");
	}
	
	ArrayList<String> currCommonAtts = tuples.get(0).getCommonAtts(tuples.get(1));
	
	for (int i = 2; i < tuples.size(); i++) {
	    ArrayList<String> newCommonAtts = tuples.get(i).getCommonAtts(currCommonAtts);
	    currCommonAtts = newCommonAtts;
	}
	
	return currCommonAtts;
    }
    
    public static ArrayList<String> getCommonCols(
	    ArrayList<String> rels, Connection dbConn) throws SQLException {
	
	ArrayList<String> commonColNames = new ArrayList<String>();
	Statement stmt = dbConn.createStatement();
	ResultSet rs = stmt.executeQuery("DESC " + rels.get(0));
	while (rs.next()) {
	    commonColNames.add(rs.getString(1));
	}
	
	for (int i = 1; i < rels.size(); i++) {
	    rs = stmt.executeQuery("DESC " + rels.get(i));
	    ArrayList<String> cols = new ArrayList<String>();
	    while (rs.next()) {
		cols.add(rs.getString(1));
	    }
	    for (int j = 0; j < commonColNames.size(); j++) {
		if (!cols.contains(commonColNames.get(j))) {
		    commonColNames.remove(j);
		    j--;
		}
	    }
	}
	
	// remove __ROWID
	while (commonColNames.contains(Parameters.ROWID_ATT_NAME)) {
	    commonColNames.remove(Parameters.ROWID_ATT_NAME);
	}
	
	rs.close();
	stmt.close();
	
	return commonColNames;
    }
    
    public static ArrayList<String> getAllResultTables(Connection dbConn) throws SQLException {
	ArrayList<String> tables = XCommon.getAllTables(dbConn);
	ArrayList<String> resultTables = new ArrayList<String>();
	for (String table : tables) {
	    if (table.startsWith(Parameters.QUERY_RESULT_PREFIX)
		    && table.endsWith(Parameters.QUERY_RESULT_SUFFIX)) {
		resultTables.add(table);
	    }
	}
	return resultTables;
    }
    
    public static ArrayList<String> getAllTables(Connection dbConn) throws SQLException {
	ArrayList<String> allTables = new ArrayList<String>();
	
	Statement stmt = dbConn.createStatement();
	ResultSet rs = stmt.executeQuery("SHOW TABLES");
	while (rs.next()) {
	    allTables.add(rs.getString(1));
	}
	
	rs.close();
	stmt.close();
	
	return allTables;
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
	Connection dbConn = null;
	// use soe server or test locally
	if (Parameters.USE_SERVER) {
	    dbConn = DriverManager.getConnection(Parameters.MYSQL_CONNECTION_STRING);
	    System.out.println("Using server.\n");
	} else {
	    dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL, Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
	    System.out.println("Using local.\n");
	}
    }

}
