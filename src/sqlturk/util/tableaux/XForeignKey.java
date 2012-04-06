package sqlturk.util.tableaux;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import sqlturk.configuration.Parameters;

public class XForeignKey {
    
    private String rel;
    private Connection dbConn;
    private HashMap<String, String> hm;
    
    XForeignKey(String rel, Connection dbConn) throws SQLException {
	this.rel = rel;
	this.dbConn = dbConn;
	this.hm = new HashMap<String, String>(); // <original col, referenced col>
	init();
    }
    
    ArrayList<String> getAllRefTables() {
	ArrayList<String> refTables = new ArrayList<String>();
	for (String s : hm.keySet()) {
	    refTables.add(hm.get(s).split("\\.")[0]);
	}
	return refTables;
    }
    
    ArrayList<String> getFKStrings() throws SQLException{
	ArrayList<String> fk = new ArrayList<String>();
	for (String k : hm.keySet()) {
	    fk.add(k + "=" + hm.get(k));
//	    System.out.println(k + "=" + hm.get(k));
	}
	return fk;
    }
    
    private void init() throws SQLException {
	Statement stmt = dbConn.createStatement();
	ResultSet rs = stmt.executeQuery(getFKQuery(rel));
	while (rs.next()) {
	    String table = rs.getString("TABLE_NAME");
	    String col = rs.getString("COLUMN_NAME");
	    String refTable = rs.getString("REFERENCED_TABLE_NAME");
	    String refCol = rs.getString("REFERENCED_COLUMN_NAME");
	    this.hm.put(table + "." + col, refTable + "." + refCol);
//	    System.out.println(table + "." + col + "->" + refTable + "." + refCol);
	}
	rs.close();
	stmt.close();
    }
        
    private static String getFKQuery(String rel) {
	String query = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, " +
			"REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, " +
			"REFERENCED_COLUMN_NAME " +
			"FROM information_schema.KEY_COLUMN_USAGE " +
			"WHERE TABLE_NAME='" + rel + "' " +
			"AND REFERENCED_TABLE_SCHEMA<>'NULL' " +
			"AND REFERENCED_TABLE_NAME<>'NULL' " +
			"AND REFERENCED_COLUMN_NAME<>'NULL'";
	return query;
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
	
	new XForeignKey("LINEITEM", dbConn).getFKStrings();
	new XForeignKey("LINEITEM", dbConn).getAllRefTables();
    }

}
