package sqlturk.util.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;

public class XCommon {
    
    private XCommon() {}
    
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
	
//	ArrayList<String> rels = new ArrayList<String>();
//	rels.add("Q0_RES");
//	rels.add("Q1_RES");
//	rels.add("Q10_RES");
//	ArrayList<String> att = XCommon.getCommonCols(rels, dbConn);
//	System.out.println(att.toString());
	
	XTuple tp1 = new XTable("Q0_RES", dbConn).getTuple(0);
	XTuple tp2 = new XTable("Q1_RES", dbConn).getTuple(0);
	XTuple tp3 = new XTable("TEST", dbConn).getTuple(0);
	XTuple tp4 = new XTable("TEST2", dbConn).getTuple(0);
	
	ArrayList<XTuple> tps = new ArrayList<XTuple>();
	tps.add(tp1);
	tps.add(tp2);
//	tps.add(tp3);
	tps.add(tp4);
	
	System.out.println(XCommon.getCommonCols(tps).toString());
    }

}
