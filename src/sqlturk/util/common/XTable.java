package sqlturk.util.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;


public class XTable {
    
    private String name;
    private ArrayList<String> schema;
    private ArrayList<XTuple> tuples;
    
    public XTable(String rel, Connection dbConn) throws SQLException {
	this.name = rel;
	this.schema = new ArrayList<String>();
	this.tuples = new ArrayList<XTuple>();
	
	// schema
	Statement stmt = dbConn.createStatement();
	ResultSet rs = stmt.executeQuery("DESC " + rel);
	while (rs.next()) {
	    this.schema.add(rs.getString(1));
	}
	
	// tuples
	rs = stmt.executeQuery("SELECT " + XCommon.flattenByComma(this.schema) + " FROM " + rel);
	while (rs.next()) {
	    ArrayList<String> values = new ArrayList<String>();
	    for (int i = 1; i <= this.schema.size(); i++) {
		values.add(rs.getString(i));
	    }
	    this.tuples.add(new XTuple(this.name, this.schema, values));
	}
	
	rs.close();
	stmt.close();
    }
    
    @Override
    public String toString() {
	String info = "Table:\n";
	info += "--Name: " + this.name + "\n";
	info += "--Schema: " + XCommon.flattenByComma(this.schema) + "\n";
	info += "--Tuples: (" + this.tuples.size() + ")\n";
	for (XTuple t : this.tuples) {
	    info += "\t" + t.valueString() + "\n";
	}
	return info;
    }
    
    public int size() {
	return this.tuples.size();
    }
    
    public XTuple getTuple(int i) {
	return this.tuples.get(i);
    }
    
    public static void main(String[] agrs) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
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
	
	String rel = "Q0_RES";
	System.out.println(new XTable(rel, dbConn).toString());
    }
}
