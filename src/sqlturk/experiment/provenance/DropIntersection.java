package sqlturk.experiment.provenance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;

public class DropIntersection {

    /**
     * @param args
     * @throws SQLException 
     */
    public static void main(String[] args) throws SQLException {
	try {
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
	    
	    Statement stmt = dbConn.createStatement();
	    ResultSet rs = stmt.executeQuery("SHOW TABLES");
	    ArrayList<String> intersections = new ArrayList<String>();
	    while (rs.next()) {
		String tableName = rs.getString(1);
		if (tableName.endsWith(Parameters.INTERSECTION_REL_NAME)) {
		    intersections.add(tableName);
		}
	    }
	    rs.close();
	    for (String intersection : intersections) {
		stmt.executeUpdate("DROP TABLE " + intersection);
	    }
	    stmt.close();
	    
	} catch (InstantiationException e) {
	    e.printStackTrace();
	} catch (IllegalAccessException e) {
	    e.printStackTrace();
	} catch (ClassNotFoundException e) {
	    e.printStackTrace();
	}

    }

}
