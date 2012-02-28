package sqlturk.experiment.provenance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;

public class DropUnion {

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
	    ArrayList<String> unions = new ArrayList<String>();
	    while (rs.next()) {
		String tableName = rs.getString(1);
		if (tableName.endsWith(Parameters.UNION_REL_NAME)) {
		    unions.add(tableName);
		}
	    }
	    for (String union : unions) {
		stmt.executeUpdate("DROP TABLE " + union);
	    }
	    rs.close();
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
