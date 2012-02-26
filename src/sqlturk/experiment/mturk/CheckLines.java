package sqlturk.experiment.mturk;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;
import sqlturk.util.tableaux.TableauxRewriter;

public class CheckLines {
    
    private CheckLines() {
	throw new AssertionError();
    }

    /**
     * @param args
     * @throws SQLException 
     */
    public static void main(String[] args) throws SQLException {
	Connection dbConn;
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
	
	System.out.println("World");
	for (int i = 0; i < 8; i++) {
	    System.out.println("\tQuery " + i);
//	    ArrayList<String> worlds = TableauxRewriter.getRewriteQueries(SQLTurkHITs.get("world", i), dbConn);
	    ArrayList<String> worlds = SQLTurkHITs.get("world", i);
	    for (int j = 0; j < worlds.size(); j++) {
//		ResultSet rs = stmt.executeQuery(worlds.get(j));
		ResultSet rs = stmt.executeQuery(worlds.get(j) + " ORDER BY RAND() LIMIT 100");
		int nRows = 0;
		while (rs.next()) {
		    nRows ++;
		}
		System.out.println("\t\tCandidate " + j + ": " + nRows );
	    }
	}
	
	System.out.println("TPCH");
	for (int i = 0; i < 8; i++) {
	    System.out.println("\tQuery " + i);
//	    ArrayList<String> tpch = TableauxRewriter.getRewriteQueries(SQLTurkHITs.get("tpch", i), dbConn);
	    ArrayList<String> tpch = SQLTurkHITs.get("tpch", i);
	    for (int j = 0; j < tpch.size(); j++) {
//		ResultSet rs = stmt.executeQuery(tpch.get(j));
		ResultSet rs = stmt.executeQuery(tpch.get(j) + " ORDER BY RAND() LIMIT 100");
		int nRows = 0;
		while (rs.next()) {
		    nRows ++;
		}
		System.out.println("\t\tCandidate " + j + ": " + nRows );
	    }
	}
	

    }

}
