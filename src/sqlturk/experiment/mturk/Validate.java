package sqlturk.experiment.mturk;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;
import sqlturk.util.tableaux.TableauxRewriter;

public class Validate {
    
    private Validate() {
	throw new AssertionError();
    }

    /**
     * @param args
     * @throws SQLException 
     */
    public static void main(String[] args){
	Connection dbConn = null;
	if (Parameters.USE_SERVER) {
	    try {
		dbConn = DriverManager
		    .getConnection(Parameters.MYSQL_CONNECTION_STRING);
	    } catch (SQLException e) {
		e.printStackTrace();
	    }
	    System.out.println("Using server.");
	} else {
	    try {
		dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL,
		    Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
	    } catch (SQLException e) {
		e.printStackTrace();
	    }
	    System.out.println("Using local.");
	}
	
	
	
	Statement stmt = null;
	
	for (int i = 0; i < 8; i++) {
	    ArrayList<String> queries = SQLTurkHITs.get("world", i);
	    for (int j = 0; j < queries.size(); j++) {
		try {
		    stmt = dbConn.createStatement();
//		    System.out.println("CREATE TABLE X AS " + queries.get(j) + " LIMIT 200");
		    stmt.execute("DROP TABLE IF EXISTS X");
		    stmt.execute("CREATE TABLE X AS " + queries.get(j) + " LIMIT 200");
		    System.out.println("Correct: dataset=world, #" + i + ", #" + (j+1));
		    stmt.execute("DROP TABLE X");
		} catch (SQLException e) {
		    System.out.println("Problematic: dataset=world, #" + i + ", #" + (j+1));
		} finally {
		    if (stmt != null) {
			try {
			    stmt.close();
			} catch (SQLException e) {
			    e.printStackTrace();
			}
		    }
		}
	    }
	}
	for (int i = 0; i < 8; i++) {
	    ArrayList<String> queries = SQLTurkHITs.get("tpch", i);
	    for (int j = 0; j < queries.size(); j++) {
		try {
		    stmt = dbConn.createStatement();
		    stmt.execute("DROP TABLE IF EXISTS X");
		    stmt.execute("CREATE TABLE X AS " + queries.get(j) + " LIMIT 200");
		    System.out.println("Correct: dataset=tpch, #" + i + ", #" + (j+1));
		    stmt.execute("DROP TABLE X");
		} catch (SQLException e) {
		    System.out.println("Problematic: dataset=tpch, #" + i + ", #" + (j+1));
		    e.printStackTrace();
		} finally {
		    if (stmt != null) {
			try {
			    stmt.close();
			} catch (SQLException e) {
			    e.printStackTrace();
			}
		    }
		}
		
	    }
	}
    }

}
