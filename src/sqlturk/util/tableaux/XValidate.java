package sqlturk.util.tableaux;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;
import sqlturk.experiment.provenance.QueryManager;

public class XValidate {

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static void main(String[] args) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
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
		Statement stmt = dbConn.createStatement();
		
		
		int[] answerIndices = {1,2,3,4,5,6,7,8,9,10};
		for (int queryIndex = 0; queryIndex < 8; queryIndex++) {
			ArrayList<String> originalQueries = QueryManager.getRankedQueries(
					"world", queryIndex, answerIndices);
			ArrayList<String> rewrittenQueries = XRewriter.getRewrittenQueries(originalQueries, dbConn);
			for (String query : rewrittenQueries) {
				
				System.out.println(query + " LIMIT 2");
				stmt.executeQuery(query + " LIMIT 2");
			}
		}
		for (int queryIndex = 0; queryIndex < 8; queryIndex++) {
			ArrayList<String> originalQueries = QueryManager.getRankedQueries(
					"tpch", queryIndex, answerIndices);
			ArrayList<String> rewrittenQueries = XRewriter.getRewrittenQueries(originalQueries, dbConn);
			for (String query : rewrittenQueries) {
				
				System.out.println(query + " LIMIT 2");
				stmt.executeQuery(query + " LIMIT 2");
			}
		}
		
		
		stmt.close();
		
	    }

	}
