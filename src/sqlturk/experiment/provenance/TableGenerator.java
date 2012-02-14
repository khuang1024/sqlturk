package sqlturk.experiment.provenance;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;
import sqlturk.util.cleaner.Cleaner;
import sqlturk.util.connected.Connected;
import sqlturk.util.fd.normal.FD;
import sqlturk.util.fd.plus.FDPlus;
import sqlturk.util.intersection.Intersection;
import sqlturk.util.metric.Metric;
import sqlturk.util.provenance.Provenance;
import sqlturk.util.query.QueryExecutor;
import sqlturk.util.tableaux.TableauxRewriter;
import sqlturk.util.union.Union;

/**
 * This class is for testing the whole process of SQLTurk.
 * 
 *
 */
public class TableGenerator {

    /**
     * @param args
     * @throws SQLException 
     * @throws IOException 
     */
    public static void main(String[] args) throws SQLException, IOException {

	try {
	    Class.forName("com.mysql.jdbc.Driver").newInstance();
	    Connection dbConn;
	    
	    /*
	     * use soe server or test locally
	     */
	    if (Parameters.USE_SERVER) {
		dbConn = DriverManager.getConnection(Parameters.MYSQL_CONNECTION_STRING);
		System.out.println("Using server.");
	    } else {
		dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL, Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
		System.out.println("Using local.");
	    }
	    
	    /*
	     * create the standard answer result table
	     */
	    String standardQuery = "select Name as Country_Name from Country where Region ='Western Europe'";
	    Statement stmt = dbConn.createStatement();
	    stmt.executeUpdate("DROP TABLE IF EXISTS " + Parameters.STANDARD_ANSWER_REL_NAME);
	    stmt.executeUpdate("CREATE TABLE " + Parameters.STANDARD_ANSWER_REL_NAME + " AS " + standardQuery);
	    stmt.close();
	    
	    
	    /*
	     * initialize the orginal query batch, odered by the their ranks
	     */
	    ArrayList<String> originalQueries = new ArrayList<String>();
	    originalQueries.add("select Name from Country where Region like 'Western Europe'"); // right answer
	    originalQueries.add("select Name from Country where Region = 'Western Europe'");
	    originalQueries.add("select Country.Name from Country where Country.Region='Western Europe'");
	    originalQueries.add("select Name from Country where Region='Europe'");
	    originalQueries.add("SELECT * FROM Country WHERE Region='Western Europe'");
	    
	    
	   
	    
	    /*
	     * clean old intermediate tables
	     */
	    System.out.println("Start clearing old intermediate and result tables ......");
	    Cleaner.dropAll(dbConn);
	    System.out.println("Finish clearing old intermediate and result tables.\n");
	    
	    /*
	     * rewrite the queries
	     */
	    ArrayList<String> rewriteQueries = new ArrayList<String>();
	    System.out.println("Start rewriting queries ...............");
	    rewriteQueries = TableauxRewriter.getRewriteQueries(originalQueries, dbConn);
	    System.out.println("Finish rewriting queries.\n");
	    
	    /*
	     * execute the queries
	     */
	    System.out.println("Start executing queries ...............");
	    QueryExecutor.executeQueries(rewriteQueries, dbConn);
	    System.out.println("Finish executing queries.\n");
	    
	    /*
	     * create PROV table
	     */
	    System.out.println("Start creating " + Parameters.PROV_REL_NAME + " ........");
	    Provenance.createWhyProvenanceRelation(rewriteQueries, dbConn);
	    System.out.println("Finish creating " + Parameters.PROV_REL_NAME + ".\n");
	    
	    /*
	     * create CONN table
	     */
	    System.out.println("Start creating " + Parameters.CONN_REL_NAME + " ..................");
	    Connected.createWhyConnectedRelation(dbConn);
	    System.out.println("Finish creating " + Parameters.CONN_REL_NAME + ".\n");
	    
	    /*
	     * create FD
	     */
	    System.out.println("Start creating " + Parameters.FD_REL_NAME + " ...............");
	    FD.createFDRelation(dbConn);
	    System.out.println("Finish creating " + Parameters.FD_REL_NAME + ".\n");
	    
	    /*
	     * create FD+
	     */
	    System.out.println("Start creating " + Parameters.FD_PLUS_REL_NAME + " .............");
	    FDPlus.createFDPlusRelation(dbConn);
	    System.out.println("Finish creating " + Parameters.FD_PLUS_REL_NAME + ".\n");
	    
	    /*
	     * create intersection
	     */
	    System.out.println("Start creating " + Parameters.INTERSECTION_REL_NAME + " .............");
	    Intersection.createIntersectionRelation(dbConn);
	    System.out.println("Finish creating " + Parameters.INTERSECTION_REL_NAME + ".\n");
	    
	    /*
	     * create union
	     */
	    System.out.println("Start creating " + Parameters.UNION_REL_NAME + " ............");
	    Union.createUionRelation(dbConn);
	    System.out.println("Finish creating " + Parameters.UNION_REL_NAME + ".\n");
	    
	    /*
	     * metric
	     */
	    System.out.println("Start evaluating performance by metric ........");
	    System.out.println("standard vs intersection: " + Metric.sim(Parameters.STANDARD_ANSWER_REL_NAME, Parameters.INTERSECTION_REL_NAME, dbConn));
	    System.out.println("standard vs union: " + Metric.sim(Parameters.STANDARD_ANSWER_REL_NAME, Parameters.UNION_REL_NAME, dbConn));
	    System.out.println("standard vs fd: " + Metric.sim(Parameters.STANDARD_ANSWER_REL_NAME, Parameters.FD_REL_NAME, dbConn));
	    System.out.println("standard vs fd+: " + Metric.sim(Parameters.STANDARD_ANSWER_REL_NAME, Parameters.FD_PLUS_REL_NAME, dbConn));
	    
	    
	    /*
	     * evaluate the results
	     */
//	    System.out.println("sim(Q1_RES, Q2_RES) = " + Metric.sim("Q1_RES", "Q2_RES", dbConn));
//	    System.out.println("sim(Q2_RES, Q1_RES) = " + Metric.sim("Q2_RES", "Q1_RES", dbConn));
	    
	    /*
	     * clear intermediate tables
	     */
	    System.out.println("Start clearing intermediate tables ............");
	    Cleaner.dropIntermediateTables(dbConn);
	    System.out.println("Finish clearing intermediate tables.\n");

	    System.out.println("Done.");
	} catch (InstantiationException e) {
	    e.printStackTrace();
	} catch (IllegalAccessException e) {
	    e.printStackTrace();
	} catch (ClassNotFoundException e) {
	    e.printStackTrace();
	}

    }

}
