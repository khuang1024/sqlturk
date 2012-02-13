package sqlturk.experiment.mturk;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;


//import sqlturk.util.DBUtil;
import sqlturk.util.connected.Connected;
import sqlturk.util.fd.normal.FD;
import sqlturk.util.fd.plus.FDPlus;
import sqlturk.util.intersection.Intersection;
import sqlturk.util.metric.Metric;
import sqlturk.util.provenance.Provenance;
import sqlturk.util.query.QueryExecutor;
//import sqlturk.util.rowid.RowIdAugmenter;
import sqlturk.util.tableaux.TableauxRewriter;
import sqlturk.util.union.Union;
import sqlturk.configuration.Parameters;
import sqlturk.util.cleaner.Cleaner;

/**
 * This class is for testing the whole process of SQLTurk.
 * 
 *
 */
public class SQLTurk {

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
	     * initialize the orginal query batch
	     */
//	    ArrayList<String> originalQueries = QueryInitializer.getAllAnswers("??");
	    ArrayList<String> originalQueries = new ArrayList<String>();
	    originalQueries.add("select Name from Country where Region ='Western Europe'"); // right answer
	    originalQueries.add("SELECT Name FROM Country WHERE Region='Western Europe'");
	    originalQueries.add("SELECT * FROM Country WHERE Region = 'Western Europe'");
//	    originalQueries.add("select Name from Country where region = 'Western Europe'");
//	    originalQueries.add("select Name from Country, CountryLanguage where Country.Continent='Europe' and Country.Region='Western'");
	    
	    
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
	     * evaluate the results
	     */
	    System.out.println("sim(Q1_RES, Q2_RES) = " + Metric.sim("Q1_RES", "Q2_RES", dbConn));
	    System.out.println("sim(Q2_RES, Q1_RES) = " + Metric.sim("Q2_RES", "Q1_RES", dbConn));
	    
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
