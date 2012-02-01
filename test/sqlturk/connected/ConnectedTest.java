package sqlturk.connected;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

import org.junit.Test;

import sqlturk.configuration.Parameters;
import sqlturk.util.cleaner.Cleaner;
import sqlturk.util.connected.Connected;
import sqlturk.util.provenance.Provenance;
import sqlturk.util.query.QueryExecutor;
import sqlturk.util.tableaux.TableauxRewriter;

public class ConnectedTest {

    private static Connection dbConn;
    private static ArrayList<String> originalQueries = new ArrayList<String>();
    private static ArrayList<String> rewriteQueries = new ArrayList<String>();

    @Test
    public void testCreateWhyConnectedRelation() throws SQLException {
	if (Parameters.USE_SERVER) {
	    dbConn = DriverManager
		    .getConnection(Parameters.MYSQL_CONNECTION_STRING);
	    System.out.println("Using server.");
	} else {
	    dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL,
		    Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
	    System.out.println("Using local.");
	}

	originalQueries
		.add("select Name, LifeExpectancy, Continent from Country where LifeExpectancy>81");
	originalQueries
		.add("select Name, LifeExpectancy, from Country where LifeExpectancy>80");
	originalQueries
		.add("select City.Name, LifeExpectancy, Continent from City, Country where LifeExpectancy<83 and LifeExpectancy>81");
	originalQueries
		.add("select LifeExpectancy, Region as Country_region from Country as ctry where LifeExpectancy<37.5");
	originalQueries
		.add("select City.Name, IndepYear from City, Country where LifeExpectancy > 83");

	/*
	 * clean old intermediate tables
	 */
	System.out.println("\n\nStart clearing old intermediate and result "
		+ "tables.");
	Cleaner.dropAll(dbConn);
	System.out.println("Finish clearing old intermediate and result "
		+ "tables.\n\n");

	/*
	 * rewrite the queries
	 */
	System.out.println("\n\nStart rewriting queries.");
	rewriteQueries = TableauxRewriter.getRewriteQueries(originalQueries,
		dbConn);
	System.out.println("Finish rewriting queries.\n\n");

	/*
	 * execute the queries
	 */
	System.out.println("\n\nStart executing queries.");
	QueryExecutor.executeQueries(rewriteQueries, dbConn);
	System.out.println("Finish executing queries.\n\n");

	/*
	 * create PROV table
	 */
	System.out.println("\n\nStart creating " + Parameters.PROV_REL_NAME);
	Provenance.createWhyProvenanceRelation(rewriteQueries, dbConn);
	System.out.println("Finish creating " + Parameters.PROV_REL_NAME
		+ ".\n\n");

	/*
	 * create CONN table
	 */
	System.out.println("\n\nStart creating " + Parameters.CONN_REL_NAME);
	Connected.createWhyConnectedRelation(dbConn);
	System.out.println("Finish creating " + Parameters.CONN_REL_NAME
		+ ".\n\n");

	/*
	 * clear intermediate tables
	 */
	System.out.println("\n\nStart clearing intermediate tables");
	Cleaner.dropIntermediateTables(dbConn);
	System.out.println("Finish clearing intermediate tables.\n\n");

	System.out.println("Done.");
    }

}
