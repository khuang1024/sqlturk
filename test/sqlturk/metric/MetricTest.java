package sqlturk.metric;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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

public class MetricTest {

    private static Connection dbConn;
    private static ArrayList<String> originalQueries = new ArrayList<String>();
    private static ArrayList<String> rewriteQueries = new ArrayList<String>();

    @BeforeClass
    public static void initialize() throws SQLException {
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
		.add("select LifeExpectancy, Continent from Country where LifeExpectancy>81");
	originalQueries
		.add("select City.Name, LifeExpectancy, Continent from City, Country where LifeExpectancy<83 and LifeExpectancy>81");
	originalQueries
		.add("select LifeExpectancy lExpectancy, Region as Country_region from Country as ctry where LifeExpectancy<37.5");
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
	 * create FD
	 */
//	System.out.println("\n\nStart creating " + Parameters.FD_REL_NAME);
//	FD.createFDRelation(dbConn);
//	System.out.println("Finish creating " + Parameters.FD_REL_NAME
//		+ ".\n\n");

	/*
	 * create FD+
	 */
	System.out.println("\n\nStart creating " + Parameters.FD_PLUS_REL_NAME);
	FDPlus.createFDPlusRelation(dbConn);
	System.out.println("Finish creating " + Parameters.FD_PLUS_REL_NAME
		+ ".\n\n");

	/*
	 * create intersection
	 */
	System.out.println("\n\nStart creating "
		+ Parameters.INTERSECTION_REL_NAME);
	Intersection.createIntersectionRelation(dbConn);
	System.out.println("Finish creating "
		+ Parameters.INTERSECTION_REL_NAME + ".\n\n");

	/*
	 * create union
	 */
	System.out.println("\n\nStart creating " + Parameters.UNION_REL_NAME);
	Union.createUionRelation(dbConn);
	System.out.println("Finish creating " + Parameters.UNION_REL_NAME
		+ ".\n\n");

	/*
	 * NOTE: cannot drop all intermediate tables here, sicne Metric uses
	 * ColumnInfo to get the original names of attributes and relations,
	 * while ColumnInfo depends on the table COLUMN_INFO. Therefore, at
	 * least, COLUMN_INFO must be kept before Metric calculation work is
	 * done.
	 */
	// System.out.println("\n\nStart clearing intermediate tables");
	// Cleaner.dropIntermediateTables(dbConn);
	// System.out.println("Finish clearing intermediate tables.\n\n");

	System.out.println("Done.");
    }

    @AfterClass
    public static void clear() throws SQLException {
	// Cleaner.dropIntermediateTables(dbConn);
	// Cleaner.dropAll(dbConn);
    }

    @Test
    public void testSim() throws SQLException {
	assertEquals(1.0, Metric.sim("Q1_RES", Parameters.FD_REL_NAME, dbConn),
		0);
	assertEquals(1.0,
		Metric.sim("Q1_RES", Parameters.FD_PLUS_REL_NAME, dbConn), 0);
	assertEquals(0.0,
		Metric.sim("Q1_RES", Parameters.INTERSECTION_REL_NAME, dbConn),
		0);
	assertEquals(0.0,
		Metric.sim("Q1_RES", Parameters.UNION_REL_NAME, dbConn), 0);

	assertEquals(1.0, Metric.sim("Q2_RES", Parameters.FD_REL_NAME, dbConn),
		0);
	assertEquals(1.0,
		Metric.sim("Q2_RES", Parameters.FD_PLUS_REL_NAME, dbConn), 0);
	assertEquals(0.0,
		Metric.sim("Q2_RES", Parameters.INTERSECTION_REL_NAME, dbConn),
		0);
	assertEquals(0.0,
		Metric.sim("Q2_RES", Parameters.UNION_REL_NAME, dbConn), 0);

	assertEquals(0.26,
		Metric.sim(Parameters.FD_REL_NAME, "Q1_RES", dbConn), 0.007);
	assertEquals(0.26,
		Metric.sim(Parameters.FD_PLUS_REL_NAME, "Q1_RES", dbConn),
		0.007);
	assertEquals(0.0,
		Metric.sim(Parameters.INTERSECTION_REL_NAME, "Q1_RES", dbConn),
		0);
	assertEquals(0.0,
		Metric.sim(Parameters.UNION_REL_NAME, "Q1_RES", dbConn), 0);

	assertEquals(0.33,
		Metric.sim(Parameters.FD_REL_NAME, "Q2_RES", dbConn), 0.004);
	assertEquals(0.33,
		Metric.sim(Parameters.FD_PLUS_REL_NAME, "Q2_RES", dbConn),
		0.004);
	assertEquals(0.0,
		Metric.sim(Parameters.INTERSECTION_REL_NAME, "Q2_RES", dbConn),
		0);
	assertEquals(0.0,
		Metric.sim(Parameters.UNION_REL_NAME, "Q2_RES", dbConn), 0);
    }

}
