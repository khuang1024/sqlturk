package sqlturk.util.score;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import sqlturk.configuration.Parameters;
import sqlturk.util.cleaner.Cleaner;
import sqlturk.util.connected.Connected;
import sqlturk.util.fd.normal.FD;
import sqlturk.util.fd.plus.FDPlus;
import sqlturk.util.intersection.Intersection;
import sqlturk.util.provenance.Provenance;
import sqlturk.util.query.QueryExecutor;
import sqlturk.util.tableaux.TableauxRewriter;
import sqlturk.util.union.Union;

public class ScoreTest {

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
	Cleaner.dropAll(dbConn);
    }

    @Test
    public void testScore() throws SQLException {

	System.out.println();
	System.out.println();
	ArrayList<Tuple> allFDPlusTuples = Score.getAllTuples(
		Parameters.FD_PLUS_REL_NAME, dbConn);

	double score = 0;
	System.out.println("==========================");
	assertEquals(0.0, score = Score.score(allFDPlusTuples.get(0), dbConn),
		0);
	System.out.println("debug:\tscore=" + score);
	System.out.println("==========================");
	assertEquals(0.5, score = Score.score(allFDPlusTuples.get(1), dbConn),
		0);
	System.out.println("debug:\tscore=" + score);
	System.out.println("==========================");
	assertEquals(0.5, score = Score.score(allFDPlusTuples.get(2), dbConn),
		0);
	System.out.println("debug:\tscore=" + score);
	System.out.println("==========================");
	assertEquals(0.5, score = Score.score(allFDPlusTuples.get(3), dbConn),
		0);
	System.out.println("debug:\tscore=" + score);
	System.out.println("==========================");
	assertEquals(0.0, score = Score.score(allFDPlusTuples.get(4), dbConn),
		0);
	System.out.println("debug:\tscore=" + score);
	System.out.println("==========================");
	assertEquals(0.0, score = Score.score(allFDPlusTuples.get(5), dbConn),
		0);
	System.out.println("debug:\tscore=" + score);

	// modify the PROV table to test
	System.out.println("==========================");
	Statement stmt = dbConn.createStatement();
	stmt.executeUpdate("INSERT INTO " + Parameters.PROV_REL_NAME
		+ " VALUES (2000000003, 1, 104907)");
	assertEquals(1, score = Score.score(allFDPlusTuples.get(1), dbConn), 0);
	System.out.println("debug:\tscore=" + score);
	stmt.executeUpdate("DELETE FROM " + Parameters.PROV_REL_NAME
		+ " WHERE " + Parameters.RESULT_ID_ATT + "=2000000003 AND "
		+ Parameters.DERIVATION_NUMBER_ATT + "=1 AND "
		+ Parameters.SOURCE_NUMBER_ATT + "=104907");

	// modify the PROV table to test
	System.out.println("==========================");
	stmt.executeUpdate("INSERT INTO " + Parameters.PROV_REL_NAME
		+ " VALUES (2000000003, 0, 104907)");
	assertEquals(0.5, score = Score.score(allFDPlusTuples.get(1), dbConn),
		0);
	System.out.println("debug:\tscore=" + score);
	stmt.executeUpdate("DELETE FROM " + Parameters.PROV_REL_NAME
		+ " WHERE " + Parameters.RESULT_ID_ATT + "=2000000003 AND "
		+ Parameters.DERIVATION_NUMBER_ATT + "=0 AND "
		+ Parameters.SOURCE_NUMBER_ATT + "=104907");

	// modify the PROV table to test
	System.out.println("==========================");
	stmt.executeUpdate("INSERT INTO " + Parameters.PROV_REL_NAME
		+ " VALUES (2000000003, 1, 000000)");
	assertEquals(0.33, score = Score.score(allFDPlusTuples.get(1), dbConn),
		0.004);
	System.out.println("debug:\tscore=" + score);
	stmt.executeUpdate("DELETE FROM " + Parameters.PROV_REL_NAME
		+ " WHERE " + Parameters.RESULT_ID_ATT + "=2000000003 AND "
		+ Parameters.DERIVATION_NUMBER_ATT + "=1 AND "
		+ Parameters.SOURCE_NUMBER_ATT + "=000000");

	// modify the PROV table to test
	System.out.println("==========================");
	stmt.executeUpdate("INSERT INTO " + Parameters.PROV_REL_NAME
		+ " VALUES (2000000003, 1, 104907)");
	stmt.executeUpdate("INSERT INTO " + Parameters.PROV_REL_NAME
		+ " VALUES (2000000003, 1, 000000)");
	assertEquals(0.66, score = Score.score(allFDPlusTuples.get(1), dbConn),
		0.007);
	System.out.println("debug:\tscore=" + score);
	stmt.executeUpdate("DELETE FROM " + Parameters.PROV_REL_NAME
		+ " WHERE " + Parameters.RESULT_ID_ATT + "=2000000003 AND "
		+ Parameters.DERIVATION_NUMBER_ATT + "=1 AND "
		+ Parameters.SOURCE_NUMBER_ATT + "=104907");
	stmt.executeUpdate("DELETE FROM " + Parameters.PROV_REL_NAME
		+ " WHERE " + Parameters.RESULT_ID_ATT + "=2000000003 AND "
		+ Parameters.DERIVATION_NUMBER_ATT + "=1 AND "
		+ Parameters.SOURCE_NUMBER_ATT + "=000000");
	System.out.println("==========================");
    }

    @Ignore
    public void testSimTuple() throws SQLException {
	ArrayList<Tuple> allFDPlusTuples = Score.getAllTuples(
		Parameters.FD_PLUS_REL_NAME, dbConn);
	ArrayList<Tuple> allContributeTuples = Score.getAllContributeTuples(
		allFDPlusTuples.get(3), dbConn);
	for (int i = 0; i < allContributeTuples.size(); i++) {
	    System.out.println("***");
	    System.out.println(allContributeTuples.get(i).getInfo());
	    System.out.println("***");
	}
	/*
	 * Example explanation: The 3rd tuple in FD+ is [81.1, Europe, San
	 * Marino, null, null] This tuple is contributed by tuple1 = [ 81.1,
	 * Europe, 2000000005] from Q1_RES and tuple2 = [San Marino, 81.1,
	 * Europe, 2000000011] from Q2_RES
	 * 
	 * 2000000005 has sources: {361}, null 2000000011 has sources: {106341,
	 * 361}, null Since there is only one provenance set for 2000000005 and
	 * 2000000011, simTuple(2000000005, 2000000011) =
	 * simProvenanceSet({361}, {106341, 361}) = 1/2
	 */
	assertEquals(
		0.5,
		Score.simTuple(allContributeTuples.get(0),
			allContributeTuples.get(1), dbConn), 0);
    }

    @Ignore
    public void testSimProvenanceSet() {
	HashSet<String> source1 = new HashSet<String>();
	HashSet<String> source2 = new HashSet<String>();

	source1.add("1");
	source1.add("2");
	source1.add("3");
	source2.add("1");
	source2.add("2");
	source2.add("4");
	assertEquals(0.5, Score.simProvenanceSet(source1, source2), 0);

	source1.clear();
	source2.clear();
	source1.add("1");
	source1.add("2");
	source1.add("3");
	source1.add("4");
	source2.add("1");
	source2.add("5");
	source2.add("6");
	assertEquals(0.16, Score.simProvenanceSet(source1, source2), 0.007);
    }

    @Ignore
    public void testGetProvenanceSets() throws SQLException {
	ArrayList<Tuple> tuples = Score.getAllTuples("Q2_RES", dbConn);
	for (int i = 0; i < tuples.size(); i++) {
	    Tuple tuple = tuples.get(i);
	    Score.getProvenanceSets(tuple, dbConn);
	}
    }

    @Ignore
    public void testGetAllContributeTuples() throws SQLException {
	ArrayList<Tuple> allFDPlusTuples = Score.getAllTuples(
		Parameters.FD_PLUS_REL_NAME, dbConn);
	ArrayList<Tuple> allContributeTuples = Score.getAllContributeTuples(
		allFDPlusTuples.get(3), dbConn);
	for (int i = 0; i < allContributeTuples.size(); i++) {
	    System.out.println(allContributeTuples.get(i).getInfo());
	}
    }

    @Test
    public void testGetAllTuples() {
    }

}
