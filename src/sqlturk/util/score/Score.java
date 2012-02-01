package sqlturk.util.score;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;

import sqlturk.configuration.Parameters;

public class Score {
    private Score() {
	throw new AssertionError();
    }

    public static void createFDPlusScoreRelation(Connection dbConn)
	    throws SQLException {
	// ArrayList<Tuple> allFDPlusTuples =
	// getAllTuples(Parameters.FD_PLUS_REL_NAME, dbConn);

    }

    public static double score(Tuple tuple, Connection dbConn)
	    throws SQLException {
	double score = 0;

	// check if this tuple is from FD+
	if (!tuple.getSource().equals(Parameters.FD_PLUS_REL_NAME)) {
	    throw new RuntimeException("Not a tuple in "
		    + Parameters.FD_PLUS_REL_NAME);
	}

	// calculate score
	ArrayList<Tuple> contributeTuples = getAllContributeTuples(tuple,
		dbConn);
	for (int i = 0; i < contributeTuples.size(); i++) {
	    for (int j = i + 1; j < contributeTuples.size(); j++) {
		score += simTuple(contributeTuples.get(i),
			contributeTuples.get(j), dbConn);
	    }
	}

	return score;
    }

    static double simTuple(Tuple tuple1, Tuple tuple2, Connection dbConn)
	    throws SQLException {
	// the two tuples must come from query result tables
	if (!tuple1.getSchema().contains(Parameters.ROWID_ATT_NAME)
		|| !tuple2.getSchema().contains(Parameters.ROWID_ATT_NAME)) {
	    throw new RuntimeException("No " + Parameters.ROWID_ATT_NAME
		    + " found.");
	}

	ArrayList<HashSet<String>> provenance1 = getProvenanceSets(tuple1,
		dbConn);
	ArrayList<HashSet<String>> provenance2 = getProvenanceSets(tuple2,
		dbConn);

	double max = simProvenanceSet(provenance1.get(0), provenance2.get(0));
	for (HashSet<String> prov1 : provenance1) {
	    for (HashSet<String> prov2 : provenance2) {
		double currentSim = simProvenanceSet(prov1, prov2);
		if (max < currentSim) {
		    max = currentSim;
		}
	    }
	}

	return max;
    }

    /*
     * use Jaccard
     */
    static double simProvenanceSet(HashSet<String> source1,
	    HashSet<String> source2) {
	HashSet<String> intersection = new HashSet<String>();
	HashSet<String> union = new HashSet<String>();

	// compute intersection
	for (String src1 : source1) {
	    for (String src2 : source2) {
		if (src1.equals(src2)) {
		    intersection.add(src1);
		    break;
		}
	    }
	}

	// compute union
	for (String src1 : source1) {
	    union.add(src1);
	}
	for (String src2 : source2) {
	    union.add(src2);
	}

	double intersectionSize = (double) intersection.size();
	double unionSize = (double) union.size();

	return intersectionSize / unionSize;
    }

    static ArrayList<HashSet<String>> getProvenanceSets(Tuple tuple,
	    Connection dbConn) throws SQLException {
	ArrayList<HashSet<String>> provenanceSets = new ArrayList<HashSet<String>>();
	ArrayList<String> schema = tuple.getSchema();
	String rowId = "";

	// check if this tuple is from query result table
	if (!schema.contains(Parameters.ROWID_ATT_NAME)) {
	    throw new RuntimeException("No " + Parameters.ROWID_ATT_NAME
		    + " found.");
	}

	// find out its __ROWID
	for (int i = 0; i < tuple.getSchema().size(); i++) {
	    if (tuple.getSchema().get(i).equals(Parameters.ROWID_ATT_NAME)) {
		rowId = tuple.getValues().get(i);
		break;
	    }
	}

	// get all the DERIV and SRC
	ArrayList<String> deriv = new ArrayList<String>();
	ArrayList<String> src = new ArrayList<String>();
	Statement stmt = dbConn.createStatement();
	String query = "SELECT " + Parameters.DERIVATION_NUMBER_ATT + ", "
		+ Parameters.SOURCE_NUMBER_ATT + " FROM "
		+ Parameters.PROV_REL_NAME + " WHERE "
		+ Parameters.RESULT_ID_ATT + "='" + rowId + "'";
	ResultSet rs = stmt.executeQuery(query);
	while (rs.next()) {
	    deriv.add(rs.getString(Parameters.DERIVATION_NUMBER_ATT));
	    src.add(rs.getString(Parameters.SOURCE_NUMBER_ATT));
	}

	// process the DERIV and SRC
	String lastDriv = deriv.get(0);
	HashSet<String> hs = new HashSet<String>();
	hs.add(src.get(0));
	provenanceSets.add(hs);
	for (int i = 1; i < src.size(); i++) {
	    if (lastDriv.equals(deriv.get(i))) {
		provenanceSets.get(provenanceSets.size() - 1).add(src.get(i));
	    } else {
		lastDriv = deriv.get(i);
		HashSet<String> newHs = new HashSet<String>();
		newHs.add(src.get(i));
		provenanceSets.add(newHs);
	    }
	}

	// debug
	System.out
		.println("debug:\tThe provenance sets of " + tuple.getRowId());
	int i = 0;
	for (HashSet<String> provenanceSet : provenanceSets) {
	    for (String source : provenanceSet) {
		System.out.println("debug:\tset[" + i + "] src = " + source);
	    }
	    i++;
	}
	System.out.println("debug:\t------");

	return provenanceSets;
    }

    static ArrayList<Tuple> getAllContributeTuples(Tuple tuple,
	    Connection dbConn) throws SQLException {
	ArrayList<Tuple> allContributeTuples = new ArrayList<Tuple>();

	Statement stmt = dbConn.createStatement();

	// first, get all result tables
	ArrayList<String> resultTables = new ArrayList<String>();
	ResultSet rs = stmt.executeQuery("SHOW TABLES");
	while (rs.next()) {
	    if (rs.getString(1).startsWith(Parameters.QUERY_RESULT_PREFIX)
		    && rs.getString(1).endsWith(Parameters.QUERY_RESULT_SUFFIX)) {
		resultTables.add(rs.getString(1));
	    }
	}

	// check each tuple in all the result tables to see if it contributes to
	// the tuple in FD+
	for (int i = 0; i < resultTables.size(); i++) {
	    String tableName = resultTables.get(i);
	    ArrayList<Tuple> tuples = getAllTuples(tableName, dbConn);
	    for (int j = 0; j < tuples.size(); j++) {
		if (tuple.isContributedBy(tuples.get(j), dbConn)) {
		    allContributeTuples.add(tuples.get(j));
		}
	    }
	}

	rs.close();
	stmt.close();

	// debug
	System.out.println("debug:\tThe contribute tuples of "
		+ tuple.getInfo());
	for (Tuple contributeTuple : allContributeTuples) {
	    System.out.println("debug:\t" + contributeTuple.getRowId());
	}
	System.out.println("debug:\t------");

	return allContributeTuples;
    }

    /*
     * get all the tuples in the given table.
     */
    static ArrayList<Tuple> getAllTuples(String tableName, Connection dbConn)
	    throws SQLException {
	ArrayList<Tuple> tuples = new ArrayList<Tuple>();

	Statement stmt = dbConn.createStatement();
	ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
	ResultSetMetaData rsmd = rs.getMetaData();

	// evaluate the schema
	ArrayList<String> schema = new ArrayList<String>();
	int nColumn = rsmd.getColumnCount();
	for (int i = 1; i <= nColumn; i++) {
	    schema.add(rsmd.getColumnLabel(i));
	}

	// evaluate the values of tuples
	while (rs.next()) {
	    ArrayList<String> values = new ArrayList<String>();
	    for (int i = 0; i < schema.size(); i++) {
		values.add(rs.getString(schema.get(i)));
	    }
	    tuples.add(new Tuple(tableName, schema, values));
	}

	rs.close();
	stmt.close();

	return tuples;
    }
}
