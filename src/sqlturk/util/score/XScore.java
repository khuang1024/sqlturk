package sqlturk.util.score;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import sqlturk.configuration.Parameters;
import sqlturk.util.common.XCommon;
import sqlturk.util.common.XTuple;

public class XScore {
    private XScore() {
	throw new AssertionError();
    }

    public static void test(Connection dbConn)
	    throws SQLException {
	ArrayList<XTuple> fdpTuples = XCommon.getTuples("WORLD_QUERY0_TOP8_FROM10_FD_PLUS", dbConn);
	System.out.println("SCORE = " + score(fdpTuples.get(0), dbConn));
//	for (XTuple t : fdpTuples) {
//	    System.out.println(t.valueString());
//	    System.out.println("SCORE = " + score(t, dbConn));
//	}
    }

    public static double score(XTuple tuple, Connection dbConn)
	    throws SQLException {
	double score = 0;

	// check if this tuple is from FD+
	if (!tuple.getTable().endsWith(Parameters.FD_PLUS_REL_NAME)) {
	    throw new RuntimeException("Not a tuple in "
		    + Parameters.FD_PLUS_REL_NAME);
	}

	// calculate score
	ArrayList<XTuple> contributeTuples = getAllContributeTuples(tuple,
		dbConn);
	for (int i = 0; i < contributeTuples.size(); i++) {
	    for (int j = i + 1; j < contributeTuples.size(); j++) {
		score += sim(contributeTuples.get(i),
			contributeTuples.get(j), dbConn);
	    }
	}

	return score;
    }

    static double sim(XTuple tuple1, XTuple tuple2, Connection dbConn)
	    throws SQLException {

	ArrayList<ArrayList<String>> provenance1 = getProvenanceSets(tuple1, dbConn);
	ArrayList<ArrayList<String>> provenance2 = getProvenanceSets(tuple2, dbConn);

	if (provenance1.isEmpty() || provenance2.isEmpty()) {
	    return 0.0;
	} else {
	    double max = sim(provenance1.get(0), provenance2.get(0));
	    for (ArrayList<String> prov1 : provenance1) {
		for (ArrayList<String> prov2 : provenance2) {
		    if (max < sim(prov1, prov2)) {
			max = sim(prov1, prov2);
		    }
		}
	    }
	    return max;
	}
	
    }

    /*
     * use Jaccard
     */
    private static double sim(ArrayList<String> source1, ArrayList<String> source2) {
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

	return (double) intersection.size() / union.size();
    }

    private static ArrayList<ArrayList<String>> getProvenanceSets(XTuple tuple,
	    Connection dbConn) throws SQLException {
	
	ArrayList<ArrayList<String>> provenanceSets = new ArrayList<ArrayList<String>>();
	
	String rowId = tuple.getValues().get(tuple.getSchema().indexOf(Parameters.ROWID_ATT_NAME));
	
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
	rs.close();
	stmt.close();

	// process the DERIV and SRC
	HashMap <String, ArrayList<String>> hm = new HashMap <String, ArrayList<String>>();
	for (int i = 0; i < deriv.size(); i++) {
	    if (hm.keySet().contains(deriv.get(i))) {
		hm.get(deriv.get(i)).add(src.get(i));
	    } else {
		ArrayList<String> srcs = new ArrayList<String>();
		srcs.add(src.get(i));
		hm.put(deriv.get(i), srcs);
	    }
	}
	
	for (String key : hm.keySet()) {
	    provenanceSets.add(hm.get(key));
	}

	return provenanceSets;
    }

    private static ArrayList<XTuple> getAllContributeTuples(XTuple tuple,
	    Connection dbConn) throws SQLException {
	ArrayList<XTuple> allContributeTuples = new ArrayList<XTuple>();
	// first, get all result tables
	ArrayList<String> resultTables = XCommon.getAllResultTables(dbConn);

	// check each tuple in all the result tables to see if it contributes to
	// the tuple in FD+
	for (int i = 0; i < resultTables.size(); i++) {
	    String tableName = resultTables.get(i);
	    ArrayList<XTuple> resultTuples = XCommon.getTuples(tableName, dbConn);
	    for (int j = 0; j < resultTuples.size(); j++) {
		if (contributes(resultTuples.get(j), tuple)) {
		    allContributeTuples.add(resultTuples.get(j));
		}
	    }
	}
	
	return allContributeTuples;
    }

    /**
     * Return true if tuple1 contributes to tuple2. Namely, tuple2 is possibily constructed by tuple1.
     * @param t1
     * @param t2
     * @return
     */
    private static boolean contributes(XTuple tuple1, XTuple tuple2) {
	
	for (String column : tuple1.getSchema()) {
	    if (!column.equals(Parameters.ROWID_ATT_NAME)) {
		if (!tuple2.getSchema().contains(column)) {
		    // if tuple1 has a column which not exists in tuple2, then
		    // tuple1 can't contribute to create tuple2.
		    return false;
		} else {
		    String value1 = tuple1.getValues().get(tuple1.getSchema().indexOf(column));
		    String value2 = tuple2.getValues().get(tuple2.getSchema().indexOf(column));
		    // if tuple2 has a non-null column, then this value must be equal
		    // to the same column in tuple1. Otherwise, tuple2 can't contribute
		    // to tuple1.
		    if (value1 != null) {
			if (!value1.equals(value2)) {
			    return false;
			}
		    }
		}
	    }
	}
	
	return true;
    }
    
    public static void main(String[] args) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
	Class.forName("com.mysql.jdbc.Driver").newInstance();
	Connection dbConn = null;
	if (Parameters.USE_SERVER) {
	    dbConn = DriverManager.getConnection(Parameters.MYSQL_CONNECTION_STRING);
	    System.out.println("Using server.\n");
	} else {
	    dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL, Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
	    System.out.println("Using local.\n");
	}
	
	test(dbConn);
    }
    
}
