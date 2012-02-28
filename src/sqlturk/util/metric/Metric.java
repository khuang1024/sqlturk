package sqlturk.util.metric;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import sqlturk.configuration.Parameters;

/**
 * Design: put all the columns which are in the same foreign key chain into a
 * pool (ArrayList<String>) 1. extract the FK constraints for each result
 * tables. 2. sort these FK constraints into a related pool
 * 
 */
public class Metric {

    private static ArrayList<ForeignKeyChain> foreignKeyChains = 
	    new ArrayList<ForeignKeyChain>();
    private static HashMap<String, ArrayList<Tuple>> tuplesOfRelation = 
	    new HashMap<String, ArrayList<Tuple>>();
    private static boolean isInitialized = false;

    private Metric() {
	throw new AssertionError();
    }

    /**
     * Compute the Sim of two relations. NOTE: Parameter order is important!
     * 
     * @param relationName1
     * @param relationName2
     * @param dbConn
     * @return
     * @throws SQLException
     */
    public static double sim(String relationName1, String relationName2,
	    Connection dbConn) throws SQLException {
	ArrayList<Tuple> tuplesOfRelation1 = getAllTuples(relationName1, dbConn);
	// ArrayList<Tuple> tuplesOfRelation2 = getAllTuples(relationName2,
	// dbConn);
	double sum = 0.0;
	double size = (double) tuplesOfRelation1.size();
	for (int i = 0; i < tuplesOfRelation1.size(); i++) {
	    // System.out.println("debug:\tsim tuple = " +
	    // sim(tuplesOfRelation1.get(i), relationName2, dbConn));
	    sum += sim(tuplesOfRelation1.get(i), relationName2, dbConn);
	}
	System.out.println("debug:\t[" + relationName1 + ", " + relationName2
		+ "]\t sum = " + sum);
	System.out.println("debug:\t[" + relationName1 + ", " + relationName2
		+ "]\t size = " + size);
	if (size == 0) { // if the resulting table is null
	    return 0;
	} else {
	    return sum / size;
	}
	
    }

    private static double sim(Tuple tup, String relationName, Connection dbConn)
	    throws SQLException {
	if (!isInitialized) {
	    initializeForeignKeyChain(dbConn);
	    isInitialized = true;
	}

	ArrayList<Tuple> tuples = getAllTuples(relationName, dbConn);

	if (tuples.isEmpty()) {
	    return 0.0;
	} else {
	    double max = tup.sim(tuples.get(0), dbConn);
	    // System.out.println("debug:\t"+"0=" + max);
	    for (int i = 1; i < tuples.size(); i++) {
		double tmp = tup.sim(tuples.get(i), dbConn);
		// System.out.println("debug:\t"+i+"=" + tmp);
		if (tmp > max) {
		    max = tmp;
		}
	    }
	    return max;
	}

	// // debug
	// for (Tuple tup : tuples) {
	// for (String v : tup.getValues()) {
	// System.out.print(v + ",");
	// }
	// System.out.println();
	// }
	//
	// // debug
	// System.out.println("debug:\t"+tuples.get(1).sim(tuples.get(2)));
    }

    private static ArrayList<Tuple> getAllTuples(String relationName,
	    Connection dbConn) throws SQLException {

	ArrayList<String> schema = new ArrayList<String>();
	ArrayList<Tuple> tuples = new ArrayList<Tuple>();

	if (tuplesOfRelation.containsKey(relationName)) {
	    return tuplesOfRelation.get(relationName);
	} else {
	    Statement stmt = dbConn.createStatement();
	    ResultSet rs = stmt.executeQuery("SELECT * FROM " + relationName);
	    ResultSetMetaData rsmd = rs.getMetaData();
	    int nColumn = rsmd.getColumnCount();
	    for (int i = 0; i < nColumn; i++) {
		schema.add(rsmd.getColumnName(i + 1));
	    }
	    while (rs.next()) {
		ArrayList<String> values = new ArrayList<String>();
		for (int i = 0; i < schema.size(); i++) {
		    values.add(rs.getString(schema.get(i)));
		}
		tuples.add(new Tuple(relationName, schema, values));
	    }
	    rs.close();
	    stmt.close();
	    tuplesOfRelation.put(relationName, tuples);
	    return tuples;
	}

    }

    static boolean isInSameForeignKeyChain(Attribute att1, Attribute att2) {
	for (ForeignKeyChain fkc : foreignKeyChains) {
	    if (fkc.hasAttribute(att1) && fkc.hasAttribute(att2)) {
		return true;
	    }
	}
	return false;
    }

    /*
     * generate the foreign key chain. VALIDATED.
     */
    private static void initializeForeignKeyChain(Connection dbConn)
	    throws SQLException {
	Statement stmt = dbConn.createStatement();
	ResultSet rs = stmt.executeQuery("SHOW TABLES");
	ArrayList<String> fkTables = new ArrayList<String>();
	while (rs.next()) {
	    String table = rs.getString(1);
	    if (table.startsWith(Parameters.FK_CONSTRAINT_TABLE_PREFIX)
		    && table.endsWith(Parameters.FK_CONSTRAINT_TABLE_SUFFIX)) {
		fkTables.add(table);
		// System.out.println(table);
	    }
	}
	for (String table : fkTables) {
	    rs = stmt.executeQuery("SELECT * FROM " + table);
	    while (rs.next()) {
		String columnName1 = rs
			.getString(Parameters.FK_CONSTRAINT_COLUMN_COLUMN_NAME);
		String columnName2 = rs
			.getString(Parameters.FK_CONSTRAINT_COLUMN_REFERENCED_COLUMN_NAME);
		Attribute att1 = new Attribute(columnName1);
		Attribute att2 = new Attribute(columnName2);
		boolean isNew = true;
		for (int i = 0; i < foreignKeyChains.size(); i++) {
		    ForeignKeyChain fkChain = foreignKeyChains.get(i);
		    if (fkChain.hasAttribute(att1)
			    && !fkChain.hasAttribute(att2)) {
			fkChain.addAttribute(att2);
			isNew = false;
			break;
		    } else if (fkChain.hasAttribute(att2)
			    && !fkChain.hasAttribute(att1)) {
			fkChain.addAttribute(att1);
			isNew = false;
			break;
		    } else if (fkChain.hasAttribute(att1)
			    && fkChain.hasAttribute(att2)) {
			isNew = false;
			break;
		    }
		}
		if (isNew) {
		    ForeignKeyChain foreignKeyChain = new ForeignKeyChain();
		    foreignKeyChain.addAttribute(att1);
		    foreignKeyChain.addAttribute(att2);
		    foreignKeyChains.add(foreignKeyChain);
		}
	    }
	    rs.close();
	}
	stmt.close();

	// // debug
	// for (ForeignKeyChain fkc : foreignKeyChains) {
	// System.out.println("debug:\tChain ID=" + fkc.getChainId());
	// System.out.println("debug:\t\t" + fkc.getChainString());
	// }
    }

}
