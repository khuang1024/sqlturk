package sqlturk.util.connected;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import sqlturk.configuration.Parameters;
import sqlturk.util.common.XCommon;
import sqlturk.util.common.XTuple;
import sqlturk.util.common.XTupleSet;

public class XConnect {
    
    private static ArrayList<String> inserted = new ArrayList<String>();

    private XConnect() {
	throw new AssertionError();
    }

    /**
     * Create the Why-Connected table for the current database. This function
     * will read the result tables (the name of a result table is specified in
     * the Parameters.java file, and this function will use that information to
     * identify result tables) and determine which tables are connected and
     * generate the CONN table based on the connectedness information.
     * 
     * @param dbConn
     *            the connection of the current database
     * @throws SQLException
     */
    public static void createWhyConnectedRelation(Connection dbConn)
	    throws SQLException {
	Statement stmt = dbConn.createStatement();
	
	ArrayList<String> resultTables = XCommon.getAllResultTables(dbConn);
	ArrayList<XTuple> resultTuples = new ArrayList<XTuple>();
	
	for (String table : resultTables) {
	    resultTuples.addAll(XCommon.getTuples(table, dbConn));
	}
	
	stmt.executeUpdate("DROP TABLE IF EXISTS " + Parameters.CONN_REL_NAME);
	stmt.executeUpdate("CREATE TABLE " + Parameters.CONN_REL_NAME
		    + " (" + Parameters.CONN_TUPLE1_ID_ATT + " int(11), "
		    + Parameters.CONN_TUPLE2_ID_ATT + " int(11))");
	stmt.close();

	populateWhyConnectedRelation(dbConn, resultTuples);
	
	inserted = null;
    }

    private static void populateWhyConnectedRelation(Connection dbConn,
	    ArrayList<XTuple> tuples) throws SQLException {

	minimizeWitnessSet(dbConn); // minimize the witness set.

	// check if they are connected for each pair of tuples
	for (int i = 0; i < tuples.size(); i++) {
	    for (int j = i + 1; j < tuples.size(); j++) {
		computeTwoTupleWhyConnected(tuples.get(i), tuples.get(j),
			Parameters.CONN_REL_NAME, dbConn);
	    }
	}
    }

    private static void computeTwoTupleWhyConnected(XTuple tuple1, XTuple tuple2,
	    String tableName, Connection dbConn) throws SQLException {

	// condition 1: t.X=t'.X for each common attribute X
	XTupleSet tupleSet = new XTupleSet(tuple1);
	if (tupleSet.jcc(tuple2)) {
	    // condition 2: exists w in why(t), exists w' in why(t'), such that
	    // w \cup w' is not null
	    hasSameProvenance(tuple1, tuple2, tableName, dbConn);
	}
    }

    private static boolean hasSameProvenance(XTuple tuple1, XTuple tuple2,
	    String tableName, Connection dbConn) throws SQLException {
	
	int indexOfRowId1 = tuple1.getSchema().indexOf(Parameters.ROWID_ATT_NAME);
	int indexOfRowId2 = tuple2.getSchema().indexOf(Parameters.ROWID_ATT_NAME);
	String rowId1 = tuple1.getValues().get(indexOfRowId1);
	String rowId2 = tuple2.getValues().get(indexOfRowId2);

	/*
	 * get the witness sets for these two tuples
	 */
	HashMap<String, HashSet<String>> witnessSetOfTuple1 = getWitnessSet(
		rowId1, dbConn);
	HashMap<String, HashSet<String>> witnessSetOfTuple2 = getWitnessSet(
		rowId2, dbConn);

	// check if they have same sets
	Statement stmt = dbConn.createStatement();
	for (String derivation1 : witnessSetOfTuple1.keySet()) {
	    for (String derivation2 : witnessSetOfTuple2.keySet()) {

		for (String src1 : witnessSetOfTuple1.get(derivation1)) {
		    for (String src2 : witnessSetOfTuple2.get(derivation2)) {
			if (src1.equals(src2)) {
			    
			    String insert = "INSERT INTO " + tableName + " VALUES ('" + rowId1 + "', '" + rowId2 + "')";
			    if (!inserted.contains(insert)) {
				inserted.add(insert);
				stmt.executeUpdate(insert);
			    }
			    return true;
			}
		    }
		}
	    }
	}
	
	stmt.close();
	return false;
    }

    /*
     * get the witness set for a specific __ROWID
     */
    private static HashMap<String, HashSet<String>> getWitnessSet(
	    String __ROWID, Connection dbConn) {
	HashMap<String, HashSet<String>> witnessSet = new HashMap<String, HashSet<String>>();

	try {
	    Statement stmt = dbConn.createStatement();

	    String query = "";
	    query += "select distinct " + Parameters.DERIVATION_NUMBER_ATT
		    + " from " + Parameters.PROV_REL_NAME + " where "
		    + Parameters.ROWID_ATT_NAME + "=" + __ROWID;
	    ResultSet rs = stmt.executeQuery(query);
	    ArrayList<String> distinctDERIV = new ArrayList<String>();
	    while (rs.next()) {
		distinctDERIV.add(rs.getString(1));
	    }
	    rs.close();

	    for (String DERIV : distinctDERIV) {
		String queryWitnessSet = "select "
			+ Parameters.SOURCE_NUMBER_ATT + " from "
			+ Parameters.PROV_REL_NAME + " where "
			+ Parameters.DERIVATION_NUMBER_ATT + "=" + DERIV
			+ " and " + Parameters.ROWID_ATT_NAME + "=" + __ROWID;
		ResultSet tmpRs = stmt.executeQuery(queryWitnessSet);
		HashSet<String> tmpWitnessSet = new HashSet<String>();
		while (tmpRs.next()) {
		    tmpWitnessSet.add(tmpRs.getString(1));
		}
		tmpRs.close();
		witnessSet.put(DERIV, tmpWitnessSet);
	    }
	    stmt.close();

	} catch (SQLException e) {
	    System.err.println("JDBC Exception.");
	    e.printStackTrace();
	}

	return witnessSet;
    }

    /*
     * minimize the witness set
     */
    private static void minimizeWitnessSet(Connection dbConn) {

	// first, remove the fully-identical items.
	removeIdenticalWitnessSet(dbConn);

	String query = "";

	query += "delete\n";
	query += "from " + Parameters.PROV_REL_NAME + "\n";
	query += "where (" + Parameters.PROV_REL_NAME + "."
		+ Parameters.ROWID_ATT_NAME + ", " + Parameters.PROV_REL_NAME
		+ "." + Parameters.DERIVATION_NUMBER_ATT + ") in \n";
	query += "  (select * from\n";
	query += "    (\n";
	query += "    select " + Parameters.ROWID_ATT_NAME + " as bad_row_id, "
		+ Parameters.DERIVATION_NUMBER_ATT + " as bad_deriv\n";
	query += "    from\n";
	query += "      (select distinct " + Parameters.ROWID_ATT_NAME + ", "
		+ Parameters.DERIVATION_NUMBER_ATT + "\n";
	query += "      from " + Parameters.PROV_REL_NAME + ") uniqProv\n";
	query += "      where exists\n";
	query += "        (\n";
	query += "          select " + Parameters.ROWID_ATT_NAME + ", "
		+ Parameters.DERIVATION_NUMBER_ATT + "\n";
	query += "          from\n";
	query += "            (select distinct " + Parameters.ROWID_ATT_NAME
		+ ", " + Parameters.DERIVATION_NUMBER_ATT + "\n";
	query += "              from " + Parameters.PROV_REL_NAME
		+ ") uniqProv2\n";
	query += "              where\n";
	query += "                uniqProv." + Parameters.ROWID_ATT_NAME
		+ "=uniqProv2." + Parameters.ROWID_ATT_NAME + "\n";
	query += "                and uniqProv."
		+ Parameters.DERIVATION_NUMBER_ATT + "<>uniqProv2."
		+ Parameters.DERIVATION_NUMBER_ATT + "\n";
	query += "                and\n";
	query += "                not exists\n";
	query += "                  (\n";
	query += "                  select *\n";
	query += "                  from " + Parameters.PROV_REL_NAME + "\n";
	query += "                  where " + Parameters.ROWID_ATT_NAME
		+ "=uniqProv." + Parameters.ROWID_ATT_NAME + " and "
		+ Parameters.DERIVATION_NUMBER_ATT + "=uniqProv2."
		+ Parameters.DERIVATION_NUMBER_ATT + "\n";
	query += "                    and " + Parameters.SOURCE_NUMBER_ATT
		+ " not in\n";
	query += "                    (\n";
	query += "                      select src_id from "
		+ Parameters.PROV_REL_NAME + " where "
		+ Parameters.ROWID_ATT_NAME + "=uniqProv."
		+ Parameters.ROWID_ATT_NAME + " and "
		+ Parameters.DERIVATION_NUMBER_ATT + "=uniqProv."
		+ Parameters.DERIVATION_NUMBER_ATT + "\n";
	query += "                    )\n";
	query += "                  )\n";
	query += "        )\n";
	query += "    ) as tmp);\n";

	// System.out.println("debug:\tquery=" + query);

	try {
	    Statement stmt = dbConn.createStatement();
	    stmt.executeUpdate(query);
	    stmt.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
    }

    /*
     * remove the fully-identical items. Refer to Bogdan's implementation note:
     * need to be careful: if there are two identical witness sets for the same
     * result tuple, they will be both pruned out
     */
    private static void removeIdenticalWitnessSet(Connection dbConn) {
	String update = "";

	// drop tmp table if it exists
	try {
	    Statement stmt = dbConn.createStatement();
	    update = "DROP TABLE IF EXISTS tmp1";
	    stmt.executeUpdate(update);
	    update = "DROP TABLE IF EXISTS tmp2";
	    stmt.executeUpdate(update);
	    update = "DROP TABLE IF EXISTS tmp3";
	    stmt.executeUpdate(update);
	    stmt.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	update = "";

	// record the totally same tuples, just using UNION
	try {
	    update += "create table tmp1\n";
	    update += "as\n";
	    update += "select * from " + Parameters.PROV_REL_NAME + "\n";
	    update += "group by " + Parameters.ROWID_ATT_NAME + ", "
		    + Parameters.DERIVATION_NUMBER_ATT + ", "
		    + Parameters.SOURCE_NUMBER_ATT + " having count(*)>1\n";
	    Statement stmt = dbConn.createStatement();
	    stmt.executeUpdate(update);
	    stmt.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	update = "";

	// remove the tuples with same __ROWID and SRC_ID
	try {
	    update += "create table tmp2\n";
	    update += "as\n";
	    update += "select * from " + Parameters.PROV_REL_NAME + "\n";
	    update += "where " + Parameters.DERIVATION_NUMBER_ATT + " in\n";
	    update += "	(select min(" + Parameters.DERIVATION_NUMBER_ATT
		    + ") from " + Parameters.PROV_REL_NAME + " group by "
		    + Parameters.ROWID_ATT_NAME + ", "
		    + Parameters.SOURCE_NUMBER_ATT + ")\n";
	    Statement stmt = dbConn.createStatement();
	    stmt.executeUpdate(update);
	    stmt.close();
	} catch (SQLException e) {
	    System.err.println("JDBC Exception!");
	    e.printStackTrace();
	}
	update = "";

	// union tmp1 and tmp2
	try {
	    update += "create table tmp3\n";
	    update += "as\n";
	    update += "select * from tmp2\n";
	    update += "union\n";
	    update += "select * from tmp1\n";
	    update += "order by " + Parameters.ROWID_ATT_NAME + ", "
		    + Parameters.DERIVATION_NUMBER_ATT;
	    Statement stmt = dbConn.createStatement();
	    stmt.executeUpdate(update);
	    stmt.close();
	} catch (SQLException e) {
	    System.err.println("JDBC Exception!");
	    e.printStackTrace();
	}
	update = "";

	// drop old PROV and rename table tmp3 to PROV
	try {
	    Statement stmt = dbConn.createStatement();
	    update = "DROP TABLE IF EXISTS " + Parameters.PROV_REL_NAME + "\n";
	    stmt.executeUpdate(update);
	    update = "RENAME TABLE tmp3 TO " + Parameters.PROV_REL_NAME + "";
	    stmt.executeUpdate(update);
	    stmt.close();
	} catch (SQLException e) {
	    System.err.println("JDBC Exception!");
	    e.printStackTrace();
	}
	update = "";

	// clear up the temporary tables
	try {
	    Statement stmt = dbConn.createStatement();
	    update = "DROP TABLE IF EXISTS tmp1";
	    stmt.executeUpdate(update);
	    update = "DROP TABLE IF EXISTS tmp2";
	    stmt.executeUpdate(update);
	    update = "DROP TABLE IF EXISTS tmp3";
	    stmt.executeUpdate(update);
	    stmt.close();
	} catch (SQLException e) {
	    System.err.println("JDBC Exception!");
	    e.printStackTrace();
	}
    }
}
