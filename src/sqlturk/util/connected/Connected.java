package sqlturk.util.connected;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import sqlturk.configuration.Parameters;
import sqlturk.util.map.ColumnInfo;

public class Connected {

    private Connected() {
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
	ResultSet rs = stmt.executeQuery("SHOW TABLES");
	ArrayList<String> resultRelationNames = new ArrayList<String>();
	ArrayList<Tuple> resultTuples = new ArrayList<Tuple>();

	// put the query result tables into an array
	while (rs.next()) {
	    if (rs.getString(1).startsWith(Parameters.QUERY_RESULT_PREFIX)
		    && rs.getString(1).endsWith(Parameters.QUERY_RESULT_SUFFIX)) {
		resultRelationNames.add(rs.getString(1));
	    }
	}

	// get all the tuples and their schemas
	for (String resultRelationName : resultRelationNames) {
	    ArrayList<String> schema = new ArrayList<String>();

	    ResultSet rsTuples = stmt.executeQuery("SELECT * FROM "
		    + resultRelationName);
	    ResultSetMetaData rsmd = rsTuples.getMetaData();
	    int nColumn = rsmd.getColumnCount();

	    // get the schema of the relation
	    for (int i = 1; i <= nColumn; i++) {
		schema.add(rsmd.getColumnLabel(i));
	    }

	    // get all the tuples
	    while (rsTuples.next()) {
		ArrayList<String> values = new ArrayList<String>();
		for (int i = 1; i <= nColumn; i++) {
		    values.add(rsTuples.getString(i));
		}
		resultTuples.add(new Tuple(resultRelationName, schema, values));
	    }
	}

	// // debug
	// for (int i = 0; i < resultTuples.size(); i++) {
	// System.out.print("debug:\tresult tuple:\t");
	// for (int j = 0; j < resultTuples.get(i).getValues().size(); j++) {
	// System.out.print(resultTuples.get(i).getValues().get(j) + ",\t\t");
	// }
	// System.out.println();
	// }
	// for (int i = 0; i < resultRelations.size(); i++) {
	// System.out.print("debug:\tresult schema:\t");
	// for (int j = 0; j < resultRelations.get(i).getSchema().size(); j++) {
	// System.out.print(resultRelations.get(i).getSchema().get(j).getRelName()
	// + "." + resultRelations.get(i).getSchema().get(j).getAttName() +
	// ",\t\t");
	// }
	// System.out.println();
	// }

	// clean the old CONN table, and create a new one
	try {
	    stmt.executeUpdate("DROP TABLE IF EXISTS "
		    + Parameters.CONN_REL_NAME);
	    stmt.executeUpdate("CREATE TABLE " + Parameters.CONN_REL_NAME
		    + " (" + Parameters.CONN_TUPLE1_ID_ATT + " int(11), "
		    + Parameters.CONN_TUPLE2_ID_ATT + " int(11))");
	    stmt.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}

	computeWhyConnected(dbConn, resultTuples);
    }

    private static void computeWhyConnected(Connection dbConn,
	    List<Tuple> tuples) throws SQLException {

	minimizeWitnessSet(dbConn); // minimize the witness set.

	for (int i = 0; i < tuples.size(); i++) {
	    for (int j = i + 1; j < tuples.size(); j++) {
		computeTwoTupleWhyConnected(tuples.get(i), tuples.get(j),
			Parameters.CONN_REL_NAME, dbConn);
	    }
	}
    }

    private static void computeTwoTupleWhyConnected(Tuple tuple1, Tuple tuple2,
	    String tableName, Connection dbConn) throws SQLException {

	// condition 1: t.X=t'.X for each common attribute X
	if (isCommonAttributeEqual(tuple1, tuple2, dbConn)) {
	    /*
	     * condition 2: exists w in why(t), exists w' in why(t'), such that
	     * w \cup w' is not null
	     */
	    hasSameSource(tuple1, tuple2, tableName, dbConn);
	}
    }

    private static boolean hasSameSource(Tuple tuple1, Tuple tuple2,
	    String tableName, Connection dbConn) {
	int indexOfRowId1 = -1;
	int indexOfRowId2 = -1;
	String rowId1 = null;
	String rowId2 = null;

	ArrayList<String> schema1 = tuple1.getSchema();
	ArrayList<String> schema2 = tuple2.getSchema();

	// get the index of __ROWID of tuple1. we will use this index to
	for (int i = 0; i < schema1.size(); i++) {
	    if (schema1.get(i).equals(Parameters.ROWID_ATT_NAME)) {
		indexOfRowId1 = i;
		break;
	    }
	}
	// get the __ROWID of tuple2
	for (int i = 0; i < schema2.size(); i++) {
	    if (schema2.get(i).equals(Parameters.ROWID_ATT_NAME)) {
		indexOfRowId2 = i;
		break;
	    }
	}

	/*
	 * check whether there is __ROWID in each relation. Please note the
	 * relation is the query result table.
	 */
	if (indexOfRowId1 == -1) {
	    throw new RuntimeException("No " + Parameters.ROWID_ATT_NAME
		    + " in Relation 1.");
	}
	if (indexOfRowId2 == -1) {
	    throw new RuntimeException("No " + Parameters.ROWID_ATT_NAME
		    + " in Relation 2.");
	}

	/*
	 * evaluate the tuple1 and tuple2 with their __ROWID
	 */
	rowId1 = tuple1.getValues().get(indexOfRowId1);
	rowId2 = tuple2.getValues().get(indexOfRowId2);

	/*
	 * get the witness sets for these two tuples
	 */
	HashMap<String, HashSet<String>> witnessSetOfTuple1 = getWitnessSet(
		rowId1, dbConn);
	HashMap<String, HashSet<String>> witnessSetOfTuple2 = getWitnessSet(
		rowId2, dbConn);

	// Set<String> derivs1 = witnessSetOfTuple1.keySet();
	// Set<String> derivs2 = witnessSetOfTuple2.keySet();
	// for (String driv : derivs1) {
	// System.out.println("debug:\t1_deriv=" + driv + ", src=" +
	// witnessSetOfTuple1.get(driv));
	// }
	// for (String driv : derivs2) {
	// System.out.println("debug:\t2_deriv=" + driv + ", src=" +
	// witnessSetOfTuple2.get(driv));
	// }

	/*
	 * check if they have same sets
	 */
	for (String derivation1 : witnessSetOfTuple1.keySet()) {
	    for (String derivation2 : witnessSetOfTuple2.keySet()) {
		// System.out.println("debug:\tderivation1_src=" +
		// witnessSetOfTuple1.get(derivation1));
		// System.out.println("debug:\tderivation2_src=" +
		// witnessSetOfTuple2.get(derivation2));

		for (String src1 : witnessSetOfTuple1.get(derivation1)) {
		    for (String src2 : witnessSetOfTuple2.get(derivation2)) {
			if (src1.equals(src2)) {
			    // stdout info
			    System.out.println();
			    System.out.println("Connected: " + rowId1
				    + " <--> " + rowId2);
			    System.out.println(rowId1 + " src:"
				    + witnessSetOfTuple1.get(derivation1));
			    System.out.println(rowId2 + " src:"
				    + witnessSetOfTuple2.get(derivation2));
			    System.out.println();

			    // put them into the table
			    try {
				Statement stmt = dbConn.createStatement();
				stmt.executeUpdate("INSERT INTO " + tableName
					+ " VALUES ('" + rowId1 + "', '"
					+ rowId2 + "')");
				stmt.close();
			    } catch (SQLException e) {
				System.err.println("SQL Exception.");
				e.printStackTrace();
			    }
			    return true;
			}
		    }
		}
	    }
	}
	witnessSetOfTuple1 = null;
	witnessSetOfTuple2 = null;
	// System.out.println("Violate condition 2: No minial witness set in common.");
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
		witnessSet.put(DERIV, tmpWitnessSet);
	    }

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

    private static boolean isCommonAttributeEqual(Tuple tuple1, Tuple tuple2,
	    Connection dbConn) throws SQLException {

	List<String> schema1 = tuple1.getSchema();
	List<String> schema2 = tuple2.getSchema();
	List<String> values1 = tuple1.getValues();
	List<String> values2 = tuple2.getValues();

	// compare each common attribute, if they are not equal, return false
	boolean hasCommonAttributes = false;
	for (int i = 0; i < schema1.size(); i++) {
	    for (int j = 0; j < schema2.size(); j++) {
		if (!schema1.get(i).equals(Parameters.ROWID_ATT_NAME)
			&& !schema2.get(j).equals(Parameters.ROWID_ATT_NAME)) {

		    String originalAtt1 = ColumnInfo
			    .getOriginalTableColumnName(tuple1.getSource(),
				    schema1.get(i), dbConn);
		    String originalAtt2 = ColumnInfo
			    .getOriginalTableColumnName(tuple2.getSource(),
				    schema2.get(j), dbConn);
		    String originalColumn1 = ColumnInfo.getOriginalColumnName(
			    tuple1.getSource(), schema1.get(i), dbConn);
		    if (originalAtt1.equals(originalAtt2)
			    && !originalColumn1
				    .equals(Parameters.ROWID_ATT_NAME)) {
			hasCommonAttributes = true;
			if (!values1.get(i).equals(values2.get(j))) {
			    // System.out.println("Violate condition 1: Different "
			    // + "values in common column " + schema1.get(i));
			    return false;
			}
		    }
		}
	    }
	}

	if (!hasCommonAttributes) {
	    return false;
	} else {
	    return true;
	}
    }
}
