package sqlturk.util.fd.normal;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;
import sqlturk.util.map.ColumnInfo;

class IncrementalFD {

    private static int tmpId = 0;

    private IncrementalFD() {
	throw new AssertionError();
    }

    static void createFDRelation(ArrayList<Relation> allResultRelations,
	    Connection dbConn) throws SQLException {
	ArrayList<String> combinedSchema = getCombinedSchema(
		allResultRelations, dbConn);
	Statement stmt = dbConn.createStatement();

	// get each FDi
	String[] unionTableAtoms = new String[allResultRelations.size()];
	for (int i = 0; i < allResultRelations.size(); i++) {
	    unionTableAtoms[i] = "SELECT * FROM "
		    + createSubFDRelationFor(allResultRelations.get(i),
			    allResultRelations, combinedSchema, dbConn) + " ";
	}

	//
	stmt.executeUpdate("DROP TABLE IF EXISTS " + Parameters.FD_REL_NAME);
	String query = "CREATE TABLE " + Parameters.FD_REL_NAME + " AS ";
	String unionTableClause = "";
	for (int i = 0; i < unionTableAtoms.length; i++) {
	    if (i < unionTableAtoms.length - 1) {
		unionTableClause += unionTableAtoms[i] + " UNION ";
	    } else {
		unionTableClause += unionTableAtoms[i];
	    }
	}
	query += unionTableClause;

	// debug
	// System.out.println("debug:\tIncrementalFD: "+query);

	// create the table
	stmt.executeUpdate(query);
	stmt.close();
    }

    private static String createSubFDRelationFor(Relation relation,
	    ArrayList<Relation> allResultRelations,
	    ArrayList<String> combinedSchema, Connection dbConn)
	    throws SQLException {

	Statement stmt = dbConn.createStatement();

	/*
	 * create the temporary table for future union. The TMP table is the FD
	 * of current relation
	 */
	String tempTableName = Parameters.FD_TEMP_TABLE_PREFIX + (tmpId++)
		+ Parameters.FD_TEMP_TABLE_SUFFIX;
	// System.out.println(tempTableName);
	stmt.executeUpdate("DROP TABLE IF EXISTS " + tempTableName);
	String query1 = "CREATE TABLE " + tempTableName + " (";
	for (int i = 0; i < combinedSchema.size(); i++) {
	    if (i < combinedSchema.size() - 1) {
		query1 += combinedSchema.get(i) + " VARCHAR(100), ";
	    } else {
		query1 += combinedSchema.get(i) + " VARCHAR(100))";
	    }
	}
	System.out.println("debug:\t" + query1);
	stmt.executeUpdate(query1);

	ArrayList<TupleSet> resultTupleSets = getFDTulpleSetsFor(relation,
		allResultRelations, dbConn);//

	// debug
	System.out.println("debug:\tThe TupleSet for relation "
		+ relation.getRelationName());
	for (TupleSet r : resultTupleSets) {
	    System.out.println("\t\t\t- " + r.getPrintInfo());
	}

	for (TupleSet ts : resultTupleSets) {
	    System.out.println("debug:\tStart processing tupleset : "
		    + ts.getPrintInfo());

	    String[] tuple = new String[combinedSchema.size()];

	    // here we should merge the compatible (JCC) result
	    for (Tuple tup : ts.getTuples()) {

		ArrayList<String> schema = tup.getSchema();

		for (int i = 0; i < schema.size(); i++) {// i : inner index
		    String originalAttName = ColumnInfo
			    .getOriginalTableColumnName(tup.getSource(),
				    schema.get(i), dbConn);
		    // j: index for combinedSchema
		    for (int j = 0; j < combinedSchema.size(); j++) {
			if (originalAttName != null) {
			    if (originalAttName.equals(combinedSchema.get(j))) {
				// theoretically, there MUST be a match!
				tuple[j] = tup.getValueAt(i);
			    }
			}
		    }
		}
	    }
	    System.out.println("debug:\tFinish processing tupleset : "
		    + ts.getPrintInfo());
	    String query = "INSERT INTO " + tempTableName + " VALUES (";
	    for (int i = 0; i < tuple.length; i++) {
		if (i < tuple.length - 1) {
		    query += "'" + tuple[i] + "', ";
		} else {
		    query += "'" + tuple[i] + "')";
		}
	    }
	    System.out.println("debug:\t|-> " + query);
	    stmt.executeUpdate(query);
	}
	System.out.println("debug:\t------------------------------------\n\n");
	stmt.close();
	return tempTableName;
    }

    /**
     * Notice: the returned combinedSchema is in format: R1_R1C1, which indicate
     * the source relation and original column name.
     * 
     * @param allResultRelations
     * @return
     * @throws SQLException
     */
    private static ArrayList<String> getCombinedSchema(
	    ArrayList<Relation> allResultRelations, Connection dbConn)
	    throws SQLException {
	ArrayList<String> combinedSchema = new ArrayList<String>();
	for (Relation relation : allResultRelations) {
	    if (relation.getTuples().size() > 0) {
		for (String attribute : relation.getTuples().get(0).getSchema()) {
		    String original = ColumnInfo.getOriginalTableColumnName(
			    relation.getRelationName(), attribute, dbConn);
		    String originalAtt = ColumnInfo.getOriginalColumnName(
			    relation.getRelationName(), attribute, dbConn);

		    // here, should use relation_name and present_attribute_name
		    // to
		    // determine if the two attribute actual from the same
		    // column
		    if ((!combinedSchema.contains(original)
			    && (original != null) && !originalAtt
				.equalsIgnoreCase(Parameters.ROWID_ATT_NAME))) {
			combinedSchema.add(original);
		    }
		}
	    }
	}

	// debug
	System.out.print("debug:\tFD_Combined schema is: ");
	for (String att : combinedSchema) {
	    System.out.print(att + ", ");
	}
	System.out.println();

	return combinedSchema;
    }

    /**
     * 
     * @param relation
     *            MUST have tuples
     * @param allResultRelations
     * @param dbConn
     * @param type
     * @throws SQLException
     */
    private static ArrayList<TupleSet> getFDTulpleSetsFor(Relation relation,
	    ArrayList<Relation> allResultRelations, Connection dbConn)
	    throws SQLException {
	ArrayList<TupleSet> complete = new ArrayList<TupleSet>();
	ArrayList<TupleSet> inComplete = new ArrayList<TupleSet>();

	for (Tuple tuple : relation.getTuples()) {
	    inComplete.add(new TupleSet(tuple));
	}

	// line 5
	while (!inComplete.isEmpty()) {

	    // line 6
	    TupleSet tupleSet = inComplete.get(0);
	    inComplete.remove(0);

	    // //debug
	    // System.out.println("XXXX");
	    // tupleSet.print();
	    // System.out.println("==inComplete");
	    // for (TupleSet ts : inComplete) {
	    // ts.print();
	    // }
	    // System.out.println("==");
	    // System.out.println("XXXX");

	    // get the maximal JCC set of for the target tupleSet: line 7
	    for (Relation rel : allResultRelations) {

		// exclude the target relation itself
		if (!rel.getRelationName().equals(relation.getRelationName())) {
		    for (Tuple tup : rel.getTuples()) {

			// exclude the tuples already in tupleSet
			if (!tupleSet.hasTuple(tup)) {
			    // when after adding tup to tupleSet, tupeSet is
			    // still JCC, then, add it
			    if (tupleSet.isJCCWith(tup, dbConn)) {
				// change to FD+
				System.out.println("debug:\t"
					+ "add new tuple ["
					+ tup.gettValuesString()
					+ "] to tupleset ["
					+ tupleSet.getPrintInfo() + "]");
				tupleSet.addTuple(tup);
			    }
			}
		    }
		}
	    }

	    // deal with the rest tuples: line 9
	    for (Relation rel : allResultRelations) {

		// exclude the target relation itself
		if (!rel.getRelationName().equals(relation.getRelationName())) {
		    for (Tuple tup : rel.getTuples()) {
			// exclude the tuples already in tupleSet
			if (!tupleSet.hasTuple(tup)) {

			    // //debug
			    // System.out.println("tupleSet has tup = " +
			    // (tupleSet.hasTuple(tup)));
			    // tup.printValues();
			    // tupleSet.print();
			    // System.out.println("-------------------------");

			    TupleSet tupleSetPrime = getTupleSetPrime(tupleSet,
				    tup, dbConn);

			    if (tupleSetPrime != null) {

				// // debug
				// System.out.println("T-prime (start) = ");
				// tupleSetPrime.print();

				if (tupleSetPrime
					.hasTupleFromRelation(relation)) {
				    boolean inserted = false;// line 13
				    if (existS(complete, tupleSetPrime)) {
					inserted = true;
				    }

				    // line 16
				    for (TupleSet ts : inComplete) {
					if (tupleSetPrime.isJCCWith(ts, dbConn)) {
					    // change to FD+
					    // line 18
					    ts.unionWith(tupleSetPrime);
					    inserted = true;
					}
				    }

				    // line 20
				    if (!inserted) {
					boolean add = true;
					for (TupleSet ts : inComplete) {
					    if (ts.isSame(tupleSetPrime)) {
						add = false;
						break;
					    }
					}
					if (add) {
					    inComplete.add(tupleSetPrime);
					}
				    }
				}

				// debug
				// System.out.println("T-prime (end) = ");
				// tupleSetPrime.print();
				// System.out.println("==inComplete");
				// for (TupleSet ts : inComplete) {
				// ts.print();
				// }
				// System.out.println("==complete");
				// for (TupleSet ts : complete) {
				// ts.print();
				// }
			    }
			}
		    }
		}
	    }

	    // line 21
	    // debug
	    // tupleSet.print();

	    boolean add = true;
	    for (TupleSet ts : complete) {
		if (ts.isSame(tupleSet)) {
		    add = false;
		    break;
		}
	    }
	    if (add) {
		complete.add(tupleSet);
	    }

	    // //debug
	    // System.out.println("-------------------");
	    // System.out.println("After an iteration:");
	    // System.out.println("=inComplete:");
	    // for (TupleSet ts : inComplete) {
	    // ts.print();
	    // }
	    // System.out.println("=complete");
	    // for (TupleSet ts : complete) {
	    // ts.print();
	    // }
	    // System.out.println("-------------------");

	    // debug
	    // System.out.println("After a iteration:");
	    // System.out.println("==inComplete");
	    // for (TupleSet ts : inComplete) {
	    // ts.print();
	    // }
	    // System.out.println("==complete");
	    // for (TupleSet ts : complete) {
	    // ts.print();
	    // }
	    // System.out.println("===================");

	    // System.out.println()
	}

	// // debug
	// System.out.println("The final FD for " + relation.getRelationName() +
	// " is:\n");
	// int i = 0;
	// for (TupleSet ts : complete) {
	// System.out.println("TupleSet " + (i++) + ": ");
	// for (Tuple t : ts.getTuples()) {
	// System.out.println(t.getSource() + " : " + t.gettValuesString());
	// }
	// System.out.println();
	// }
	// System.out.println("==================");

	return complete;
    }

    private static TupleSet getTupleSetPrime(TupleSet tupleSet, Tuple tuple,
	    Connection dbConn) throws SQLException {
	TupleSet tupleSetPrime = new TupleSet();

	for (Tuple t1 : tupleSet.getTuples()) {
	    TupleSet temp = new TupleSet();
	    temp.addTuple(tuple);

	    // debug
	    // t1.printValues();
	    // temp.print();
	    // System.out.println();

	    if (temp.isJCCWith(t1, dbConn)) { // it they are not JCC, skip
		temp.addTuple(t1);
		for (Tuple t2 : tupleSet.getTuples()) {
		    if (!Tuple.isSame(t1, t2)) {
			if (temp.isJCCWith(t2, dbConn)) {
			    // change to FD+
			    temp.addTuple(t2);
			}
		    }
		}
	    }
	    if (temp.size() > tupleSetPrime.size()) {
		tupleSetPrime = temp.copy();
	    }
	}

	/*
	 * if there is only one element in tupleSetPrime (the element itself),
	 * there are no other elements is JCC with this element. So, return
	 * null.
	 */
	if (tupleSetPrime.size() == 1) {
	    return null;
	}
	return tupleSetPrime;
    }

    private static boolean existS(ArrayList<TupleSet> complete,
	    TupleSet tupleSetPrime) {
	for (TupleSet ts : complete) {
	    if (tupleSetPrime.isSubsetOf(ts)) {
		return true;
	    }
	}
	return false;
    }

    /**
     * @param args
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws SQLException
     */
    public static void main(String[] args) throws InstantiationException,
	    IllegalAccessException, ClassNotFoundException, SQLException {
	Class.forName("com.mysql.jdbc.Driver").newInstance();
	Connection dbConn;
	/*
	 * use soe server or test locally
	 */
	if (Parameters.USE_SERVER) {
	    dbConn = DriverManager
		    .getConnection(Parameters.MYSQL_CONNECTION_STRING);
	    System.out.println("Using server.");
	} else {
	    dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL,
		    Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
	    System.out.println("Using local.");
	}

	String source1 = "Climates";
	String source2 = "Accommodations";
	String source3 = "Sites";

	ArrayList<String> schema1 = new ArrayList<String>();
	ArrayList<String> schema2 = new ArrayList<String>();
	ArrayList<String> schema3 = new ArrayList<String>();

	schema1.add("Country");
	schema1.add("Climate");

	schema2.add("Country");
	schema2.add("City");
	schema2.add("Hotel");
	schema2.add("Stars");

	schema3.add("Country");
	schema3.add("City");
	schema3.add("Site");

	ArrayList<String> values11 = new ArrayList<String>();
	ArrayList<String> values12 = new ArrayList<String>();
	ArrayList<String> values13 = new ArrayList<String>();
	values11.add("Canada");
	values11.add("diverse");
	values12.add("UK");
	values12.add("temporate");
	values13.add("Bahamas");
	values13.add("tropical");

	ArrayList<String> values21 = new ArrayList<String>();
	ArrayList<String> values22 = new ArrayList<String>();
	ArrayList<String> values23 = new ArrayList<String>();
	values21.add("Canada");
	values21.add("Toronto");
	values21.add("Plaza");
	values21.add("4");
	values22.add("Canada");
	values22.add("London");
	values22.add("Ramada");
	values22.add("3");
	values23.add("Bahamas");
	values23.add("Nassau");
	values23.add("Hilton");
	values23.add(null);

	ArrayList<String> values31 = new ArrayList<String>();
	ArrayList<String> values32 = new ArrayList<String>();
	ArrayList<String> values33 = new ArrayList<String>();
	ArrayList<String> values34 = new ArrayList<String>();
	values31.add("Canada");
	values31.add("London");
	values31.add("Air Show");
	values32.add("Canada");
	values32.add(null);
	values32.add("Mount Logan");
	values33.add("UK");
	values33.add("London");
	values33.add("Buckingham");
	values34.add("UK");
	values34.add("London");
	;
	values34.add("Hyde Park");

	Tuple t11 = new Tuple(source1, schema1, values11);
	Tuple t12 = new Tuple(source1, schema1, values12);
	Tuple t13 = new Tuple(source1, schema1, values13);

	Tuple t21 = new Tuple(source2, schema2, values21);
	Tuple t22 = new Tuple(source2, schema2, values22);
	Tuple t23 = new Tuple(source2, schema2, values23);

	Tuple t31 = new Tuple(source3, schema3, values31);
	Tuple t32 = new Tuple(source3, schema3, values32);
	Tuple t33 = new Tuple(source3, schema3, values33);
	Tuple t34 = new Tuple(source3, schema3, values34);

	ArrayList<Tuple> tuples1 = new ArrayList<Tuple>();
	ArrayList<Tuple> tuples2 = new ArrayList<Tuple>();
	ArrayList<Tuple> tuples3 = new ArrayList<Tuple>();

	tuples1.add(t11);
	tuples1.add(t12);
	tuples1.add(t13);

	tuples2.add(t21);
	tuples2.add(t22);
	tuples2.add(t23);

	tuples3.add(t31);
	tuples3.add(t32);
	tuples3.add(t33);
	tuples3.add(t34);

	ArrayList<Relation> relations = new ArrayList<Relation>();
	Relation relation1 = new Relation(source1, tuples1);
	Relation relation2 = new Relation(source2, tuples2);
	Relation relation3 = new Relation(source3, tuples3);
	relations.add(relation1);
	relations.add(relation2);
	relations.add(relation3);

	createFDRelation(relations, dbConn);

    }

}
