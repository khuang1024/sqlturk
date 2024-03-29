package sqlturk.util.fd.normal;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;
import sqlturk.experiment.provenance.SQLTurk;
import sqlturk.util.equi.Equivalence;
import sqlturk.util.map.ColumnInfo;
import sqlturk.util.query.QueryExecutor;

class IncrementalFD {

    private static int tmpId = 0;

    private IncrementalFD() {
	throw new AssertionError();
    }
    
    static void createFDRelationNormally(ArrayList<Relation> allResultRelations,
	    Connection dbConn) throws SQLException {
	createFDRelation(allResultRelations, false, dbConn);
    }
    
    static void createFDRelationOptimally(ArrayList<Relation> allResultRelations,
	    String datasetName, int queryIndex, int topN, int[] candidates,
	    Connection dbConn
	    ) throws SQLException {
	// optimization: if we have previous top(k-1) FD table, we can use it
	// actually, FD can be created recursively. (top(k) = top(k-1)FD(the additional query))
	String previousFDName = SQLTurk.getFDName(datasetName, queryIndex, topN-1, candidates);
	if (isExist(previousFDName, dbConn)) {
	    // if the previous FD exists in the database
	    
	    System.out.println("debug:\tprevious FD exists! Use it for optimzied computation.");
	    
//	    if (!QueryExecutor.getCurrentLastQueryResultTableName(dbConn).equals(getAdditionalResultTableName(dbConn))) {
//		System.out.println(QueryExecutor.getCurrentLastQueryResultTableName(dbConn));
//		System.out.println(getAdditionalResultTableName(dbConn));
//		throw new RuntimeException("Confusing additional resulting table.");
//	    }
	    
	    String additionalResultRelationName = getAdditionalResultTableName(dbConn);
	    System.out.println("=====================================================================");
	    System.out.println("debug:\tThe additional resulting table is: " + additionalResultRelationName);
	    System.out.println("=====================================================================");
	    Relation previousFDRelation = new Relation(previousFDName, FD.getTuples(previousFDName, dbConn));
	    Relation additionalResultRelation = new Relation(additionalResultRelationName, FD.getTuples(additionalResultRelationName, dbConn));
	    ArrayList<Relation> relationsToCompute = new ArrayList<Relation>();
	    relationsToCompute.add(additionalResultRelation); // the order is important! the first one must be additionalResultRelation
	    relationsToCompute.add(previousFDRelation);
	    createFDRelation(relationsToCompute, true, dbConn);
	} else {
	    createFDRelation(allResultRelations, false, dbConn);
	}
    }

    private static void createFDRelation(ArrayList<Relation> allResultRelations, boolean hasFD,
	    Connection dbConn) throws SQLException {
	Statement stmt = dbConn.createStatement();
	
	// first, get common schema, using the same function, but neet to wrap it
	ArrayList<String> combinedSchema = null;
	if (hasFD) {
	    
	    // if we have FD, maybe this new query is equivalent to a previous one
	    // if so, we don't need to compute it, so we reduce our computation more
	    ArrayList<String> needComputeResultTableNames = Equivalence.getInequivalentResultTables(dbConn);
	    Relation additionalResultRelation = allResultRelations.get(0);
	    if (!needComputeResultTableNames.contains(additionalResultRelation.getRelationName())) {
		// if the new table is not a representative, then we don't need to compute.
		
		System.out.println("Congrats! " + additionalResultRelation.getRelationName() + " is equal to at least one previous query. We don't need any computation.");
//		System.out.println("Create table " + )
		
		// create the FD table directly
		stmt.executeUpdate("DROP TABLE IF EXISTS " + Parameters.FD_REL_NAME);
		String query = "CREATE TABLE " + Parameters.FD_REL_NAME + " AS SELECT * FROM " + allResultRelations.get(1).getRelationName();

		// debug
		 System.out.println("debug:\tIncrementalFD: "+query);

		// create the table
		stmt.executeUpdate(query);
		stmt.close();
		return;
	    } else {
		ArrayList<Relation> wrapper = new ArrayList<Relation>();
		wrapper.add(allResultRelations.get(0));
		ArrayList<String> additionalResultSchema = getCombinedSchema(wrapper, dbConn); // commbinedSchema is the rewritten schema of additionalResultRelation
		ArrayList<String> previousFDSchema = allResultRelations.get(1).getTuples().get(0).getSchema(); // FD' schema has already been rewritten, so don't need to rewrite
		combinedSchema = new ArrayList<String>();
		// get their combined schema
		for (String col : previousFDSchema) {
		    if (combinedSchema.contains(col)) {
			throw new RuntimeException("Duplicate column names in previous FD.");
		    } else {
			combinedSchema.add(col);
		    }
		}
		for (String col : additionalResultSchema) {
		    if (!combinedSchema.contains(col)) {
			combinedSchema.add(col);
		    }
		}
	    }
	} else {
	    combinedSchema = getCombinedSchema(allResultRelations, dbConn);
	}
	

	
	// get each FDi
	ArrayList<String> unionTableAtoms = new ArrayList<String>();
	
	if (hasFD) {
	    for (int i = 0; i < allResultRelations.size(); i++) {
		unionTableAtoms.add("SELECT * FROM " + createSubFDRelationFor(allResultRelations.get(i), allResultRelations, combinedSchema, dbConn));
	    } 
	} else {
	    // filter
	    ArrayList<Relation> needComputeResultTables = new ArrayList<Relation>();
	    ArrayList<String> needComputeResultTableNames = Equivalence.getInequivalentResultTables(dbConn);
	    for (int i = 0; i < allResultRelations.size(); i++) {
		if (needComputeResultTableNames.contains(allResultRelations.get(i).getRelationName())) {
		    System.out.println(allResultRelations.get(i).getRelationName() + " will be used for computation.");
		    needComputeResultTables.add(allResultRelations.get(i));
		} else {
		    System.out.println("debug:\t" + allResultRelations.get(i).getRelationName() + " has an equivalent table. Skip its FD_i.");
		}
	    }
	    // compute
	    for (int i = 0; i < needComputeResultTables.size(); i++) {
		unionTableAtoms.add("SELECT * FROM " + createSubFDRelationFor(needComputeResultTables.get(i), needComputeResultTables, combinedSchema, dbConn));
	    }
	}
	
	//
	stmt.executeUpdate("DROP TABLE IF EXISTS " + Parameters.FD_REL_NAME);
	String query = "CREATE TABLE " + Parameters.FD_REL_NAME + " AS ";
	String unionTableClause = "";
	for (int i = 0; i < unionTableAtoms.size(); i++) {
	    if (i < unionTableAtoms.size() - 1) {
		unionTableClause += unionTableAtoms.get(i) + " UNION ";
	    } else {
		unionTableClause += unionTableAtoms.get(i);
	    }
	}
	query += unionTableClause;

	// debug
	 System.out.println("debug:\tIncrementalFD: "+query);

	// create the table
	stmt.executeUpdate(query);
	stmt.close();
	unionTableAtoms = null;
	combinedSchema = null;
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
		    String originalAttName = null;
		    if (Tuple.isAlreadyOriginalTableColumnName(schema, dbConn)) {
			originalAttName = schema.get(i);
		    } else {
			originalAttName = ColumnInfo.getOriginalTableColumnName(tup.getSource(), schema.get(i), dbConn);
		    }
		    
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
		
		schema = null;
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
	    
	    tuple = null;
	    
	    System.out.println("debug:\t|-> " + query);
	    stmt.executeUpdate(query);
	}
	
	resultTupleSets = null; // release the reference
	
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
		    // to determine if the two attribute actual from the same
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

			    TupleSet tupleSetPrime = getTupleSetPrime(tupleSet,
				    tup, dbConn);

			    if (tupleSetPrime != null) {

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
			    }
			    tupleSetPrime = null;
			}
		    }
		}
	    }

	    // line 21
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
	    tupleSet = null;
	}
	inComplete = null;
	
	return complete;
    }

    private static TupleSet getTupleSetPrime(TupleSet tupleSet, Tuple tuple,
	    Connection dbConn) throws SQLException {
	TupleSet tupleSetPrime = new TupleSet();

	for (Tuple t1 : tupleSet.getTuples()) {
	    TupleSet temp = new TupleSet();
	    temp.addTuple(tuple);

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
		tupleSetPrime = temp;
	    }
	    temp = null;
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
    
    private static boolean isExist(String tableName, Connection dbConn) throws SQLException {
	Statement stmt = dbConn.createStatement();
	ResultSet rs = stmt.executeQuery("SHOW TABLES");
	while (rs.next()) {
	    if (rs.getString(1).equals(tableName)) {
		if (rs != null) {
		    rs.close();
		}
		if (stmt != null) {
		    stmt.close();
		}
		return true;
	    }
	}
	if (rs != null) {
	    rs.close();
	}
	if (stmt != null) {
	    stmt.close();
	}
	return false;
    }
    
    private static String getAdditionalResultTableName(Connection dbConn) throws SQLException {
	Statement stmt = dbConn.createStatement();
	ResultSet rs = stmt.executeQuery("SHOW TABLES");
	String additionalResultTableName = "";
	while (rs.next()) {
	    if (rs.getString(1).startsWith(Parameters.QUERY_RESULT_PREFIX)
		    && rs.getString(1).endsWith(Parameters.QUERY_RESULT_SUFFIX)) {
		additionalResultTableName = rs.getString(1);
	    }
	}
	if (rs != null) {
	    rs.close();
	}
	if (stmt != null) {
	    stmt.close();
	}
	return additionalResultTableName;
    }
}
