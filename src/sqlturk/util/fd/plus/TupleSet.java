package sqlturk.util.fd.plus;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;

import sqlturk.configuration.Parameters;

class TupleSet {
    private HashSet<Tuple> tupleSet;

    TupleSet() {
	tupleSet = new HashSet<Tuple>();
    }

    TupleSet(Tuple t) {
	tupleSet = new HashSet<Tuple>();
	// System.out.println("debug:\txxx-" + t.gettValuesString());
	tupleSet.add(t);
    }

    TupleSet copy() {
	TupleSet ts = new TupleSet();
	for (Tuple tup : this.tupleSet) {
	    ts.addTuple(tup);
	}
	return ts;
    }

    void addTuple(Tuple t) {
	tupleSet.add(t);
    }

    void setNull() {
	tupleSet.clear();
    }

    int size() {
	return tupleSet.size();
    }

    HashSet<Tuple> getTuples() {
	return tupleSet;
    }

    boolean hasTuple(Tuple tuple) {
	for (Tuple t : tupleSet) {
	    if (Tuple.isSameValue(t, tuple)) {
		return true;
	    }
	}
	return false;
    }

    boolean hasTupleFromRelation(String relationName) {
	for (Tuple tuple : this.tupleSet) {
	    if (tuple.getSource().equals(relationName)) {
		return true;
	    }
	}
	return false;
    }

    boolean hasTupleFromRelation(Relation relation) {
	for (Tuple tuple : this.tupleSet) {
	    if (tuple.getSource().equals(relation.getRelationName())) {
		return true;
	    }
	}
	return false;
    }

    boolean isSubsetOf(TupleSet superSet) {
	for (Tuple tuple : this.tupleSet) {
	    if (!superSet.hasTuple(tuple)) {
		return false;
	    }
	}
	return true;
    }

    boolean isConnectedWith(TupleSet targetTupleSet, String rowId, String CONN,
	    Connection dbConn) throws SQLException {
	for (Tuple tup : targetTupleSet.getTuples()) {
	    if (!this.isConnectedWith(tup, rowId, CONN, dbConn)) {
		return false;
	    }
	}
	return true;
    }

    /*
     * for every tuple in this tuple set, should be connected with the tuple
     */
    boolean isConnectedWith(Tuple targetTuple, String rowId, String CONN_TABLE,
	    Connection dbConn) throws SQLException {

	// Note: TupleSet tupleSet = inComplete.get(0); So, no need to consider
	// the scenario that this.tupleSet is null.

	// store all the pairs of the CONN relation in firstColumn and
	// secondColumn
	Statement stmt = dbConn.createStatement();
	ResultSet rs = stmt.executeQuery("SELECT * FROM " + CONN_TABLE);
	ArrayList<String> firstColumn = new ArrayList<String>();
	ArrayList<String> secondColumn = new ArrayList<String>();
	while (rs.next()) {
	    firstColumn.add(rs.getString(Parameters.CONN_TUPLE1_ID_ATT));
	    secondColumn.add(rs.getString(Parameters.CONN_TUPLE2_ID_ATT));
	}

	// extract all the __ROWID of tuples of this TupleSet
	HashSet<String> ROWIDs = new HashSet<String>();
	for (Tuple tup : this.tupleSet) {
	    ROWIDs.add(tup.getValueAt(rowId, dbConn));
	}
//	ArrayList<String> ROWIDs = new ArrayList<String>();
//	for (Tuple tup : this.tupleSet) {
//	    String value = tup.getValueAt(rowId, dbConn);
//	    if (!ROWIDs.contains(value)) {
//		ROWIDs.add(value);
//	    }
//	    value = null;
//	}

	/*
	 * self-validation: if the tuple is not in the CONN table, it means it
	 * is not why-connected with any other tuples. Therefore, this tuple
	 * should be placed in a single row. Here, I return false, to prevent it
	 * from calculating if it can be grouped with other tuples, since it is
	 * unnecessary.
	 */
	for (String ROWID : ROWIDs) {
	    if ((!firstColumn.contains(ROWID))
		    && (!secondColumn.contains(ROWID))) {
		// // debug
		// System.out.println("debug:\tROWID="+ROWID);
		// for (String str : firstColumn) {
		// System.out.println("debug:\tfirst_column=" + str);
		// }
		// for (String str : secondColumn) {
		// System.out.println("debug:\tsecond_column=" + str);
		// }

		return false;
		// throw new
		// RuntimeException("Why-connected: self-validation failed.");
	    }
	}

	// find out all the ROWIDs which are paired with this tuple and store
	// them in targetTuplePair
	String targetTupleId = targetTuple.getValueAt(rowId, dbConn);
	HashSet<String> targetTuplePair = new HashSet<String>();
//	ArrayList<String> targetTuplePair = new ArrayList<String>();
	for (int i = 0; i < firstColumn.size(); i++) {
	    if (firstColumn.get(i).equals(targetTupleId)) {
		targetTuplePair.add(secondColumn.get(i));
	    }
	}
	for (int i = 0; i < secondColumn.size(); i++) {
	    if (secondColumn.get(i).equals(targetTupleId)) {
		targetTuplePair.add(firstColumn.get(i));
	    }
	}

	// // debug
	// for (String t : targetTuplePair) {
	// System.out.println("debug:\tThe current tuple " + targetTupleId +
	// " is why-connected to: "+t);
	// }

	// check if there is any tuple which is not why-connected with the
	// target tuple
	for (String ROWID : ROWIDs) {
	    if (!targetTuplePair.contains(ROWID)) {
		return false;
	    }
	}

	rs.close();
	stmt.close();
	return true;
    }

    boolean isSame(TupleSet ts) {
	for (Tuple tup : ts.getTuples()) {
	    if (!this.hasTuple(tup)) {
		return false;
	    }
	}
	for (Tuple tup : tupleSet) {
	    if (!ts.hasTuple(tup)) {
		return false;
	    }
	}
	return true;
    }

    void unionWith(TupleSet ts) {
	for (Tuple tuple : ts.getTuples()) {
	    if (!hasTuple(tuple)) {
		tupleSet.add(tuple);
	    }
	}
    }

    String getPrintInfo() {
	String info = "";
	for (Tuple tuple : tupleSet) {
	    info += "(" + tuple.gettValuesString() + "), ";
	}
	return info;
    }

    void print() {
	int i = 0;
	for (Tuple tuple : tupleSet) {
	    System.out.println("Tuple " + (i++) + ": "
		    + tuple.gettValuesString());
	}
    }
}
