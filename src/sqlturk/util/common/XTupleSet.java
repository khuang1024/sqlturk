package sqlturk.util.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;

public class XTupleSet {
    
    private ArrayList<XTuple> tuples;
    
    public XTupleSet() {
	this.tuples = new ArrayList<XTuple>();
    }
    
    public XTupleSet(XTuple tuple) {
	this.tuples = new ArrayList<XTuple>();
	this.add(tuple);
    }
    
    public XTupleSet(ArrayList<XTuple> tuples) {
	this.tuples = new ArrayList<XTuple>();
	this.addAll(tuples);
    }
    
    public boolean contains(XTuple tuple) {
	for (XTuple t : this.tuples) {
	    if (t.equals(tuple)) {
		return true;
	    }
	}
	return false;
    }
    
    public void add(XTuple t) {
	this.tuples.add(t);
    }
    
    public void addAll(ArrayList<XTuple> tuples) {
	for (XTuple t : tuples) {
	    this.add(t);
	}
    }
    
    public void addAll(XTupleSet ts) {
	for (XTuple t : ts.tuples) {
	    this.add(t);
	}
    }
    
    public ArrayList<XTuple> getTuples() {
	return this.tuples;
    }
    
    /**
     * Merge the tuple set to a single tuple based on the given schema
     * (and relation name).
     * @param rel
     * @param schema
     * @param dbConn
     * @return
     * @throws SQLException
     */
    public XTuple toTuple(String rel, ArrayList<String> schema) {
	ArrayList<String> values = new ArrayList<String>();
	for (int i = 0; i < schema.size(); i++) {
	    String currCol = schema.get(i);
	    boolean found = false;
	    for (XTuple t : this.tuples) {
		if (t.getSchema().contains(currCol)) {
		    int index = t.getSchema().indexOf(currCol);
		    values.add(t.getValues().get(index));
		    found = true;
		    break;
		}
	    }
	    if (!found) {
		values.add("NULL");
	    }
	}
	
	return new XTuple(rel, schema, values);
    }
    
    public boolean hasTupleFrom(XTable targetTable) {
	for (int i = 0; i < targetTable.size(); i++) {
	    if (this.contains(targetTable.getTuple(i))) {
		return true;
	    }
	}
	return false;
    }
    
    public boolean subsetOf(XTupleSet tupleSet) {
	for (XTuple t : tupleSet.tuples) {
	    if (!this.contains(t)) {
		return false;
	    }
	}
	return true;
    }
    
    public String getValue(String column) {
	for (XTuple t : this.tuples) {
	    if (t.getSchema().contains(column)) {
		return t.getValues().get(t.getSchema().indexOf(column));
	    }
	}
	return null;
    }
    
    public boolean jcc(XTupleSet ts) {
	
	boolean hasCommonAtt = false;
	
	for (XTuple thisTuple : this.tuples) {
	    ArrayList<String> thisSchema = thisTuple.getSchema();
	    ArrayList<String> thisValues = thisTuple.getValues();
	    for (XTuple thatTuple : ts.tuples) {
		ArrayList<String> thatSchema = thatTuple.getSchema();
		ArrayList<String> thatValues = thatTuple.getValues();
		
		for (String column : thisSchema) {
		    if (thatSchema.contains(column)) {
			hasCommonAtt = true;
			String thisValue = thisValues.get(thisSchema.indexOf(column));
			String thatValue = thatValues.get(thatSchema.indexOf(column));
			
			if (thisValue != null) {
			    if (!thisValue.equals(thatValue)) {
				return false;
			    }
			} else {
			    if (thatValue != null) {
				return false;
			    }
			}
		    }
		}
	    }
	}
	
	if (hasCommonAtt) {
	    return true;
	} else {
	    return false;
	}
    }
    
    public boolean jcc(XTuple tuple) {
	
	boolean hasCommonAtt = false;
	
	// check whether common attributes are equal for each tuple
	
	// check each tuple in this tuple set to see whether they have
	// common attributes with the given tuple and the values are equal
	for (XTuple thisTuple : this.tuples) {
	    ArrayList<String> thisSchema = thisTuple.getSchema(); // schema of the current tuple
	    for (int i = 0; i < thisSchema.size(); i++) {
		String thisColumn = thisSchema.get(i);
		if (!thisColumn.equals(Parameters.ROWID_ATT_NAME)) {
		    if (tuple.getSchema().contains(thisColumn)) { // if they have common attribute
			hasCommonAtt = true;
			int index = tuple.getSchema().indexOf(thisColumn);
			String thatValue = tuple.getValues().get(index);
			
			// note the value may be NULL
			if (thatValue != null) {
			    if (!thatValue.equals(thisTuple.getValues().get(i))) {
				return false;
			    }
			} else {
			    if (thisTuple.getValues().get(i) != null) {
				return false;
			    }
			}
		    }
		}
	    }
	}
	if (hasCommonAtt) {
	    return true;
	} else {
	    return false;
	}
    }
    
    public boolean connects(XTupleSet ts, Connection dbConn) throws SQLException {
	
	if (!this.jcc(ts)) {
	    return false;
	} else {
	    // there is a tuple in this tuple set connecting to a tuple
	    // from the given tuple set
	    for (XTuple thisTuple : this.tuples) {
		for (XTuple thatTuple : ts.tuples) {
		    if (XTupleSet.connects(thisTuple, thatTuple, dbConn)) {
			return true;
		    }
		}
	    }
	    return false;
	}
    }
    
    public boolean connects(XTuple thatTuple, Connection dbConn) throws SQLException {
	
	if (!this.jcc(thatTuple)) {
	    return false;
	}
	
	// check whether there is a tuple from this tuple set is connected
	// to the given target thatTuple
	for (XTuple thisTuple : this.tuples) {
	    if (XTupleSet.connects(thisTuple, thatTuple, dbConn)) {
		 return true;
	    }
	}
	return false;
    }
    
    public static boolean connects(XTuple tuple1, XTuple tuple2, Connection dbConn) throws SQLException {
	String rowId1 = tuple1.getValues().get(tuple1.getSchema().indexOf(Parameters.ROWID_ATT_NAME));
	String rowId2 = tuple2.getValues().get(tuple2.getSchema().indexOf(Parameters.ROWID_ATT_NAME));
	return connects(rowId1, rowId2, dbConn);
    }
    
    public static boolean connects(String rowId1, String rowId2, Connection dbConn) throws SQLException {
	Statement stmt = dbConn.createStatement();
	String query = "SELECT *" +
			" FROM " + Parameters.CONN_REL_NAME +
			" WHERE " + Parameters.CONN_TUPLE1_ID_ATT + "=" + rowId1 + 
			" AND " + Parameters.CONN_TUPLE2_ID_ATT + "=" + rowId2;
	ResultSet rs = stmt.executeQuery(query);
	if (rs.next()) {
	    rs.close();
	    stmt.close();
	    return true;
	}
	
	// flip the two columns
	query = "SELECT *" +
		" FROM " + Parameters.CONN_REL_NAME +
		" WHERE " + Parameters.CONN_TUPLE1_ID_ATT + "=" + rowId2 + 
		" AND " + Parameters.CONN_TUPLE2_ID_ATT + "=" + rowId1;
	rs = stmt.executeQuery(query);
	if (rs.next()) {
	    rs.close();
	    stmt.close();
	    return true;
	}
	
	rs.close();
	stmt.close();
	return false;
    }

    /**
     * @param args
     * @throws SQLException 
     * @throws ClassNotFoundException 
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     */
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
	
	System.out.println(XTupleSet.connects("2000000077", "2000000113", dbConn));
    }

}
