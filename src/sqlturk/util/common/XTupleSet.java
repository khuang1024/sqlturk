package sqlturk.util.common;

import java.sql.SQLException;
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
	
	ArrayList<String> schema1 = XCommon.getCommonCols(this.tuples);
	ArrayList<String> schema2 = XCommon.getCommonCols(ts.tuples);
	boolean hasCommonAtt = false;
	for (int i = 0; i < schema1.size(); i++) {
	    String col1 = schema1.get(i);
	    if (schema2.contains(col1)) {
		hasCommonAtt = true;
		String value1 = this.getValue(col1);
		String value2 = ts.getValue(col1);
		if (!value1.equals(value2)) {
		    return false;
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
	for (XTuple t : this.tuples) {
	    ArrayList<String> schema = t.getSchema(); // schema of the current tuple
	    for (int i = 0; i < schema.size(); i++) {
		String col = schema.get(i);
		if (col.equals(Parameters.ROWID_ATT_NAME)) {
		    continue;
		}
		if (tuple.getSchema().contains(col)) { // if they have common attribute
		    hasCommonAtt = true;
		    int index = tuple.getSchema().indexOf(col);
		    String value = tuple.getValues().get(index);
		    if (!value.equals(t.getValues().get(i))) {
			return false;
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

    /**
     * @param args
     */
    public static void main(String[] args) {
	
    }

}
