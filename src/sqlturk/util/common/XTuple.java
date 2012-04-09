package sqlturk.util.common;

import java.util.ArrayList;

public class XTuple {
    
    private String rel;
    private ArrayList<String> schema;
    private ArrayList<String> values;
    
    public XTuple(String rel, ArrayList<String> schema, ArrayList<String> values) {
	this.rel = rel;
	this.schema = schema;
	this.values = values;
    }
    
    public ArrayList<String> getCommonAtts(ArrayList<String> schema2) {
	ArrayList<String> commonAtts = new ArrayList<String>();
	
	ArrayList<String> schema1 = this.schema;
	
	for (String att : schema1) {
	    if (schema2.contains(att)) {
		commonAtts.add(att);
	    }
	}
	
	return commonAtts;
    }
    
    public ArrayList<String> getCommonAtts(XTuple tuple) {
	ArrayList<String> commonAtts = new ArrayList<String>();
	
	ArrayList<String> schema1 = this.schema;
	ArrayList<String> schema2 = tuple.schema;
	
	for (String att : schema1) {
	    if (schema2.contains(att)) {
		commonAtts.add(att);
	    }
	}
	
	return commonAtts;
    }
    
    public String getTable() {
	return this.rel;
    }
    
    public ArrayList<String> getSchema() {
	return this.schema;
    }
    
    public ArrayList<String> getValues() {
	return this.values;
    }
    
    public String valueString() {
	String value = "";
	for (int i = 0; i < values.size(); i++) {
	    value += values.get(i) + ", ";
	}
	value = value.substring(0, value.length() - 2);
	return value;
    }
    
    public boolean equals(XTuple tuple) {
	// first, do NOT forget check the number of columns
	if (this.schema.size() != tuple.schema.size()) {
	    return false;
	}
	
	
	// only check schema and values
	for (int i = 0; i < schema.size(); i++) {
	    String col = schema.get(i);
	    String value = values.get(i);
	    if (!tuple.schema.contains(col)) { // check schema
		return false;
	    } else {
		int index = tuple.schema.indexOf(col);
		
		if (value != null || tuple.values.get(index) != null) { // they both are not null
		    if (value != null) { // when this is not null, check the value
			if (!value.equals(tuple.values.get(index))) {
			    return false; 
			}
		    } else {
			if (tuple.values.get(index) != null) { // when this is null, the other must be null
			    return false;
			}
		    }
		}
	    }
	}
	return true;
    }
    
    /**
     * Return true if all the three private members are the same
     * @param tuple
     */
    public boolean is(XTuple tuple) {
	
	// if they have same reference
	if (this == tuple) {
	    return true;
	}
	
	// check table name
	if (!this.rel.equals(tuple.rel)) {
	    return false;
	}
	
	// check schema and values
	for (int i = 0; i < schema.size(); i++) {
	    String col1 = schema.get(i);
	    if (!tuple.schema.contains(col1)) { // if no such column
		return false;
	    } else {
		int index = tuple.schema.indexOf(col1);
		String value1 = this.values.get(i);
		String value2 = tuple.values.get(index); // if values are not equal
		if (!value1.equals(value2)) {
		    return false;
		}
	    }
	}
	
	return true;
    }

}
