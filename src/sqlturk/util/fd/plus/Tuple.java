package sqlturk.util.fd.plus;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import sqlturk.configuration.Parameters;
import sqlturk.util.map.ColumnInfo;

class Tuple {

    private String source;
    private ArrayList<String> schema;
    private ArrayList<String> values;

    Tuple(String source, ArrayList<String> schema, ArrayList<String> values) {
	if (schema.size() != values.size()) {
	    throw new RuntimeException("Lengths of schema and values dismatch!");
	}

	this.source = source;
	this.schema = schema;
	this.values = values;
    }

    String getSource() {
	return this.source;
    }

    ArrayList<String> getSchema() {
	return this.schema;
    }

    String getValueAt(int i) {
	return this.values.get(i);
    }

    String getValueAt(String colName, Connection dbConn) throws SQLException {

	if (colName.equals(Parameters.ROWID_ATT_NAME)) {
	    // when look for ROWID

	    Statement stmt = dbConn.createStatement();
	    ResultSet rs = stmt.executeQuery("SELECT * FROM " + this.source);
	    // ResultSetMetaData rsmd = rs.getMetaData();
	    while (rs.next()) {
		boolean found = true;

		// compare each column except ROWID to see if the tuple
		// is the one we want to find
		for (int i = 0; i < this.schema.size(); i++) {
		    String attribute = schema.get(i);

		    // System.out.println("debug:\tattribute=" + attribute);
		    // System.out.println("debug:\tsource=" + this.source);
		    // System.out.println("debug:\tvalue=" + values.get(i));
		    // System.out.println("debug:\ti=" + i);

		    if (rs.getString(attribute) != null) {
			if (!rs.getString(attribute).equals(values.get(i))) {
			    found = false;
			    break;
			}
		    } else if (values.get(i) != null) {
			found = false;
			break;
		    }
		}
		if (found) {
		    return rs.getString(colName);
		}
	    }
	    throw new RuntimeException("No ROWID found.");
	} else {
	    // when look for a normal attribute
	    for (int i = 0; i < this.schema.size(); i++) {
		if (this.schema.get(i).equals(colName)) {
		    return this.values.get(i);
		}
	    }
	}
	throw new RuntimeException("No column found.");
	// return null;
    }

    static boolean isSame(Tuple t1, Tuple t2) {
	// boolean isSame = true;

	// 1. check the source table name
	if (!t1.source.equals(t2.source)) {
	    return false;
	}

	// 2. check the schema
	if (t1.schema.size() != t2.schema.size()) {
	    return false;
	}
	for (int i = 0; i < t1.schema.size(); i++) {
	    if (!t1.schema.get(i).equals(t2.schema.get(i))) {
		return false;
	    }
	}

	// 3. check the values
	if (t1.values.size() != t2.values.size()) {
	    return false;
	}
	for (int i = 0; i < t1.values.size(); i++) {

	    if ((t1.values.get(i) != null) && (t2.values.get(i) != null)) {
		if (!t1.values.get(i).equals(t2.values.get(i))) {
		    return false;
		}
	    }
	}

	return true;
    }

    /*
     * NOTE: the consistent MUST be checked under the original attribute!
     */
    static boolean isJoinConsistent(Tuple t1, Tuple t2, Connection dbConn)
	    throws SQLException {

	boolean hasCommonAttributes = false;
	boolean isConsistent = false;

	/*
	 * NOTE: here we should pre-process the tuples, since the result tuple
	 * may have alias in their schema. But actually, we should use their
	 * "real" attribute names.
	 */

	ArrayList<String> schema1 = t1.schema;
	ArrayList<String> schema2 = t2.schema;
	ArrayList<String> values1 = t1.values;
	ArrayList<String> values2 = t2.values;

	for (int i = 0; i < schema1.size(); i++) {
	    for (int j = 0; j < schema2.size(); j++) {
		String originalAtt1 = ColumnInfo.getOriginalTableColumnName(
			t1.getSource(), schema1.get(i), dbConn);
		String originalAtt2 = ColumnInfo.getOriginalTableColumnName(
			t2.getSource(), schema2.get(j), dbConn);
		// String originalAtt1 = getOriginalAttName(schema1.get(i),
		// t1.getSource());
		// String originalAtt2 = getOriginalAttName(schema2.get(j),
		// t2.getSource());
		if (originalAtt1.equals(originalAtt2)) {
		    hasCommonAttributes = true;
		    if (values1.get(i) == null || values2.get(j) == null) {
			return false;
		    }
		    if (values1.get(i).equals(values2.get(j))) {
			isConsistent = true;
		    } else {
			return false;
		    }
		}
	    }
	}

	return (hasCommonAttributes && isConsistent);

    }

    // private static ArrayList<String> proprocess(ArrayList<String> schema,
    // String resultTableName) {
    // ArrayList<String> newSchema = new ArrayList<String>();
    //
    // //debug
    // System.out.println("debug:\tresultTableName = " + resultTableName);
    // for (int i = 0; i < schema.size(); i++) {
    // String realName = SPJParser.getAttRealName(schema.get(i),
    // resultTableName);
    // if (realName == null) {
    // newSchema.add(schema.get(i));
    // } else {
    // newSchema.add(realName);
    // }
    // }
    //
    // return newSchema;
    // }

    String gettValuesString() {
	String val = "";
	for (String v : values) {
	    val += (v + ", ");
	}
	return val;
    }

    void printValues() {
	for (String v : values) {
	    System.out.print(v + "\t");
	}
	System.out.println();
    }

    void printSchema() {
	for (String s : schema) {
	    System.out.print(s + "\t");
	}
	System.out.println();
    }
}