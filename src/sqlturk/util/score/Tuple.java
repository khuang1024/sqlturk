package sqlturk.util.score;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;
import sqlturk.util.map.ColumnInfo;

class Tuple {
    private String source; // where is this tuple from
    private ArrayList<String> schema;
    private ArrayList<String> values;

    Tuple(String source, ArrayList<String> schema, ArrayList<String> values) {
	this.source = source;
	this.schema = schema;
	this.values = values;
    }

    public String getSource() {
	return source;
    }

    public ArrayList<String> getSchema() {
	return schema;
    }

    public ArrayList<String> getValues() {
	return values;
    }

    public boolean isContributedBy(Tuple tuple, Connection dbConn)
	    throws SQLException {

	if (!tuple.getSource().startsWith(Parameters.QUERY_RESULT_PREFIX)
		|| !tuple.getSource().endsWith(Parameters.QUERY_RESULT_SUFFIX)) {
	    throw new RuntimeException("Not a tuple form result table");
	}

	if (!source.equals(Parameters.FD_PLUS_REL_NAME)) {
	    throw new RuntimeException("Not a tuple form FD+ table");
	}

	/*
	 * when we can find every column of the given tuple in this tuple, and
	 * their values are equal, we say the given tuple contributes to this
	 * tuple.
	 */
	Tuple thisTuple = this;
	Tuple thatTuple = tuple;
	for (int i = 0; i < thatTuple.schema.size(); i++) {

	    // ignore ROWID
	    if (!thatTuple.schema.get(i).equals(Parameters.ROWID_ATT_NAME)) {
		String newColumnName = ColumnInfo.getOriginalTableColumnName(
			thatTuple.source, thatTuple.schema.get(i), dbConn);
		if (!thisTuple.schema.contains(newColumnName)) {
		    return false;
		}

		for (int j = 0; j < thisTuple.schema.size(); j++) {

		    if (thisTuple.schema.get(j).equals(newColumnName)) {

			// when they have same column but the value is not equal
			if (!thisTuple.values.get(j).equals(
				thatTuple.values.get(i))) {
			    return false;
			}
		    }
		}
	    }
	}
	return true;
    }

    public String getRowId() {
	for (int i = 0; i < schema.size(); i++) {
	    if (schema.get(i).equals(Parameters.ROWID_ATT_NAME)) {
		return values.get(i);
	    }
	}
	return "No " + Parameters.ROWID_ATT_NAME + " found.";
    }

    public String getInfo() {
	String info = "";
	info += "rowId: " + getRowId() + "| ";
	info += "source: " + source + "| ";
	info += "schema and values: ";
	for (int i = 0; i < schema.size(); i++) {
	    info += "(" + schema.get(i) + ", " + values.get(i) + "), ";
	}
	return info;
    }
}
