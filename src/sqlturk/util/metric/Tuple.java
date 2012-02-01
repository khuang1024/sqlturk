package sqlturk.util.metric;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;
import sqlturk.util.map.ColumnInfo;

public class Tuple {
    /*
     * here, the schema from FD or FD+ is the original column name of the source
     * tables, so we don't know to care about alias
     */
    private String source;
    private ArrayList<String> schema;
    private ArrayList<String> values;

    Tuple(String source, ArrayList<String> schema, ArrayList<String> values) {
	if (schema.size() != values.size()) {
	    throw new RuntimeException(
		    "Tuple:The sizes of schema and values do not match!");
	}
	this.source = source;
	this.schema = schema;
	this.values = values;
    }

    ArrayList<String> getSchema() {
	return this.schema;
    }

    ArrayList<String> getValues() {
	return this.values;
    }

    String getValueAt(int i) {
	return values.get(i);
    }

    /**
     * NOTE: order is important, due to we have to normalize the result with the
     * length of first tuple.
     * 
     * @param tup1
     * @param tup2
     * @return return how many equivalent attributes of the same value the two
     *         tuples have
     * @throws SQLException
     */
    double sim(Tuple tup, Connection dbConn) throws SQLException {
	double count = 0;
	Tuple thisTuple = this;
	Tuple thatTuple = tup;
	int size = this.schema.size();
	for (int i = 0; i < thisTuple.getSchema().size(); i++) {

	    /*
	     * get the attribute name: - only tranlate columns in result tables
	     * - ignore __ROWID
	     */
	    String tup1Att = "";
	    if (thisTuple.source.startsWith(Parameters.QUERY_RESULT_PREFIX)
		    && thisTuple.source
			    .endsWith(Parameters.QUERY_RESULT_SUFFIX)
		    && !thisTuple.getSchema().get(i)
			    .equals(Parameters.ROWID_ATT_NAME)) {
		tup1Att = ColumnInfo.getOriginalTableColumnName(
			thisTuple.source, thisTuple.getSchema().get(i), dbConn);
	    } else {
		tup1Att = thisTuple.getSchema().get(i);
	    }

	    for (int j = 0; j < thatTuple.getSchema().size(); j++) {

		// get the attribute name
		String tup2Att = "";
		if (thatTuple.source.startsWith(Parameters.QUERY_RESULT_PREFIX)
			&& thatTuple.source
				.endsWith(Parameters.QUERY_RESULT_SUFFIX)
			&& !thatTuple.getSchema().get(j)
				.equals(Parameters.ROWID_ATT_NAME)) {
		    tup2Att = ColumnInfo.getOriginalTableColumnName(
			    thatTuple.source, thatTuple.getSchema().get(j),
			    dbConn);
		} else {
		    tup2Att = thatTuple.getSchema().get(j);
		}

		Attribute att1 = new Attribute(tup1Att);
		Attribute att2 = new Attribute(tup2Att);
		if (((tup1Att.equals(tup2Att)) || (Metric
			.isInSameForeignKeyChain(att1, att2)))
			&& (this.values.get(i).equals(tup.values.get(j)))) {
		    count++;
		    break;
		}

	    }
	}
	if (this.schema.contains(Parameters.ROWID_ATT_NAME)) {
	    size -= 1;
	}
	return count / size;
    }

}
