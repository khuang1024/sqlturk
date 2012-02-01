package sqlturk.util.connected;

import java.util.ArrayList;

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
}
