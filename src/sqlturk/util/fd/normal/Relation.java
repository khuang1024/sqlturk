package sqlturk.util.fd.normal;

import java.util.ArrayList;

class Relation {
    private String relationName;
    private ArrayList<Tuple> tuples;

    Relation(String relationName, ArrayList<Tuple> tuples) {
	this.relationName = relationName;
	this.tuples = tuples;
    }

    String getRelationName() {
	return this.relationName;
    }

    ArrayList<Tuple> getTuples() {
	return this.tuples;
    }
}
