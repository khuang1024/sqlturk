package sqlturk.util.tableaux;

import java.util.ArrayList;
import java.util.HashMap;

@Deprecated
class Clause {
    private HashMap<String, String> alias; // the clause has<rel, alias>
    private String key; // the name of the clause, "select", "from", "where"
    private ArrayList<String> allElements;
    private ArrayList<String> elementsWithoutAlias;
    private ArrayList<String> functions;
    private ArrayList<String> functionParameters;

    // Clause(String key, HashMap<String, String> alias,
    // ArrayList<String> relations,
    // ArrayList<String> relationsWithoutAlias) {
    // this.key = key;
    // this.alias = alias;
    // this.allElements = relations;
    // this.elementsWithoutAlias = relationsWithoutAlias;
    // this.functions = new ArrayList<String>();
    // this.functionParameters = new ArrayList<String>();
    // }

    Clause(String key, HashMap<String, String> alias,
	    ArrayList<String> relations,
	    ArrayList<String> relationsWithoutAlias,
	    ArrayList<String> functions, ArrayList<String> functionParameters) {
	this.key = key;
	this.alias = alias;
	this.allElements = relations;
	this.elementsWithoutAlias = relationsWithoutAlias;
	this.functions = functions;
	this.functionParameters = functionParameters;
    }

    void addAlias(String realName, String alias) {
	this.alias.put(realName, alias);
    }

    HashMap<String, String> getAlias() {
	return alias;
    }

    String getClauseName() {
	return key;
    }

    ArrayList<String> getAllElements() {
	return this.allElements;
    }

    ArrayList<String> getRelationsWithoutAlias() {
	return this.elementsWithoutAlias;
    }

    ArrayList<String> getFunctions() {
	return this.functions;
    }

    ArrayList<String> getFunctionParameters() {
	return this.functionParameters;
    }
}
