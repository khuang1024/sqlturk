package sqlturk.util.tableaux;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

class AliasPool {

    private static final String ALIAS_NAME_PREFIX = "t";
    private static final String ALIAS_NAME_SUFFIX = "";
    private static final int ALIAS_NAME_START = 1;

    private int globalAliasID;

    // all alias , have duplicates
    private ArrayList<AliasPair> aliasMap;

    // record the relations that have been referenced by other relations.
    private HashSet<String> referencedPool;

    // record the relations that have referenced other relations
    private HashSet<String> referencingPool;
    private HashMap<String, ArrayList<AliasPair>> localReferencingPool;

    // debug
    void printAliasMap() {
	for (AliasPair ap : aliasMap) {
	    System.out.println("debug:\t" + ap.getOriginalName() + "->"
		    + ap.getAliasName());
	}
    }

    // get the realname-alias
    ArrayList<String> getFKAlias() {
	ArrayList<String> fkAlias = new ArrayList<String>();
	for (AliasPair ap : aliasMap) {
	    fkAlias.add(ap.getOriginalName() + " as " + ap.getAliasName());
	}
	return fkAlias;
    }

    AliasPool() {
	this.globalAliasID = ALIAS_NAME_START;
	this.aliasMap = new ArrayList<AliasPair>();
	this.referencedPool = new HashSet<String>();
	this.referencingPool = new HashSet<String>();
	this.localReferencingPool = new HashMap<String, ArrayList<AliasPair>>();
    }

    boolean isInAliasMapAsOriginalName(String originalName) {
	for (AliasPair ap : aliasMap) {
	    if (ap.getOriginalName().equals(originalName)) {
		return true;
	    }
	}
	return false;
    }

    boolean isInAliasMapAsAliasName(String aliasName) {
	for (AliasPair ap : aliasMap) {
	    if (ap.getAliasName().equals(aliasName)) {
		return true;
	    }
	}
	return false;
    }

    /*
     * referencingTarget is the relation which references to this
     * relation(originalName)
     */
    boolean isGolobalReferenced(String originalName) {
	if (referencedPool.contains(originalName)) {
	    return true;
	} else {
	    return isInAliasMapAsOriginalName(originalName);
	}
    }

    /**
     * e.g. exp1.v1=exp2.v2
     * 
     * @param referencingName
     *            exp1
     * @param originalName
     *            exp2
     * @return
     */
    boolean isLocalReferenced(String referencingName, String originalName) {
	// debug: *************** pay attention, here is a Runtime exception!!!!
	if (!this.localReferencingPool.containsKey(referencingName)) {
	    return false;
	}

	ArrayList<AliasPair> aliasArray = this.localReferencingPool
		.get(referencingName);
	for (AliasPair ap : aliasArray) {
	    if (ap.getOriginalName().equals(originalName)) {
		return true;
	    }
	}
	return false;
    }

    String getLocalReferencedName(String referencingName, String originalName) {
	ArrayList<AliasPair> aliasArray = this.localReferencingPool
		.get(referencingName);
	for (AliasPair ap : aliasArray) {
	    if (ap.getOriginalName().equals(originalName)) {
		return ap.getAliasName();
	    }
	}
	System.err.println("No local alias found!");
	return null;
    }

    boolean isReferencing(String originalName) {
	if (referencingPool.contains(originalName)) {
	    return true;
	} else {
	    return false;
	}
    }

    void createLocalReferencingPollFor(String originalName) {
	this.localReferencingPool.put(originalName, new ArrayList<AliasPair>());
    }

    String createReferencingAliasName(String originalName) {
	String aliasName = ALIAS_NAME_PREFIX + (globalAliasID++)
		+ ALIAS_NAME_SUFFIX;

	while (isInAliasMapAsAliasName(aliasName)) {
	    aliasName = ALIAS_NAME_PREFIX + (globalAliasID++)
		    + ALIAS_NAME_SUFFIX;
	}

	this.referencingPool.add(originalName);
	this.aliasMap.add(new AliasPair(originalName, aliasName));
	this.localReferencingPool.put(originalName, new ArrayList<AliasPair>());
	return aliasName;
    }

    String createReferencedAliasName(String referencingName, String originalName) {
	String aliasName = ALIAS_NAME_PREFIX + (globalAliasID++)
		+ ALIAS_NAME_SUFFIX;

	while (isInAliasMapAsAliasName(aliasName)) {
	    aliasName = ALIAS_NAME_PREFIX + (globalAliasID++)
		    + ALIAS_NAME_SUFFIX;
	}

	this.referencedPool.add(originalName);
	this.aliasMap.add(new AliasPair(originalName, aliasName));
	this.localReferencingPool.get(referencingName).add(
		new AliasPair(originalName, aliasName));
	return aliasName;
    }

    String getReferencingAliasName(String originalName) {
	for (int i = 0; i < this.aliasMap.size(); i++) {
	    if (this.aliasMap.get(i).getOriginalName().equals(originalName)) {
		return this.aliasMap.get(i).getAliasName();
	    }
	}
	System.err.println("No alias name found!");
	return null;
    }

    void addToAliasMap(String originalName, String aliasName) {
	this.aliasMap.add(new AliasPair(originalName, aliasName));
    }
}
