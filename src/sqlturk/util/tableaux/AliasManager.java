package sqlturk.util.tableaux;

import java.util.ArrayList;

import sqlturk.configuration.Parameters;

class AliasManager {
    private ArrayList<AliasPair> globalAliasPairWarehouse;
    private ArrayList<String> referencedRelations;
    private static int aliasId = Parameters.TABLEAUX_ALIAS_START_ID;

    AliasManager() {
	globalAliasPairWarehouse = new ArrayList<AliasPair>();
	referencedRelations = new ArrayList<String>();
    }

    String createAliasPair(String originalName) {
	String aliasName = Parameters.TABLEAUX_ALIAS_PREFIX + (aliasId++)
		+ Parameters.TABLEAUX_ALIAS_SUFFIX;
	while (this.isInGlobalAliasPair(aliasName)) {
	    aliasName = Parameters.TABLEAUX_ALIAS_PREFIX + (aliasId++)
		    + Parameters.TABLEAUX_ALIAS_SUFFIX;
	    // DEBUG
	    System.out.println("debug:\t" + aliasName);
	}
	this.globalAliasPairWarehouse
		.add(new AliasPair(originalName, aliasName));
	return aliasName;
    }

    void addGlobalAliasPair(String originalName, String aliasName) {
	globalAliasPairWarehouse.add(new AliasPair(originalName, aliasName));
    }

    void addReferencedRelation(String relationName) {
	referencedRelations.add(relationName);
    }

    boolean isReferencedRelation(String relationName) {
	return this.referencedRelations.contains(relationName);
    }

    boolean isInGlobalAliasPair(String relationName) {
	for (AliasPair ap : this.globalAliasPairWarehouse) {
	    if (ap.getOriginalName().equals(relationName)) {
		return true;
	    }
	}
	return false;
    }

    String getGlobalAliasName(String relationName) {
	for (int i = 0; i < this.globalAliasPairWarehouse.size(); i++) {
	    if (this.globalAliasPairWarehouse.get(i).getOriginalName()
		    .equals(relationName)) {
		return this.globalAliasPairWarehouse.get(i).getAliasName();
	    }
	}
	return null;
    }

    String getLastGlobalAliasName(String relationName) {
	for (int i = globalAliasPairWarehouse.size() - 1; i >= 0; i--) {
	    if (this.globalAliasPairWarehouse.get(i).getOriginalName()
		    .equals(relationName)) {
		return this.globalAliasPairWarehouse.get(i).getAliasName();
	    }
	}
	return null;
    }

    ArrayList<String> getAllAliasPairs() {
	ArrayList<String> allAliasPairs = new ArrayList<String>();
	for (AliasPair ap : this.globalAliasPairWarehouse) {
	    if (ap.getOriginalName().equals(ap.getAliasName())) {
		allAliasPairs.add(ap.getOriginalName());
	    } else {
		allAliasPairs.add(ap.getOriginalName() + " AS "
			+ ap.getAliasName());
	    }
	}
	return allAliasPairs;
    }
}
