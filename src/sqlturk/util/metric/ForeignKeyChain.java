package sqlturk.util.metric;

import java.util.ArrayList;

class ForeignKeyChain {
    private static int chainIdStart = 0;

    private final int chainId;
    private ArrayList<Attribute> foreignKeyChain;

    ForeignKeyChain() {
	this.chainId = chainIdStart++;
	this.foreignKeyChain = new ArrayList<Attribute>();
    }

    ForeignKeyChain(ArrayList<Attribute> foreignKeyChain) {
	this.chainId = chainIdStart++;
	this.foreignKeyChain = foreignKeyChain;
    }

    void addAttribute(Attribute attribute) {
	this.foreignKeyChain.add(attribute);
    }

    int getChainId() {
	return this.chainId;
    }

    boolean hasAttribute(Attribute attribute) {
	for (int i = 0; i < this.foreignKeyChain.size(); i++) {
	    Attribute att = this.foreignKeyChain.get(i);
	    if (att.isSame(attribute)) {
		return true;
	    }
	}
	return false;
    }

    String getChainString() {
	String chainString = "";
	for (Attribute att : foreignKeyChain) {
	    chainString += att.getAttributeString() + ", ";
	}
	return chainString;
    }
}
