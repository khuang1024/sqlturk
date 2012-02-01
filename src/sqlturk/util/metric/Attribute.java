package sqlturk.util.metric;

class Attribute {

    private String attributeName;

    Attribute(String attributeName) {
	this.attributeName = attributeName;
    }

    boolean isSame(Attribute att) {
	if (this.attributeName.equals(att.attributeName)) {
	    return true;
	} else {
	    return false;
	}
    }

    String getAttributeString() {
	return "<" + attributeName + ">";
    }
}
