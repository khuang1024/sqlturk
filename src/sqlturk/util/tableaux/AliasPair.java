package sqlturk.util.tableaux;

class AliasPair {
    private String originalName;
    private String aliasName;

    AliasPair(String originalName, String aliasName) {
	this.originalName = originalName;
	this.aliasName = aliasName;
    }

    public String getOriginalName() {
	return this.originalName;
    }

    public String getAliasName() {
	return this.aliasName;
    }
}
