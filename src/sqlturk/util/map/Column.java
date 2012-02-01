package sqlturk.util.map;

public class Column {
    private String resultTableName;
    private String resultColumnName;
    private String sourceTableAliasName;
    private String sourceTableOriginalName;
    private String sourceColumnName;

    public Column(String resultTableName, String resultColumnName,
	    String sourceTableAliasName, String sourceTableOriginalName,
	    String sourceColumnName) {
	this.resultTableName = resultTableName;
	this.resultColumnName = resultColumnName;
	this.sourceTableAliasName = sourceTableAliasName;
	this.sourceTableOriginalName = sourceTableOriginalName;
	this.sourceColumnName = sourceColumnName;
    }

    public void printColumn() {
	System.out.println("\tresultTableName\t\t= " + resultTableName);
	System.out.println("\tresultColumnName\t= " + resultColumnName);
	System.out.println("\tsourceTableAliasName\t= " 
		+ sourceTableAliasName);
	System.out.println("\tsourceTableOriginalName\t= "
		+ sourceTableOriginalName);
	System.out.println("\tsourceColumnName\t= " + sourceColumnName);
    }

    public String getResultTableName() {
	return resultTableName;
    }

    public String getResultColumnName() {
	return resultColumnName;
    }

    public String getSourceTableAliasName() {
	return sourceTableAliasName;
    }

    public String getSourceTableOriginalName() {
	return sourceTableOriginalName;
    }

    public String getSourceColumnName() {
	return sourceColumnName;
    }

}
