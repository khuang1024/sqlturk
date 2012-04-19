package sqlturk.util.tableaux;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

public class XParser {
    
    private String oQuery;
    private HashMap<String, String> select; 
    private HashMap<String, String> from;
    private String where;
    
    XParser(String oQuery) {
	this.oQuery = oQuery;
	this.select = new HashMap<String, String>(); // select <alias, original> (original can be Country.Code or Code, depends on the original input by workers)
	this.from = new HashMap<String, String>(); // from <alias, original>
	this.where = null; // where predicates
	init();
    }
    
    ArrayList<String> getAllOriginalCols() {
	ArrayList<String> cols = new ArrayList<String>();
	for (String k : this.select.keySet()) {
	    cols.add(this.select.get(k));
	}
	return cols;
    }
    
    ArrayList<String> getAllOriginalTables() {
	ArrayList<String> tables = new ArrayList<String>();
	for (String t : this.from.keySet()) {
	    tables.add(this.from.get(t));
	}
	return tables;
    }
    
    ArrayList<String> getAllAliasTables() {
	ArrayList<String> tables = new ArrayList<String>();
	for (String t : this.from.keySet()) {
	    tables.add(t);
	}
	return tables;
    }
    
    String getOriginalTable(String alias) {
	if (this.from.get(alias) == null) {
	    throw new RuntimeException("Error: No such original table.");
	}
	return this.from.get(alias);
    }
    
    String getWherePredicates() {
	if (this.where == null) {
	    throw new RuntimeException("Error: No predicates in where clause.");
	}
	return this.where;
    }
    
    String replaceAlias(String table, String wheres, Connection dbConn) throws SQLException {
    	ArrayList<String> columns = this.getColumns(table, dbConn);
    	wheres = " " + wheres; // make the first replaceAll work
    	for (String column : columns) {
    		wheres = wheres.replaceAll("\\s+" + column + "\\s+", " " + table + "." + column + " ");
		    wheres = wheres.replaceAll("\\+" + column + "\\s+", "+" + table + "." + column + ".");
		    wheres = wheres.replaceAll("-" + column + "\\s+", "-" + table + "." + column + ".");
		    wheres = wheres.replaceAll("\\*" + column + "\\s+", "*" + table + "." + column + ".");
		    wheres = wheres.replaceAll("/" + column + "\\s+", "/" + table + "." + column + ".");
		    wheres = wheres.replaceAll("\\(" + column + "\\s+", "(" + table + "." + column + ".");
		    wheres = wheres.replaceAll("\\)" + column + "\\s+", ")" + table + "." + column + ".");
		    wheres = wheres.replaceAll("=" + column + "\\s+", "=" + table + "." + column + ".");
		    wheres = wheres.replaceAll(">" + column + "\\s+", ">" + table + "." + column + ".");
		    wheres = wheres.replaceAll("<" + column + "\\s+", "<" + table + "." + column + ".");
    	}
    	return wheres;
    }
    
    private ArrayList<String> getColumns(String table, Connection dbConn) throws SQLException {
    	ArrayList<String> columns = new ArrayList<String>();
    	Statement stmt = dbConn.createStatement();
    	ResultSet rs = stmt.executeQuery("DESC " + table);
    	while (rs.next()) {
    		columns.add(rs.getString(1));
    	}
    	rs.close();
    	stmt.close();
    	return columns;
    }
    
    /**
     * 
     * @param oQuery the original query
     * @param dbConn the database connection object
     * @return
     */
    private void init() {
	int indexOfSelect = this.oQuery.toUpperCase().indexOf("SELECT ");
	int indexOfFrom = this.oQuery.toUpperCase().indexOf("FROM ");
	int indexOfWhere = this.oQuery.toUpperCase().indexOf("WHERE ");
	
	if (this.oQuery.toUpperCase().indexOf("HAVING ") != -1) {
	    throw new RuntimeException("Error: Contain HAVING.");
	}
	if (this.oQuery.toUpperCase().indexOf("ORDER BY ") != -1) {
	    throw new RuntimeException("Error: Contain ORDERY BY.");
	}
	if (this.oQuery.toUpperCase().indexOf("GROUP BY ") != -1) {
	    throw new RuntimeException("Error: Contain GROUP BY.");
	}
	if (indexOfSelect == -1 || indexOfFrom == -1) {
//	    System.out.println(oQuery);
	    throw new RuntimeException("Error: Not a valid query. " + oQuery);
	}
	if (indexOfWhere == -1) {
		indexOfWhere = oQuery.length();
	}
	
	// select clause
	String[] selects = oQuery.substring(indexOfSelect + 6, indexOfFrom).split(",");
	for (int i = 0; i < selects.length; i++) {
	    selects[i] = selects[i].trim();
	    if (selects[i].contains(" ")) {
		String[] tables;
		if (Pattern.compile(Pattern.quote(" AS "), Pattern.CASE_INSENSITIVE).matcher(selects[i]).find()) {
		    tables = selects[i].split("(?i)as");
		    for (int j = 0; j < tables.length; j++) {
			tables[j] = tables[j].trim();
		    }
		} else {
		    tables = selects[i].split("\\s+");
		}
		for (int j = 0; j < tables.length; j++) {
			tables[j] = tables[j].trim();
		}
		this.select.put(tables[1], tables[0]);
	    } else {
		this.select.put(selects[i], selects[i]);
	    }
	}
	selects = null;
//	for (String k : this.select.keySet()) {
//	    System.out.println(k + "<-" + this.select.get(k));
//	}
	
	// from clause
	String[] froms = oQuery.substring(indexOfFrom + 4, indexOfWhere).split(",");
	for (int i = 0; i < froms.length; i++) {
	    froms[i] = froms[i].trim();
	    if (froms[i].contains(" ")) {
		String[] tables;
		if (Pattern.compile(Pattern.quote(" AS "), Pattern.CASE_INSENSITIVE).matcher(froms[i]).find()) {
		    tables = froms[i].split("(?i)as");
		    for (int j = 0; j < tables.length; j++) {
			tables[j] = tables[j].trim();
		    }
		} else {
		    tables = froms[i].split("\\s+");
		}
		for (int j = 0; j < tables.length; j++) {
			tables[j] = tables[j].trim();
		}
		this.from.put(tables[1], tables[0]);
	    } else {
		this.from.put(froms[i], froms[i]);
	    }
	}
	froms = null;
//	for (String k : this.from.keySet()) {
//	    System.out.println(k + "<-" + this.from.get(k));
//	}
	
	// where clause
	if (indexOfWhere < oQuery.length()) {
		this.where = oQuery.substring(indexOfWhere + 5, oQuery.length()).trim();
	} else {
		this.where = "";
	}
	
    }
    

    /**
     * @param args
     */
    public static void main(String[] args) {
//	String q = "SELECT S_NAME, S_PHONE FROM SUPPLIER, PARTSUPP WHERE PS_SUPPKEY=S_SUPPKEY  AND S_ACCTBAL>1000  AND PS_SUPPLYCOST>2096";
//	XParser xp = new XParser(q);
//	ArrayList<String> t = xp.getOriginalCols();
//	for(String s : t) {
//	    System.out.println(s);
//	}
//	System.out.println(xp.getOriginalTable("x"));
//	System.out.println(xp.getWherePredicates());
    }

}
