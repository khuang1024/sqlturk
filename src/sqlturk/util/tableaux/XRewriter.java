package sqlturk.util.tableaux;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.regex.Pattern;

import sqlturk.configuration.Parameters;

public class XRewriter {
    
    private XRewriter() {}
    
    public static ArrayList<String> getRewrittenQueries(ArrayList<String> oQueries, Connection dbConn)
	    throws SQLException {
	ArrayList<String> rQueries = new ArrayList<String>();
	for (String oQuery : oQueries) {
	    rQueries.add(getRewrittenQuery(oQuery, dbConn));
	}
	return rQueries;
    }
    
    public static String getRewrittenQuery(String oQuery, Connection dbConn)
	    throws SQLException {
	String rQuery = null;
	
	// distinct
	boolean hasDistinct = Pattern.compile(Pattern.quote(" DISTINCT "), 
				Pattern.CASE_INSENSITIVE).matcher(oQuery).find();
	if (hasDistinct) {
	    int indexoOfDistinct = oQuery.toUpperCase().indexOf(" DISTINCT ");
	    oQuery = oQuery.substring(0, indexoOfDistinct) + oQuery.substring(indexoOfDistinct + 9);
	}
	
	XParser xParser = new XParser(oQuery);
	
	// rewrite select
	ArrayList<String> oCols = xParser.getAllOriginalCols(); // the original projection
	ArrayList<String> rCols = new ArrayList<String>(); // the new alias
	ArrayList<String> sCols = new ArrayList<String>(); // the new projection string
	for (String oCol : oCols) {
	    String oTable = getOriginalTable(oCol, xParser, dbConn);
	    String oColumn = getOriginalCol(oCol);
	    String rColumn = getRewrittenCol(oTable, oColumn);
	    rCols.add(rColumn);
	    sCols.add(oTable + "." + oColumn + " AS " + rColumn);
	}
	
	// rewrite from
	ArrayList<String> oTables = xParser.getAllOriginalTables();
	
	// rewrite where
	String wheres = xParser.getWherePredicates();
	ArrayList<String> allTableAlias = xParser.getAllAliasTables();
	for (String alias : allTableAlias) {
	    wheres = wheres.replaceAll("\\s+" + alias + "\\.", " " + xParser.getOriginalTable(alias) + ".");
	    wheres = wheres.replaceAll("\\+" + alias + "\\.", "+" + xParser.getOriginalTable(alias) + ".");
	    wheres = wheres.replaceAll("-" + alias + "\\.", "-" + xParser.getOriginalTable(alias) + ".");
	    wheres = wheres.replaceAll("\\*" + alias + "\\.", "*" + xParser.getOriginalTable(alias) + ".");
	    wheres = wheres.replaceAll("/" + alias + "\\.", "/" + xParser.getOriginalTable(alias) + ".");
	    wheres = wheres.replaceAll("\\(" + alias + "\\.", "(" + xParser.getOriginalTable(alias) + ".");
	    wheres = wheres.replaceAll("\\)" + alias + "\\.", ")" + xParser.getOriginalTable(alias) + ".");
	    wheres = wheres.replaceAll("=" + alias + "\\.", "=" + xParser.getOriginalTable(alias) + ".");
	    wheres = wheres.replaceAll(">" + alias + "\\.", ">" + xParser.getOriginalTable(alias) + ".");
	    wheres = wheres.replaceAll("<" + alias + "\\.", "<" + xParser.getOriginalTable(alias) + ".");
	    if (wheres.startsWith(alias + ".")) {
		wheres = xParser.getOriginalTable(alias) + wheres.substring(alias.length());
	    }
	}
	
	// append tableaux
	XTableaux xTableaux = new XTableaux(xParser, dbConn);
	String additionalFroms = xTableaux.getAdditionalFroms();
	String additionalWheres = xTableaux.getAdditionalWheres();
	//System.out.println("additionalFroms:\n-" + additionalFroms);
	//System.out.println("additionalWheres:\n-" + additionalWheres);
	
	
	// assemble all
	rQuery = "SELECT DISTINCT";
	for (String sCol : sCols) {
	    rQuery += " " + sCol + ",";
	}
	rQuery = rQuery.substring(0, rQuery.length()-1); // remove period
	rQuery += " FROM";
	for (String oTable : oTables) {
	    rQuery += " " + oTable + ",";
	}
	rQuery = rQuery.substring(0, rQuery.length()-1);
	rQuery += (additionalFroms == null)? "" : "," + additionalFroms;
	rQuery += " WHERE ";
	rQuery += wheres;
	rQuery += (additionalWheres == null)? "" : " AND" + additionalWheres;
	
	// finally, replace extra whitespaces
	rQuery = rQuery.trim().replaceAll("\\s+", " ");
	
	return rQuery;
    }
    
    private static String getRewrittenCol(String oTable, String oColumn) {
	return oTable + Parameters.REWRITE_ATT_DELIM + oColumn;
    }
    
    private static String getOriginalCol(String oCol) {
	if (oCol.contains(".")) {
	    return oCol.trim().split("\\.")[1];
	} else {
	    return oCol.trim();
	}
    }
    
    private static String getOriginalTable(String oCol, XParser xParser, 
	    Connection dbConn) throws SQLException {
	String table = getTablePrefix(oCol);
	if (table != null) { // if it is R.A
	    return xParser.getOriginalTable(table);
	} else { // if it is only A
	    Statement stmt = dbConn.createStatement();
	    ResultSet rs = null;
	    ArrayList<String> tables = xParser.getAllOriginalTables();
	    for (String t : tables) {
		rs = stmt.executeQuery("DESC " + t);
		while (rs.next()) {
		    if (rs.getString(1).equals(oCol)) {
			rs.close();
			stmt.close();
			return t;
		    }
		}
	    }
	    rs.close();
	    stmt.close();
	    throw new RuntimeException("Error: " + oCol + " Original table not found.");
	}
    }
    
    private static String getTablePrefix(String col) {
	if (col.contains(".")) {
	    return col.split("\\.")[0].trim();
	} else {
	    return null;
	}
    }

    /**
     * @param args
     * @throws ClassNotFoundException 
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     * @throws SQLException 
     */
    public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
//	String query = "SELECT X.S_NAME, S_PHONE FROM SUPPLIER as X, PARTSUPP WHERE PS_SUPPKEY=X.S_SUPPKEY  AND S_ACCTBAL>1000  AND PS_SUPPLYCOST>2096";
//	String query = "select Name from Country where Region = 'Western Europe'";
	String query = "SELECT S_NAME, S_PHONE  FROM SUPPLIER,  PARTSUPP,  PART WHERE S_ACCTBAL > 1000 AND S_SUPPKEY = PS_SUPPKEY AND PS_PARTKEY = P_PARTKEY  AND P_RETAILPRICE > 2096";
	
	Class.forName("com.mysql.jdbc.Driver").newInstance();

	Connection dbConn = null;
	
	// use soe server or test locally
	if (Parameters.USE_SERVER) {
	    dbConn = DriverManager.getConnection(Parameters.MYSQL_CONNECTION_STRING);
	    System.out.println("Using server.\n");
	} else {
	    dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL, Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
	    System.out.println("Using local.\n");
	}
	System.out.println(query);
	System.out.println(XRewriter.getRewrittenQuery(query, dbConn));
    }

}
