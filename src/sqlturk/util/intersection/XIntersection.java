package sqlturk.util.intersection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;
import sqlturk.util.common.XCommon;

public class XIntersection {
    
    private static int tempTableIndex = 0;
    
    private XIntersection() {
	throw new AssertionError();
    }
    
    public static String createIntersecTable(Connection dbConn) throws SQLException {
	ArrayList<String> resultTables = XCommon.getAllResultTables(dbConn);
	return createIntersecTable(resultTables, dbConn);
    }
    
    private static String createIntersecTable(ArrayList<String> rels,
	    Connection dbConn) throws SQLException {
	Statement stmt = dbConn.createStatement();
	
	ArrayList<String> commonCols = XCommon.getCommonColumns(rels, dbConn);
	
	String commonAtt = "";
	for (String att : commonCols) {
	    commonAtt += att + ", ";
	}
	
	// first, drop table
	stmt.execute("DROP TABLE IF EXISTS " + Parameters.INTERSECTION_REL_NAME);
	
	if (commonAtt.equals("")) { // if no common attributes
	    String create = "CREATE TABLE " + Parameters.INTERSECTION_REL_NAME + " (NO_COMMON VARCHAR(10))";
	    String instert = "INSERT INTO " + Parameters.INTERSECTION_REL_NAME + " VALUES (\"NO_COMMON\")";
	    System.out.println(create);
	    stmt.execute(create);
	    stmt.execute(instert);
	} else {
	    commonAtt = commonAtt.substring(0, commonAtt.length()-2);
	    
	    String currentTable = getTempTable();
	    
	    stmt.execute("DROP TABLE IF EXISTS " + currentTable);
	    stmt.execute("CREATE TABLE " + currentTable + " AS SELECT * FROM " + rels.get(0));
	    
	    for (int i = 1; i < rels.size(); i++) {
		String newTempTable = getTempTable();
		stmt.execute("DROP TABLE IF EXISTS " + newTempTable);
		stmt.execute("CREATE TABLE " + newTempTable + " AS " + getIntersectionQuery(commonAtt, currentTable, rels.get(i)));
		stmt.execute("DROP TABLE " + currentTable);
		currentTable = newTempTable;
	    }
	    stmt.execute("CREATE TABLE " + Parameters.INTERSECTION_REL_NAME + " AS SELECT DISTINCT * FROM " + currentTable);
	    stmt.execute("DROP TABLE " + currentTable);
	}
	
	stmt.close();
	
	return Parameters.INTERSECTION_REL_NAME;
	
    }
    
    private static String getTempTable() {
	return "TEMPTABLE" + (tempTableIndex++);
    }
    
    private static String getIntersectionQuery(String commonAtt, String rel1, String rel2) {
	String query = "SELECT "+ commonAtt +" FROM " + rel1 + " INNER JOIN " + rel2 + " USING (" + commonAtt + ")";
	return query;
    }
    
    
    

    /**
     * @param args
     * @throws ClassNotFoundException 
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     * @throws SQLException 
     */
    public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
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
	
	ArrayList<String> rels = new ArrayList<String>();
//	rels.add("Q0_RES"); //19
////	rels.add("Q1_RES"); //200
////	rels.add("Q2_RES");
////	rels.add("Q3_RES");
////	rels.add("Q4_RES"); //200
////	rels.add("Q5_RES");
////	rels.add("Q6_RES"); //200
//	rels.add("Q7_RES"); //24
//	rels.add("Q8_RES"); //19
//	rels.add("Q9_RES"); //19
	
	rels.add("R");
	rels.add("S");
	rels.add("T");
	XIntersection.createIntersecTable(rels, dbConn);
	
	System.out.println("Done.");
    }

}
