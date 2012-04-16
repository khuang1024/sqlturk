package sqlturk.util.union;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;
import sqlturk.util.common.XCommon;

public class XUnion {
    
    private XUnion() {
	throw new AssertionError();
    }
    
    public static String createUnionTable(Connection dbConn) throws SQLException {
	ArrayList<String> resultTables = XCommon.getAllResultTables(dbConn);
	return createUnionTable(resultTables, dbConn);
    }
    
    private static String createUnionTable(ArrayList<String> rels, 
	    Connection dbConn) throws SQLException {
	Statement stmt = dbConn.createStatement();
	
	ArrayList<String> commonCols = XCommon.getCommonColumns(rels, dbConn);
	
	String commonAtt = "";
	for (String att : commonCols) {
	    commonAtt += att + ", ";
	}
	
	// first, drop table
	stmt.execute("DROP TABLE IF EXISTS " + Parameters.UNION_REL_NAME);
	
	if (commonAtt.equals("")) {
	    String create = "CREATE TABLE " + Parameters.UNION_REL_NAME + " (NO_COMMON VARCHAR(10))";
	    String instert = "INSERT INTO " + Parameters.UNION_REL_NAME + " VALUES (\"NO_COMMON\")";
	    stmt.execute(create);
	    stmt.execute(instert);
	} else {
	    commonAtt = commonAtt.substring(0, commonAtt.length()-2);
	    String create = "CREATE TABLE " + Parameters.UNION_REL_NAME + " AS " + getUnionQuery(commonAtt, rels);
	    stmt.execute(create);
	}
	
	stmt.close();
	
	return Parameters.UNION_REL_NAME;
    }
    
    private static String getUnionQuery(String commonAtt, ArrayList<String> rels) {
	String query = "";
	for (int i = 0; i < rels.size(); i++) {
	    query += "SELECT " + commonAtt + " FROM " + rels.get(i) + " UNION ";
	}
	query = query.substring(0, query.length() - 7);
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
	XUnion.createUnionTable(rels, dbConn);
	
	System.out.println("Done.");

    }

}
