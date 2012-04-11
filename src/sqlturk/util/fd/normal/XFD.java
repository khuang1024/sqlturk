package sqlturk.util.fd.normal;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;
import sqlturk.util.cleaner.Cleaner;
import sqlturk.util.common.XCommon;
import sqlturk.util.common.XTable;
import sqlturk.util.common.XTuple;
import sqlturk.util.common.XTupleSet;

public class XFD {
    
    private static int fdiIndex = 0;

    private XFD() {
	throw new AssertionError();
    }
    
    public static String createFDTable(Connection dbConn) throws SQLException {
	ArrayList<String> resultTables = XCommon.getAllResultTables(dbConn);
	return createFDTable(resultTables, dbConn);
    }
    
    public static String createFDTable(ArrayList<String> rels, Connection dbConn) throws SQLException {
	Statement stmt = dbConn.createStatement();
	
	ArrayList<String> FDIs = new ArrayList<String>();
	ArrayList<String> commonSchema = XCommon.getAllColumns(rels, dbConn);
	for (String rel : rels) {
	    FDIs.add(createFDI(rel, rels, commonSchema, dbConn)); // get all FDi
	}
	
	// do the union operation
	String union = "";
	for (String fdi : FDIs) {
	    union += "SELECT * FROM " + fdi + " UNION ";
	}
	union = union.substring(0, union.length() - "UNION ".length());
	
	// drop the old FD table
	stmt.execute("DROP TABLE IF EXISTS " + Parameters.FD_REL_NAME);
	
	// create the FD
	String create = "CREATE TABLE " + Parameters.FD_REL_NAME + " AS " + union;
	stmt.execute(create);
	
	
	// drop all FDi tables
	for (String fdi : FDIs) {
	    stmt.execute("DROP TABLE " + fdi);
	}
	
	stmt.close();
	
	return Parameters.FD_REL_NAME;
    }
    
    static String createFDI(String rel, ArrayList<String> rels, ArrayList<String> commonSchema, Connection dbConn) throws SQLException {
	Statement stmt = dbConn.createStatement();
	
	String fdiName = "TMPFD" + (fdiIndex++);
	
	ArrayList<XTupleSet> tupleSets = getCompleteTupleSets(rel, rels, dbConn);
	
	ArrayList<String> allRels = new ArrayList<String>();
	allRels.add(rel);
	allRels.addAll(rels);
	
	
	String commonSchemaString = "";
	for (String col : commonSchema) {
	    commonSchemaString += col + " VARCHAR(50), ";
	}
	commonSchemaString = commonSchemaString.substring(0, commonSchemaString.length() - 2);
	
	// drop the existing FDi (old FDi)
	stmt.execute("DROP TABLE IF EXISTS " + fdiName);
	
	String create = "CREATE TABLE " + fdiName + " (" + commonSchemaString + ")";
	stmt.execute(create);
	System.out.println("\n" + create);
	
	for (XTupleSet ts : tupleSets) {
	    XTuple newTuple = ts.toTuple(fdiName, commonSchema);
	    ArrayList<String> values = newTuple.getValues();
	    String valueString = "";
	    for (String value : values) {
		valueString += "'" + value + "', ";
	    }
	    valueString = valueString.substring(0, valueString.length() - 2);
	    String insert = "INSERT INTO " + fdiName + " VALUES (" + valueString + ")";
	    stmt.execute(insert);
	    System.out.println(insert);
	}
	System.out.println();
	
	stmt.close();
	
	return fdiName;
    }
    
    
    /**
     * Return the tuple sets of the given target relation for creating its 
     * FDi. Note: this function only returns the tuple sets (complete).
     * @param rel the target relation.
     * @param rels all the relations in this database
     * @param dbConn
     * @return
     * @throws SQLException
     */
    static ArrayList<XTupleSet> getCompleteTupleSets(String rel, ArrayList<String> rels, Connection dbConn) throws SQLException {
	
	// evaluate tables
	XTable targetTable = new XTable(rel, dbConn);
	ArrayList<XTable> otherTables = new ArrayList<XTable>();
	for (int i = 0; i < rels.size(); i++) {
	    if (!rel.equals(rels.get(i))) { // exclude the target table
		otherTables.add(new XTable(rels.get(i), dbConn));
	    }
	}
	
	ArrayList<XTupleSet> complete = new ArrayList<XTupleSet>();
	ArrayList<XTupleSet> inComplete = new ArrayList<XTupleSet>();
	
	for (int i = 0; i < targetTable.size(); i++) {
	    inComplete.add(new XTupleSet(targetTable.getTuple(i))); // line 3-4
	}
	
	while (!inComplete.isEmpty()) {
	    XTupleSet tupleSet = pop(inComplete);
	    
	    // check all other tuples which are JCC with is tuple set. Line 7-8
	    for (XTable table : otherTables) {
		for (int i = 0; i < table.size(); i++) {
		    XTuple tuple = table.getTuple(i);
		    if (tupleSet.jcc(tuple) && (!tupleSet.contains(tuple))) {
			tupleSet.add(tuple);
			// here, we should break, because there can not be
			// two tuples in a single table which are JCC with
			// the same tuple set.
			break;
		    }
		}
	    }
	    
	    // Line 9 - 20
	    for (XTable table : otherTables) { // Line 9
		for (int i = 0; i < table.size(); i++) {
		    XTuple tuple = table.getTuple(i);
		    if (!tupleSet.contains(tuple)) { // Line 9
			XTupleSet prime = getPrime(tuple, tupleSet); // Line 11
			
			if (prime.hasTupleFrom(targetTable)) { // Line 12
			    boolean inserted = false; // Line 13
			    if (isInComplete(prime, complete)) {
				inserted = true; // Line 15
				for (int j = 0 ; j < inComplete.size(); i++) { // Line 16
				    XTupleSet ts = inComplete.get(j); // Line 16
				    if (prime.jcc(ts)) { // Line 17
					ts.addAll(prime);
					inserted = true;
				    }
				}
				if (!inserted) {
				    inComplete.add(prime);
				}
			    }
			}
		    }
		}
	    }
	    
	    // Line 22
	    complete.add(tupleSet);
	}
	
	return complete;
    }
    
    private static boolean isInComplete(XTupleSet prime, ArrayList<XTupleSet> complete) {
	for (XTupleSet ts : complete) {
	    if (prime.subsetOf(ts)) {
		return true;
	    }
	}
	return false;
    }
    
    private static XTupleSet getPrime(XTuple tuple, XTupleSet tupleSet) {
	XTupleSet prime = new XTupleSet(tuple);
	for (XTuple t : tupleSet.getTuples()) {
	    if (prime.jcc(t)) {
		prime.add(t);
	    }
	}
	return prime;
    }
    
    private static XTupleSet pop(ArrayList<XTupleSet> inComplete) {
	XTupleSet ts = inComplete.get(0);
	inComplete.remove(0);
	return ts;
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
	if (Parameters.USE_SERVER) {
	    dbConn = DriverManager.getConnection(Parameters.MYSQL_CONNECTION_STRING);
	    System.out.println("Using server.\n");
	} else {
	    dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL, Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
	    System.out.println("Using local.\n");
	}
	
	Cleaner.dropFDTable(dbConn);
	Cleaner.dropFDPlusTable(dbConn);
	
	ArrayList<String> rels = new ArrayList<String>();
	rels.add("Q0_RES");
	rels.add("Q1_RES");
	rels.add("Q2_RES");
	rels.add("Q3_RES");
	rels.add("TEST");
	
	System.out.println(createFDTable(rels, dbConn));

    }

}
