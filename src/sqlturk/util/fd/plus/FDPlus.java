package sqlturk.util.fd.plus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;

public class FDPlus {

    private FDPlus() {
	throw new AssertionError();
    }

    /**
     * Create the Full Disjunction (why-connected) table for the current
     * database and rename it as the given name.
     * 
     * @param dbConn
     * @param FDRelationName
     * @throws SQLException
     */
    public static void createFDPlusRelation(Connection dbConn,
	    String FDRelationName) throws SQLException {
	createFDPlusRelation(dbConn);
	Statement stmt = dbConn.createStatement();
	stmt.executeUpdate("ALTER TABLE " + Parameters.FD_REL_NAME
		+ " RENAME TO " + FDRelationName);
	stmt.close();
    }

    /**
     * Create the Full Disjunction Plus (why-connected) table for the current
     * database.
     * 
     * @param dbConn
     * @throws SQLException
     */
    public static void createFDPlusRelation(Connection dbConn)
	    throws SQLException {
	ArrayList<Relation> allResultRelations = getAllResultRelations(dbConn);
	IncrementalFDPlus.createFDPlusRelation(allResultRelations, dbConn);
	allResultRelations = null;
    }

    // The returned relations are with tuples, and the schemas of these tuples
    // do not have ROWID.
    private static ArrayList<Relation> getAllResultRelations(Connection dbConn)
	    throws SQLException {
	ArrayList<Relation> allResultRelations = new ArrayList<Relation>();
	ArrayList<String> allResultRelationNames = getAllResultRelationNames(dbConn);
	for (String resultRelationName : allResultRelationNames) {
	    // Please note, the schema of the tuples of this relation has no
	    // ROWID column. The getTuples function has already filtered out
	    // ROWID.
	    allResultRelations.add(new Relation(resultRelationName, getTuples(
		    resultRelationName, dbConn)));
	}
	return allResultRelations;
    }

    // Retrieve all the tuples of a given relation. Please note, the schema of
    // the tuples of this relation has no ROWID column.
    private static ArrayList<Tuple> getTuples(String relationName,
	    Connection dbConn) throws SQLException {
	ArrayList<Tuple> allTuples = new ArrayList<Tuple>();
	ArrayList<String> schema = getSchema(relationName, dbConn);

	Statement stmt = dbConn.createStatement();
	ResultSet rs = stmt.executeQuery("SELECT * FROM " + relationName);
	ResultSetMetaData rsmd = rs.getMetaData();
	if (rsmd.getColumnCount() != schema.size()) {
	    throw new RuntimeException(
		    "Lengths of schema and values do not match.");
	}
	while (rs.next()) {
	    ArrayList<String> values = new ArrayList<String>();
	    for (int i = 0; i < schema.size(); i++) {
		if (!schema.get(i).equals(Parameters.ROWID_ATT_NAME)) {
		    values.add(rs.getString(schema.get(i)));
		} else {
		    schema.remove(i);
		    i--;
		}
	    }
	    allTuples.add(new Tuple(relationName, schema, values));
	}
	return allTuples;
    }

    // get the raw schema of the relation, which probably includes ROWID
    private static ArrayList<String> getSchema(String relationName,
	    Connection dbConn) throws SQLException {
	ArrayList<String> schema = new ArrayList<String>();
	Statement stmt = dbConn.createStatement();
	ResultSet rs = stmt.executeQuery("DESC " + relationName);
	while (rs.next()) {
	    schema.add(rs.getString(1));
	}
	return schema;
    }

    // get the names of all resulting tables
    private static ArrayList<String> getAllResultRelationNames(Connection dbConn)
	    throws SQLException {
	ArrayList<String> allResultRelations = new ArrayList<String>();
	Statement stmt = dbConn.createStatement();

	// put the result tables into the array
	ResultSet rs = stmt.executeQuery("SHOW TABLES");
	while (rs.next()) {
	    if (rs.getString(1).startsWith(Parameters.QUERY_RESULT_PREFIX)
		    && rs.getString(1).endsWith(Parameters.QUERY_RESULT_SUFFIX)) {
		allResultRelations.add(rs.getString(1));
		// System.out.println(rs.getString(1));
	    }
	}
	return allResultRelations;
    }

    /**
     * @param args
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws SQLException
     */
    public static void main(String[] args) throws InstantiationException,
	    IllegalAccessException, ClassNotFoundException, SQLException {
	Class.forName("com.mysql.jdbc.Driver").newInstance();
	Connection dbConn;
	/*
	 * use soe server or test locally
	 */
	if (Parameters.USE_SERVER) {
	    dbConn = DriverManager
		    .getConnection(Parameters.MYSQL_CONNECTION_STRING);
	    System.out.println("Using server.");
	} else {
	    dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL,
		    Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
	    System.out.println("Using local.");
	}

	createFDPlusRelation(dbConn);
    }

}
