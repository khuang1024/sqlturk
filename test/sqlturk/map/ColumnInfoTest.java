package sqlturk.map;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import sqlturk.configuration.Parameters;
import sqlturk.util.cleaner.Cleaner;
import sqlturk.util.map.ColumnInfo;
import sqlturk.util.query.QueryExecutor;
import sqlturk.util.tableaux.TableauxRewriter;

public class ColumnInfoTest {

    private static Connection dbConn;
    private static ArrayList<String> originalQueries = new ArrayList<String>();
    private static ArrayList<String> rewriteQueries = new ArrayList<String>();

    @BeforeClass
    public static void initialize() throws SQLException {
	if (Parameters.USE_SERVER) {
	    dbConn = DriverManager
		    .getConnection(Parameters.MYSQL_CONNECTION_STRING);
	    System.out.println("Using server.");
	} else {
	    dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL,
		    Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
	    System.out.println("Using local.");
	}

	originalQueries
		.add("select LifeExpectancy, Continent from Country where LifeExpectancy > 81");
	originalQueries
		.add("select LifeExpectancy LiEx, Region as Country_Region from Country as Ctry where LifeExpectancy < 37.5");

	// clear old tables
	Cleaner.dropAll(dbConn);

	// rewrite queries
	rewriteQueries = TableauxRewriter.getRewriteQueries(originalQueries,
		dbConn);

	// execute queries
	QueryExecutor.executeQueries(rewriteQueries, dbConn);
    }

    @AfterClass
    public static void clear() throws SQLException {
	Cleaner.dropAll(dbConn);
    }

    @Test
    public void testGetOriginalColumnName() throws SQLException {
	assertEquals("Column", "LifeExpectancy",
		ColumnInfo.getOriginalColumnName("Q1_RES", "LifeExpectancy",
			dbConn));
	assertEquals("Column", "LifeExpectancy",
		ColumnInfo.getOriginalColumnName("Q2_RES", "LiEx", dbConn));
	assertEquals("Column", "Region", ColumnInfo.getOriginalColumnName(
		"Q2_RES", "Country_Region", dbConn));
    }

    @Test
    public void testGetOriginalTableName() throws SQLException {
	assertEquals("Table", "Country", ColumnInfo.getOriginalTableName(
		"Q1_RES", "LifeExpectancy", dbConn));
	assertEquals("Table", "Country",
		ColumnInfo.getOriginalTableName("Q2_RES", "LiEx", dbConn));
	assertEquals("Table", "Country", ColumnInfo.getOriginalTableName(
		"Q2_RES", "Country_Region", dbConn));
    }

}
