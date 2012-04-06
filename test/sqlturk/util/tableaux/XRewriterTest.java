package sqlturk.util.tableaux;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

import sqlturk.configuration.Parameters;

public class XRewriterTest {
    
    private static Connection dbConn;
    
    @BeforeClass
    public static void init() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
	Class.forName("com.mysql.jdbc.Driver").newInstance();
	
	// use soe server or test locally
	if (Parameters.USE_SERVER) {
	    dbConn = DriverManager.getConnection(Parameters.MYSQL_CONNECTION_STRING);
	    System.out.println("Using server.\n");
	} else {
	    dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL, Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
	    System.out.println("Using local.\n");
	}
    }

    @Test
    public void testGetRewrittenQuery() throws SQLException {
	String q1 = "select Name from Country where Region = 'Western Europe'";
	String a1 = "SELECT DISTINCT Country.Name AS Country_Name FROM Country WHERE Region = 'Western Europe'";
	assertEquals(a1, XRewriter.getRewrittenQuery(q1, dbConn));
	assertEquals(a1, a1);

	String q2 = "select CountryCode from CountryLanguage where Language = 'French'";
	String a2 = "SELECT DISTINCT CountryLanguage.CountryCode AS CountryLanguage_CountryCode FROM CountryLanguage, Country WHERE Language = 'French' AND CountryLanguage.CountryCode=Country.Code";
	assertEquals(a2, XRewriter.getRewrittenQuery(q2, dbConn));

	String q3 = "select C.Name from City C,Country CU,CountryLanguage CL where CU.Population>3000000 and CL.Language='Chinese'";
	String a3 = "SELECT DISTINCT City.Name AS City_Name FROM Country, City, CountryLanguage, Country AS TABLEAUX0 WHERE Country.Population>3000000 and CountryLanguage.Language='Chinese' AND City.CountryCode=Country.Code AND CountryLanguage.CountryCode=TABLEAUX0.Code";
	assertEquals(a3, XRewriter.getRewrittenQuery(q3, dbConn));

	String q4 = "select O_CLERK from ORDERS O where O_TOTALPRICE >= 500000";
	String a4 = "SELECT DISTINCT ORDERS.O_CLERK AS ORDERS_O_CLERK FROM ORDERS, CUSTOMER, NATION, REGION WHERE O_TOTALPRICE >= 500000 AND ORDERS.O_CUSTKEY=CUSTOMER.C_CUSTKEY AND CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY AND NATION.N_REGIONKEY=REGION.R_REGIONKEY";
	assertEquals(a4, XRewriter.getRewrittenQuery(q4, dbConn));

	String q5 = "SELECT DISTINCT O_ORDERKEY, O_ORDERSTATUS  FROM ORDERS, LINEITEM  WHERE L_ORDERKEY=O_ORDERKEY  AND L_DISCOUNT=0  AND O_TOTALPRICE>530000";
	String a5 = "SELECT DISTINCT ORDERS.O_ORDERSTATUS AS ORDERS_O_ORDERSTATUS, ORDERS.O_ORDERKEY AS ORDERS_O_ORDERKEY FROM ORDERS, LINEITEM, CUSTOMER, PARTSUPP, PARTSUPP AS TABLEAUX0, NATION, SUPPLIER, PART, REGION, NATION AS TABLEAUX1 WHERE L_ORDERKEY=O_ORDERKEY AND L_DISCOUNT=0 AND O_TOTALPRICE>530000 AND ORDERS.O_CUSTKEY=CUSTOMER.C_CUSTKEY AND LINEITEM.L_ORDERKEY=ORDERS.O_ORDERKEY AND LINEITEM.L_SUPPKEY=TABLEAUX0.PS_SUPPKEY AND LINEITEM.L_PARTKEY=TABLEAUX0.PS_PARTKEY AND CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY AND PARTSUPP.PS_SUPPKEY=SUPPLIER.S_SUPPKEY AND PARTSUPP.PS_PARTKEY=PART.P_PARTKEY AND NATION.N_REGIONKEY=REGION.R_REGIONKEY AND SUPPLIER.S_NATIONKEY=TABLEAUX1.N_NATIONKEY";
	assertEquals(a5, XRewriter.getRewrittenQuery(q5, dbConn));

	String q6 = "SELECT S_NAME, S_PHONE  FROM SUPPLIER,  PARTSUPP,  PART WHERE S_ACCTBAL > 1000 AND S_SUPPKEY = PS_SUPPKEY AND PS_PARTKEY = P_PARTKEY  AND P_RETAILPRICE > 2096";
	String a6 = "SELECT DISTINCT SUPPLIER.S_NAME AS SUPPLIER_S_NAME, SUPPLIER.S_PHONE AS SUPPLIER_S_PHONE FROM SUPPLIER, PARTSUPP, PART, NATION, REGION WHERE S_ACCTBAL > 1000 AND S_SUPPKEY = PS_SUPPKEY AND PS_PARTKEY = P_PARTKEY AND P_RETAILPRICE > 2096 AND SUPPLIER.S_NATIONKEY=NATION.N_NATIONKEY AND PARTSUPP.PS_SUPPKEY=SUPPLIER.S_SUPPKEY AND PARTSUPP.PS_PARTKEY=PART.P_PARTKEY AND NATION.N_REGIONKEY=REGION.R_REGIONKEY";
	assertEquals(a6, XRewriter.getRewrittenQuery(q6, dbConn));
    }

}
