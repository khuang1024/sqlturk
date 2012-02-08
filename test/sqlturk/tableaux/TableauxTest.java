package sqlturk.tableaux;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
//import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;

import sqlturk.util.tableaux.TableauxRewriter;
import sqlturk.configuration.Parameters;

public class TableauxTest {

    private static Connection dbConn;

    @BeforeClass
    public static void init() throws SQLException {
	if (Parameters.USE_SERVER) {
	    dbConn = DriverManager
		    .getConnection(Parameters.MYSQL_CONNECTION_STRING);
	    System.out.println("Using server.");
	} else {
	    dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL,
		    Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
	    System.out.println("Using local.");
	}
    }

    @Test
    public void testGetRewriteQuery() throws SQLException {
//	String testQuery1 = "select City.Name, District from City where CountryCode='USA'";
//	String testQuery2 = "select count(Name) as countName, District from City as ct where CountryCode='USA' group by District having avg(ID)>0 and count(*)<3";
//	String testQuery3 = "select count(Name), District district from City where CountryCode = 'USA' group by (District)";
//	String testQuery4 = "select LifeExpectancy, Continent from Country where LifeExpectancy > 81";
//	String testQuery5 = "select orderNumber, sum(quantityOrdered * priceEach) from OrderDetails group by orderNumber";
//	String testQuery6 = "select customerName, productName from Customers, Products, Orders, OrderDetails where Products.productCode=OrderDetails.productCode and OrderDetails.orderNumber=Orders.orderNumber and Orders.customerNumber=Customers.customerNumber group by (customerName) having count(*)>50";
//	String testQuery7 = "select ci.Name from Country c, City ci where c.GovernmentForm='Republic' and c.LifeExpectancy < 60 and c.Code = ci.CountryCode group by c.Name";
//
//	TableauxRewriter.getRewriteQuery(testQuery1, dbConn);
//	TableauxRewriter.getRewriteQuery(testQuery2, dbConn);
//	TableauxRewriter.getRewriteQuery(testQuery3, dbConn);
//	TableauxRewriter.getRewriteQuery(testQuery4, dbConn);
//	TableauxRewriter.getRewriteQuery(testQuery5, dbConn);
//	TableauxRewriter.getRewriteQuery(testQuery6, dbConn);
//	TableauxRewriter.getRewriteQuery(testQuery7, dbConn);
	
	String testQuery8 = "select CountryCode from CountryLanguage where Language like 'French'";
	String testQuery9 = "select Name from City where CountryCode in(select Language from CountryLanguage)";
	String testQuery10 = "SELECT Country.Name,CountryLanguage.Language FROM Country,CountryLanguage WHERE Country.Code = CountryLanguage.CountryCode ORDER BY Country.Name";
	String testQuery11 = "select Country.Name from Country,CountryLanguage  where (Country.Code = CountryLanguage.CountryCode) and (CountryLanguage.Language = 'Swedish') and (CountryLanguage.IsOfficial = 'T')";
	String testQuery12 = "select ctry.Name from Country as ctry,CountryLanguage as lan where lan.Language like 'Swedish' and lan.IsOfficial = 'Y' and lan.CountryCode = ctry.Code";
	String testQuery13 = "select District from City where Population>3000000 and CountryCode in(select CountryCode from CountryLanguage where Language like 'Chinese')";
	String testQuery14 = "select Name,Capital from Country where code in(select CountryCode from CountryLanguage where language like 'English' and Percentage<=50)";
	String testQuery15 = "select C.Name, Cy.Name from Country C, City Cy WHERE C.Capital = Cy.ID  AND EXISTS (SELECT * FROM CountryLanguage WHERE CountryCode = C.Code AND Language='English' AND Percentage>='50.0')";
	String testQuery17 = "select Name,Population from City where Population<200000 and CountryCode in(select CountryCode from CountryLanguage where Language like 'Spanish')";
	String testQuery18 = "SELECT Cy.Name, Cy.Population FROM City Cy  WHERE Cy.CountryCode IN (Select C.Code FROM Country C, CountryLanguage CL WHERE C.Code = CL.CountryCode AND CL.IsOfficial='T' AND CL.Language='Spanish' AND C.Continent='Europe') AND Cy.Population<200000";
	String testQuery19 = "select Name, Population from City where CountryCode in (select CountryCode from CountryLanguage where Language = 'Spanish') and Population <= 200000";
	String testQuery21 = "select O_CUSTKEY,O_ORDERSTATUS from ORDERS where O_TOTALPRICE<530000 in(select L_DISCOUNT from LINEITEM where L_DISCOUNT=0)";
	String testQuery22 = "SELECT S_NAME, S_PHONE, S_ACCTBAL FROM SUPPLIER, NATION  WHERE N_NATIONKEY = S_NATIONKEY AND N_NATIONKEY = 'CHINA' AND S_ACCTBAL > 9000";
	String testQuery23 = "select S.S_NAME,S.S_PHONE from SUPPLIER S  where S.S_ACCTBAL>9000 and S.S_NATIONKEY in (select N.N_NATIONKEY from NATION N,REGION R where R.R_NAME='CHINA' and N.N_REGIONKEY=R.R_REGIONKEY)";
	String testQuery24 = "select S_NAME,S_PHONE from SUPPLIER where S_ACCTBAL>9000 in(select N_NATIONKEY from NATION where N_NAME like 'CHINA')";
	String testQuery25 = "SELECT C_NAME, C_ACCTBAL FROM CUSTOMER C, NATION N, REGION R  WHERE C.C_ACCTBAL>9990  AND C.c_NATIONKEY=N.N_NATIONKEY  AND N.N_REGIONKEY=R.R_REGIONKEY  AND lower(R_NAME)='europe'";
	String testQuery26 = "select C_NAME, C_ACCTBAL   from CUSTOMER  where C_NATIONKEY in  (select N_NATIONKEY from NATION where N_NAME = 'EUROPE')";
	String testQuery27 = "";
		
	TableauxRewriter.getRewriteQuery(testQuery26, dbConn);
    }

}
