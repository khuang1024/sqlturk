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
	String testQuery1 = "select City.Name, District from City where CountryCode='USA'";
	String testQuery2 = "select count(Name) as countName, District from City as ct where CountryCode='USA' group by District having avg(ID)>0 and count(*)<3";
	String testQuery3 = "select count(Name), District district from City where CountryCode = 'USA' group by (District)";
	String testQuery4 = "select LifeExpectancy, Continent from Country where LifeExpectancy > 81";
	String testQuery5 = "select orderNumber, sum(quantityOrdered * priceEach) from OrderDetails group by orderNumber";
	String testQuery6 = "select customerName, productName from Customers, Products, Orders, OrderDetails where Products.productCode=OrderDetails.productCode and OrderDetails.orderNumber=Orders.orderNumber and Orders.customerNumber=Customers.customerNumber group by (customerName) having count(*)>50";
	String testQuery7 = "select ci.Name from Country c, City ci where c.GovernmentForm='Republic' and c.LifeExpectancy < 60 and c.Code = ci.CountryCode group by c.Name";

	TableauxRewriter.getRewriteQuery(testQuery1, dbConn);
	TableauxRewriter.getRewriteQuery(testQuery2, dbConn);
	TableauxRewriter.getRewriteQuery(testQuery3, dbConn);
	TableauxRewriter.getRewriteQuery(testQuery4, dbConn);
	TableauxRewriter.getRewriteQuery(testQuery5, dbConn);
	TableauxRewriter.getRewriteQuery(testQuery6, dbConn);
	TableauxRewriter.getRewriteQuery(testQuery7, dbConn);
    }

}
