package sqlturk.experiment.mturk;

import java.util.ArrayList;

public class SQLTurkHITs {
    private SQLTurkHITs() {
	throw new AssertionError();
    }
    
    private static ArrayList<String> world00 = new ArrayList<String> ();
    private static ArrayList<String> world01 = new ArrayList<String> ();
    private static ArrayList<String> world02 = new ArrayList<String> ();
    private static ArrayList<String> world03 = new ArrayList<String> ();
    private static ArrayList<String> world04 = new ArrayList<String> ();
    private static ArrayList<String> world05 = new ArrayList<String> ();
    private static ArrayList<String> world06 = new ArrayList<String> ();
    private static ArrayList<String> world07 = new ArrayList<String> ();
    private static ArrayList<String> tpch00 = new ArrayList<String> ();
    private static ArrayList<String> tpch01 = new ArrayList<String> ();
    private static ArrayList<String> tpch02 = new ArrayList<String> ();
    private static ArrayList<String> tpch03 = new ArrayList<String> ();
    private static ArrayList<String> tpch04 = new ArrayList<String> ();
    private static ArrayList<String> tpch05 = new ArrayList<String> ();
    private static ArrayList<String> tpch06 = new ArrayList<String> ();
    private static ArrayList<String> tpch07 = new ArrayList<String> ();
    
    public static ArrayList<String> get(String dataset, int index) {
	if (dataset.equals("world")) {
	    switch (index) {
	    case 0:
		return copy(world00);
	    case 1:
		return copy(world01);
	    case 2:
		return copy(world02);
	    case 3:
		return copy(world03);
	    case 4:
		return copy(world04);
	    case 5:
		return copy(world05);
	    case 6:
		return copy(world06);
	    case 7:
		return copy(world07);
	    default:
		return null;
	    }
	} else if (dataset.equals("tpch")) {
	    switch (index) {
	    case 0:
		return copy(tpch00);
	    case 1:
		return copy(tpch01);
	    case 2:
		return copy(tpch02);
	    case 3:
		return copy(tpch03);
	    case 4:
		return copy(tpch04);
	    case 5:
		return copy(tpch05);
	    case 6:
		return copy(tpch06);
	    case 7:
		return copy(tpch07);
	    default:
		return null;
	    }
	} else {
	    throw new RuntimeException("Invalid dataset name.");
	}
    }
    
    private static ArrayList<String> copy(ArrayList<String> origin) {
	ArrayList<String> copies = new ArrayList<String> ();
	for (int i = 0; i < origin.size(); i++) {
	    copies.add(origin.get(i));
	}
	return copies;
    }
    
    static {
	world00.add("select Name from Country where Region like 'Western Europe'");
	world00.add("select Name from Country where Region = 'Western Europe'");
	world00.add("select Country.Name from Country where Country.Region='Western Europe'");
	world00.add("select Region from Country where Region='Europe'");
	world00.add("select Name from Country where Region='Europe'");
	world00.add("SELECT * FROM Country WHERE Region='Western Europe'");
	world00.add("SELECT Name from Country WHERE Region='Western Europe'");
	world00.add("select Country.Name  From Country  Where Region = 'Western Europe'");
	world00.add("select Name from Country where Region like 'Western Europe'");
	world00.add("select Name from Country where Region = 'Western Europe'");
	
	world01.add("select CountryCode from CountryLanguage where Language like 'French'");
	world01.add("select CountryCode from CountryLanguage where Language = 'French'");
	world01.add("Select CountryLanguage.CountryCode from CountryLanguage where CountryLanguage.Language ='French'");
	world01.add("select CountryCode from CountryLanguage where Language = 'French'");
	world01.add("select CountryCode from CountryLanguage where Language = 'French'");
	world01.add("SELECT * FROM CountryLanguage WHERE Language = 'French'");
	world01.add("SELECT CountryCode FROM CountryLanguage WHERE Language = 'French'");
	world01.add("select CountryLanguage.CountryCode  From CountryLanguage where Language = 'French'");
	world01.add("select CountryCode from CountryLanguage where Language like 'French'");
	world01.add("select CountryCode from CountryLanguage where Language='French'");
	
	world02.add("select a.Name as aName,b.Name as bName from Country a, City b where a.Capital = b.ID");
	world02.add("select Country.Name as CountryName,City.Name as CityName from Country,City where Country.Capital = City.ID");
	world02.add("select Country.Name,Country.Capital from Country");
	world02.add("select Name,Capital from Country");
	world02.add("select Name, Capital from Country");
	world02.add("SELECT Country.Name, Country.Capital FROM City, Country WHERE City.CountryCode = Country.Code");
	world02.add("select ctry.Name as CtryName, cty.Name as CtyName from Country as ctry, City as cty where ctry.Capital = cty.ID");
	world02.add("SELECT Country.Name as Name1, City.Name as Name2  FROM Country, City  WHERE Country.Capital= City.ID");
	world02.add("select Name,Capital from Country");
	world02.add("select c2.Name Country, c1.Name, Capital from City c1, Country c2  where c1.ID=c2.Capital");
	
	world03.add("select a.Name,b.Language from Country a, CountryLanguage b where a.Code = b.CountryCode");
	world03.add("Select Country.Name,CountryLanguage.Language from Country,CountryLanguage where Country.Code = CountryLanguage.CountryCode");
	world03.add("select Country.Name,CountryLanguage.Language from Country,CountryLanguage where Country.Code = CountryLanguage.CountryCode");
	world03.add("select C.Name,CL.Language from Country C,CountryLanguage CL where C.Code=CL.CountryCode");
	world03.add("select c.Name, cl.Language from Country c, CountryLanguage cl  where c.Code=cl.CountryCode");
	world03.add("SELECT Country.Name,CountryLanguage.Language FROM Country,CountryLanguage WHERE Country.Code = CountryLanguage.CountryCode"); // correct
	world03.add("select ctry.Name,lan.Language from Country as ctry, CountryLanguage as lan where lan.CountryCode = ctry.Code");
	world03.add("SELECT Country.Name, CountryLanguage.Language   FROM Country, CountryLanguage  WHERE Country.Code = CountryLanguage.CountryCode");
	world03.add("Select c.Name,cl.Language from Country c,CountryLanguage cl where c.Code=cl.CountryCode");
	world03.add("select c2.Name Country, c3.Language Language  from  Country c2, CountryLanguage c3  where c3.CountryCode= c2.Code");
	
	world04.add("select c.Name from Country c,CountryLanguage l where c.Code = l.CountryCode and l.Language='Swedish' and l.IsOfficial='T'");
	world04.add("select a.Name from Country a, CountryLanguage c where c.CountryCode = a.Code and c.Language = 'Swedish' and c.IsOfficial = 'T'");
	world04.add("Select a.Name,b.IsOfficial from Country as a,CountryLanguage as b where a.Name='Swedish'");
	world04.add("select Country.Name from Country,CountryLanguage  where (Country.Code = CountryLanguage.CountryCode) and (CountryLanguage.Language = 'Swedish') and (CountryLanguage.IsOfficial = 'T')"); // correct
	world04.add("select C.Name from Country C,CountryLanguage CL where CL.Language='Swedish' and CL.IsOfficial='T'");
	world04.add("SELECT Name from Country C, CountryLanguage CL WHERE C.Code = CL.CountryCode  AND CL.IsOfficial is true  and CL.Language='Swedish'");
	world04.add("select ctry.Name from Country as ctry,CountryLanguage as lan where lan.Language like 'Swedish' and lan.IsOfficial = 'Y' and lan.CountryCode = ctry.Code"); 
	world04.add("select c2.Name Name  from Country c2, CountryLanguage c3  where c3.CountryCode= c2.Code and  c3.Language='Swedish'");
	world04.add("Select Country.Name from Country, CountryLanguage where CountryLanguage.Language = 'Swedish' and Country.Code = CountryLanguage.CountryCode");
	world04.add("SELECT Country.Name FROM Country, CountryLanguage WHERE CountryLanguage.CountryCode = Country.Code AND CountryLanguage.Language = 'Swedish' AND CountryLanguage.IsOfficial = 'T'");
	
	world05.add("select a.Name from City a,Country b,CountryLanguage c where a.CountryCode=c.CountryCode and a.CountryCode=b.Code and c.Language='Chinesse' and b.Population > 3000000");
	world05.add("select b.Name from City b, CountryLanguage c where c.CountryCode = b.CountryCode and c.Language = 'Chinese' and b.Population > 3000000");
	world05.add("select Name from City where Population>=3000000");
	world05.add("select City.Name from City, CountryLanguage where (City.CountryCode = CountryLanguage.CountryCode) and (CountryLanguage.Language = 'Chinese') and (City.Population > 3000000)");
	world05.add("select C.Name from City C,Country CU,CountryLanguage CL where CU.Population>3000000 and CL.Language='Chinese'");
	world05.add("SELECT Cy.Name from City Cy, Country C, CountryLanguage CL where Cy.CountryCode = C.Code and C.Code=CL.CountryCode  and CL.Language='Chinese'  and Cy.Population>3000000");
	world05.add("select cty.Name from City as cty,CountryLanguage as lan where lan.Language like 'Chinese' and cty.Population > 3000000");
	world05.add("select c1.Name Name  from City c1, Country c2, CountryLanguage c3  where c1.CountryCode=c2.Code and  c3.CountryCode= c2.Code and  c3.Language='Chinese' and   c1.Population>3000000");
	world05.add("Select City.Name  From City, CountryLanguage, Country  Where CountryLanguage.Language = 'Chinese'  and City.Population > 3000000 and Country.Code = CountryLanguage.CountryCode  and City.CountryCode = Country.Code");
	world05.add("SELECT City.Name FROM City, Country, CountryLanguage WHERE CountryLanguage.CountryCode = Country.Code AND Country.Code = City.CountryCode AND CountryLanguage.Language = 'Chinese' AND City.Population > 3000000");
	
	world06.add("select a.Name,a.Capital from Country a,CountryLanguage b where a.Code=b.CountryCode and b.Language='English' and b.Percentage >= 50");
	world06.add("select a.Name as Ctry,b.Name as Cty from Country a, City b, CountryLanguage c where a.Capital = b.ID and c.CountryCode = a.Code and c.Language = 'English' and c.Percentage >= 50");
	world06.add("select Name, Capital from Country, CountryLanguage where (Country.Code = CountryLanguage.CountryCode) and (CountryLanguage.Language = 'English' and CountryLanguage.Percentage >= 50)");
	world06.add("Select C.Name,C.Capital from Country C,CountryLanguage CL where CL.Language='English' and CL.Percentage>'50.0'");
	world06.add("Select Country.Name, Country.Capital  From Country , CountryLanguage   Where CountryLanguage.Language = 'English' and CountryLanguage.Percentage >= 50  and CountryLanguage.CountryCode = Country.Code");
	world06.add("select Country.Name,Country.Capital from Country,CountryLanguage where CountryLanguage.Language='English' AND Country.Population>=(Country.Population*0.5) AND Country.code=CountryLanguage.CountryCode");
	world06.add("select c2.Name Country, c1.Name Capital from City c1, Country c2, CountryLanguage c3  where c1.ID=c2.Capital and c3.CountryCode= c2.Code and c3.Language='English' and c3.Percentage >=50");
	world06.add("SELECT Name, Capital FROM Country, CountryLanguage WHERE Country.Code = CountryLanguage.CountryCode    AND CountryLanguage.Percentage >= 50 AND Language = 'ENGLISH'");
	world06.add("SELECT C.Name,C.LocalName FROM City CC,Country C,CountryLanguage CCC WHERE CC.CountryCode=C.Code AND C.Code=CCC.CountryCode AND CCC.Language='English' AND C.Population > 50");
	world06.add("SELECT Name ,Capital FROM Country , CountryLanguage where Language = 'English' and Percentage >= 50");
	
	world07.add("select a.Name,a.Population from City a,Country b,CountryLanguage c where a.CountryCode=b.Code and a.CountryCode=c.CountryCode and b.Continent='Europe' and c.Language='Spanish' and c.IsOfficial='T' and a.Population< 200000");
	world07.add("select b.Name,b.Population from Country a ,City b, CountryLanguage c where a.Capital = b.ID and a.Continent = 'Europe' and a.Code = c.CountryCode and c.Language = 'Spanish' and c.IsOfficial = 'T'");
	world07.add("select City.Name,City.Population from City, Country,CountryLanguage where (Country.Code = City.CountryCode) and (Country.Code = CountryLanguage.CountryCode) and  (CountryLanguage.Language = 'Spanish') and (Country.Continent = 'Europe') and (City.Population < 200000)");
	world07.add("select C.Name,C.Population from City C,Country CU,CountryLanguage CL where CL.Language='Spanish' and CU.Region='Europe' and C.Population<200000");
	world07.add("Select City.Name, City.Population  From City, Country, CountryLanguage   Where Country.Continent = 'Europe' and CountryLanguage.Language = 'Spanish' and Country.Population < 200000  and CountryLanguage.CountryCode = Country.Code and City.CountryCode = Country.Code");
	world07.add("SELECT City.Name, City.Population from City, CountryLanguage, Country where Country.Continent='Europe'AND Country.Population<200000 AND Country.Code=City.CountryCode AND City.CountryCode=CountryLanguage.CountryCode AND CountryLanguage.Language='Spanish'");
	world07.add("select c1.Name Name, c1.Population Population  from City c1, Country c2, CountryLanguage c3  where c1.ID=c2.Capital and c3.CountryCode= c2.Code and c2.Continent='Europe' and c3.Language='Spanish' and c1.Population<200000");
	world07.add("SELECT City.Name, City.Population  FROM City,  CountryLanguage,  Country  WHERE Country.Code = CountryLanguage.CountryCode  AND Country.Continent= 'Europe'  AND City.CountryCode = Country.code  AND Language = 'Spanish' AND IsOfficial = 'T'");
	world07.add("SELECT C.Name,C.Population FROM City C,Country CC,CountryLanguage CL WHERE C.CountryCode=CC.Code AND CL.CountryCode=C.CountryCode AND CC.Continent='Europe' AND CL.Language='Spanish' AND CL.IsOfficial='T' AND  C.Population < 200000");
	world07.add("Select Name , Population FROM Country , CountryLanguage where Continent = 'Europe' and Language = 'Spanish' and IsOfficial = 'T' and Population  < 200000");
	
	
	tpch00.add("select O_CLERK from ORDERS O where O_TOTALPRICE >= 500000");
	tpch00.add("SELECT O_CLERK from ORDERS where O_TOTALPRICE > 500000");
	tpch00.add("SELECT O_CLERK FROM ORDERS WHERE O_TOTALPRICE > 500000");
	tpch00.add("SELECT O_CLERK FROM ORDERS WHERE O_TOTALPRICE > 500000");
	tpch00.add("Select O_CLERK from ORDERS where O_TOTALPRICE > 500000");
	tpch00.add("select O_CLERK from ORDERS where O_TOTALPRICE >= 500000");
	tpch00.add("SELECT DISTINCT O_CLERK FROM ORDERS WHERE O_TOTALPRICE > 500000");
	tpch00.add("select O_CLERK From ORDERS where O_TOTALPRICE > 500000");
	tpch00.add("SELECT O_CLERK FROM ORDERS WHERE O_TOTALPRICE > 500000");
	tpch00.add("SELECT O_CLERK FROM ORDERS WHERE O_TOTALPRICE > 500000");
	
	tpch01.add("select S_NAME from SUPPLIER S where S_ACCTBAL >= 9960");
	tpch01.add("SELECT S_NAME from SUPPLIER where S_ACCTBAL > 9960");
	tpch01.add("SELECT S_NAME FROM SUPPLIER WHERE S_ACCTBAL > 9960");
	tpch01.add("SELECT S_NAME FROM SUPPLIER WHERE S_ACCTBAL > 9960");
	tpch01.add("select S_NAME from SUPPLIER where S_ACCTBAL > 9960.0");
	tpch01.add("select S_NAME from SUPPLIER where S_ACCTBAL >= 9960");
	tpch01.add("SELECT S_NAME FROM SUPPLIER WHERE S_ACCTBAL > 9960");
	tpch01.add("select S_NAME From SUPPLIER where S_ACCTBAL > 9960");
	tpch01.add("SELECT S_NAME FROM SUPPLIER WHERE S_ACCTBAL > 9960");
	tpch01.add("SELECT S_NAME FROM SUPPLIER WHERE S_ACCTBAL > 9960");
	
	tpch02.add("SELECT N_NATIONKEY,R_REGIONKEY FROM NATION,REGION WHERE N_REGIONKEY=R_REGIONKEY");
	tpch02.add("select N_NAME, R_NAME from NATION, REGION  where N_REGIONKEY = R_REGIONKEY");
	tpch02.add("SELECT N_NAME, R_NAME FROM NATION, REGION  WHERE N_REGIONKEY=R_REGIONKEY");
	tpch02.add("SELECT A.N_NATIONKEY,A.N_NAME,A.N_REGIONKEY,A.N_COMMENT,B.R_REGIONKEY,B.R_NAME,B.R_COMMENT FROM NATION AS A,REGION AS B WHERE A.N_NAME=B.R_NAME");
	tpch02.add("select N.N_NAME,R.R_NAME from REGION R,NATION N where R.R_REGIONKEY=N.N_REGIONKEY");
	tpch02.add("SELECT NATION.N_NAME, REGION.R_NAME FROM NATION, REGION WHERE NATION.N_REGIONKEY = REGION.R_REGIONKEY");
	tpch02.add("select N_NATIONKEY,N_NAME,N_REGIONKEY from NATION ");
	tpch02.add("SELECT NATION.N_NAME,REGION.R_NAME FROM NATION,REGION");
	tpch02.add("SELECT N_NAME, R_NAME FROM NATION, REGION");
	tpch02.add("SELECT N_NAME, R_NAME FROM NATION, REGION WHERE N_REGIONKEY=R_REGIONKEY");

	tpch03.add("SELECT S_SUPPKEY,N_NAME FROM SUPPLIER ,NATION  WHERE S_NATIONKEY=N_NATIONKEY");
	tpch03.add("SELECT S_NAME , N_NAME FROM SUPPLIER , NATION WHERE S_NATIONKEY = N_NATIONKEY");
	tpch03.add("SELECT S_NAME, N_NAME FROM SUPPLIER, NATION  WHERE S_NATIONKEY=N_NATIONKEY");
	tpch03.add("SELECT A.S_NAME,B.N_NAME FROM SUPPLIER AS A,NATION AS B WHERE A.S_NAME=B.N_NAME");
	tpch03.add("select S.S_NAME,N.N_NAME from SUPPLIER S,NATION N where S.S_NATIONKEY=N.N_NATIONKEY");
	tpch03.add("SELECT SUPPLIER.S_NAME, REGION.R_NAME FROM SUPPLIER, REGION, NATION   WHERE SUPPLIER.S_NATIONKEY = NATION.N_NATIONKEY  AND NATION.N_REGIONKEY = REGION.R_REGIONKEY");
	tpch03.add("select S_NAME,S_NATIONKEY from SUPPLIER");
	tpch03.add("SELECT S_NAME,N_NAME FROM SUPPLIER,NATION");
	tpch03.add("SELECT S_NAME, N_NAME FROM SUPPLIER, NATION WHERE SUPPLIER.S_NATIONKEY =  NATION.N_NATIONKEY");
	tpch03.add("SELECT S_NAME FROM SUPPLIER, NATION WHERE S_NATIONKEY=N_NATIONKEY");
	
	tpch04.add("select O_ORDERKEY , O_ORDERSTATUS from ORDERS , LINEITEM  where O_TOTALPRICE > 530000 and L_DISCOUNT = 0");
	tpch04.add("SELECT DISTINCT O_ORDERKEY, O_ORDERSTATUS  FROM ORDERS, LINEITEM  WHERE L_ORDERKEY=O_ORDERKEY  AND L_DISCOUNT=0  AND O_TOTALPRICE>530000");
	tpch04.add("SELECT A.O_TOTALPRICE FROM ORDERS AS A,LINEITEM WHERE A.O_TOTALPRICE>=530000 AND LINEITEM.L_DISCOUNT=0");
	tpch04.add("SELECT O_ORDERKEY, O_ORDERSTATUS FROM ORDERS, LINEITEM WHERE  L_ORDERKEY = O_ORDERKEY AND O_TOTALPRICE > 530000 AND L_DISCOUNT = 0");
	tpch04.add("select O_ORDERKEY,O_ORDERSTATUS FROM ORDERS O,LINEITEM L where (O_TOTALPRICE > 530000 AND L_DISCOUNT=0 AND O_ORDERKEY=L_ORDERKEY)");
	tpch04.add("SELECT O_ORDERKEY, O_ORDERSTATUS FROM ORDERS, LINEITEM WHERE O_ORDERKEY = L_ORDERKEY AND L_DISCOUNT = 0 AND O_TOTALPRICE > 530000");
	tpch04.add("SELECT  O_ORDERKEY, O_ORDERSTATUS  FROM ORDERS,LINEITEM  WHERE  ORDERS.O_TOTALPRICE > 530000  AND LINEITEM.L_ORDERKEY = ORDERS.O_ORDERKEY  AND L_DISCOUNT = 0");
	tpch04.add("SELECT O_ORDERKEY, O_ORDERSTATUS FROM ORDERS, LINEITEM WHERE O_TOTALPRICE>530000 AND L_DISCOUNT=0");
	tpch04.add("select L_ORDERKEY,O_ORDERSTATUS From ORDERS, LINEITEM  where L_ORDERKEY=O_ORDERKEY and  O_TOTALPRICE > 530000 and L_DISCOUNT = 0");
	tpch04.add("SELECT ORDERS.O_ORDERKEY, ORDERS.O_ORDERSTATUS FROM ORDERS,LINEITEM WHERE ORDERS.O_TOTALPRICE>530000 AND LINEITEM.L_DISCOUNT=0 AND LINEITEM.L_ORDERKEY=ORDERS.O_ORDERKEY");
	// with sample
	tpch05.add("select S_NAME , S_PHONE FROM SUPPLIER , NATION where N_NAME = 'CHINA' and S_ACCTBAL > 9000");
	tpch05.add("SELECT S_NAME, S_PHONE FROM SUPPLIER, NATION  WHERE S_ACCTBAL>9000  AND S_NATIONKEY=N_NATIONKEY  AND LOWER(N_NAME)='china'");
	tpch05.add("SELECT A.S_NAME,A.S_PHONE FROM SUPPLIER AS A,CUSTOMER AS B WHERE B.C_ADDRESS='CHINA' AND B.C_ACCTBAL>=9000");
	tpch05.add("SELECT S_NAME, S_PHONE, S_ACCTBAL FROM SUPPLIER, NATION  WHERE N_NATIONKEY = S_NATIONKEY AND N_NAME = 'CHINA' AND S_ACCTBAL > 9000");
	tpch05.add("select S_NAME,S_PHONE from SUPPLIER S,NATION N where (S_ACCTBAL >= 9000 and N_NAME='CHINA' and S_NATIONKEY=N_NATIONKEY)");
	tpch05.add("SELECT S_NAME, S_PHONE FROM NATION, SUPPLIER WHERE N_NATIONKEY= S_NATIONKEY AND  N_NAME = 'China'  AND  S_ACCTBAL > 9000");
	tpch05.add("SELECT S_NAME,S_PHONE  FROM SUPPLIER,NATION  WHERE N_NAME ='CHINA'  AND S_NATIONKEY = N_NATIONKEY  AND S_ACCTBAL > 9000");
	tpch05.add("SELECT S_NAME, S_PHONE FROM SUPPLIER, NATION WHERE N_NAME='CHINA' AND S_ACCTBAL>9000");
	tpch05.add("select S_NAME,S_PHONE from SUPPLIER ,NATION   where N_NATIONKEY=S_NATIONKEY and  N_NAME = 'CHINA' and  S_ACCTBAL > 9000");
	tpch05.add("SELECT SUPPLIER.S_NAME, SUPPLIER.S_PHONE FROM SUPPLIER, NATION  WHERE NATION.N_NATIONKEY=SUPPLIER.S_NATIONKEY AND NATION.N_NAME='CHINA' AND SUPPLIER.S_ACCTBAL>9000");

	tpch06.add("SELECT C_NAME , C_ACCTBAL FROM CUSTOMER , NATION  WHERE N_NAME ='EUROPE' AND C_ACCTBAL > 9990");
	tpch06.add("SELECT C_NAME, C_ACCTBAL FROM CUSTOMER C, NATION N, REGION R  WHERE C.C_ACCTBAL>9990  AND C.c_NATIONKEY=N.N_NATIONKEY  AND N.N_REGIONKEY=R.R_REGIONKEY  AND lower(R_NAME)='europe'"); // good query
	tpch06.add("SELECT C_NAME,C_ACCTBAL FROM CUSTOMER WHERE C_ADDRESS='EUROPE'  AND C_ACCTBAL>=9990");
	tpch06.add("select a.C_NAME,a.C_ACCTBAL from CUSTOMER a,NATION b where a.C_NATIONKEY=b.N_NATIONKEY and b.N_NAME='Europe' and a.C_ACCTBAL >9990");
	tpch06.add("select C_NAME,C_ACCTBAL from CUSTOMER,NATION where CUSTOMER.C_NATIONKEY= NATION.N_NATIONKEY AND NATION.N_NAME='EUROPE' AND CUSTOMER.C_ACCTBAL>9990");
	tpch06.add("SELECT C_NAME, C_ACCTBAL  FROM CUSTOMER,  NATION,  REGION   WHERE C_NATIONKEY = N_NATIONKEY AND N_REGIONKEY = R_REGIONKEY AND R_NAME = 'EUROPE'  AND C_ACCTBAL > 9990");
	tpch06.add("SELECT CUSTOMER.C_NAME,NATION.N_NAME FROM CUSTOMER,NATION WHERE NATION.N_NAME='EUROPE' AND CUSTOMER.C_ACCTBAL>9990");
	tpch06.add("SELECT C_NAME,C_ACCTBAL FROM CUSTOMER WHERE C_ADDRESS = 'EUROPE' AND C_ACCTBAL > 9990");
	tpch06.add("SELECT C_NAME  FROM CUSTOMER C, NATION N  WHERE C.C_NATIONKEY = N.N_NATIONKEY  AND N.N_NAME = 'EUROPE'  AND C_ACCTBAL > 9990");
	tpch06.add("SELECT C_NAME, C_ACCTBAL FROM CUSTOMER, NATION WHERE N_NAME='EUROPE' AND C_ACCTBAL>9990");
	// with sample
	tpch07.add("SELECT S_NAME , S_PHONE FROM SUPPLIER , PART WHERE S_ACCTBAL > 1000 AND 	P_RETAILPRICE > 2096");
	tpch07.add("SELECT S_NAME, S_PHONE FROM SUPPLIER, PARTSUPP WHERE PS_SUPPKEY=S_SUPPKEY  AND S_ACCTBAL>1000  AND PS_SUPPLYCOST>2096");
	tpch07.add("SELECT A.S_NAME,A.S_PHONE FROM SUPPLIER AS A,PART AS B WHERE A.S_ACCTBAL>1000 AND B.P_RETAILPRICE>2096");
	tpch07.add("select a.S_NAME,a.S_PHONE from SUPPLIER a,PARTSUPP b where a.S_SUPPKEY=b.PS_SUPPKEY and a.S_ACCTBAL>1000 and b.PS_SUPPLYCOST>2096");
	tpch07.add("select SUPPLIER.S_NAME,SUPPLIER.S_PHONE from SUPPLIER,PARTSUPP,PART where SUPPLIER.S_ACCTBAL>1000 AND PART.P_RETAILPRICE>2096 AND PARTSUPP.PS_SUPPKEY=SUPPLIER.S_SUPPKEY AND PARTSUPP.PS_PARTKEY=PART.P_PARTKEY");
	tpch07.add("SELECT S_NAME, S_PHONE  FROM SUPPLIER,  PARTSUPP,  PART WHERE S_ACCTBAL > 1000 AND S_SUPPKEY = PS_SUPPKEY AND PS_PARTKEY = P_PARTKEY  AND P_RETAILPRICE > 2096");
	tpch07.add("SELECT S_NAME,S_PHONE FROM SUPPLIER WHERE S_ACCTBAL >1000");
	tpch07.add("SELECT S_NAME,S_PHONE FROM SUPPLIER, PARTSUPP WHERE SUPPLIER.S_ACCTBAL > 1000 AND PARTSUPP.PS_SUPPLYCOST > 2096");
	tpch07.add("SELECT S.S_NAME, S.S_PHONE  FROM SUPPLIER  S, PART P, PARTSUPP PS  WHERE S_ACCTBAL > 1000   AND P.P_PARTKEY = PS.PS_PARTKEY  AND S.S_SUPPKEY = PS.PS_SUPPKEY  AND P_RETAILPRICE > 2096");
	tpch07.add("SELECT DISTINCT S_NAME, S_PHONE FROM SUPPLIER, PART, PARTSUPP WHERE PS_PARTKEY = P_PARTKEY AND PS_SUPPKEY=S_SUPPKEY AND P_RETAILPRICE>2096");
    }
}
