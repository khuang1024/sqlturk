package sqlturk.experiment.mturk;

import java.util.ArrayList;

import sqlturk.configuration.Parameters;
import sqlturk.mturk.RankingManager;

import com.amazonaws.mturk.service.axis.RequesterService;
import com.amazonaws.mturk.util.PropertiesClientConfig;

/**
 * This class is for deploying tasks on MTurk.
 * 
 * @author Kerui Huang
 * 
 */
public class CreateRankingHITs {

    private CreateRankingHITs() {
	throw new AssertionError();
    }

    @SuppressWarnings("unused")
    public static void main(String[] args) {
	RequesterService service;
	if (Parameters.CURRENT_MODE == Parameters.PRODUCTION_MODE) {
	    service = new RequesterService(new PropertiesClientConfig(
		    Parameters.MTURK_PROPS_FILE_PRODUCTION));
	} else {
	    service = new RequesterService(new PropertiesClientConfig(
		    Parameters.MTURK_PROPS_FILE_SANDBOX));
	}
	String reason = "Sorry, since you were one of the authors writing these queries, you are not allowed to rank these queries.";

	/*
	 * "world"/"tpch" is the name of the dataset used for this HIT
	 * the index starts from 0
	 * 	- 0 is the index of question
	 * 	- 1 is the index of a similar question to the previous one
	 */
	
	/*
	 * Dataset: demograpgics/word
	 * Questions: 0, 1
	 * HIT: 2LNUUKQOYGNIQJ9IV570507TXYGYGU
	 */
	ArrayList<String> worldCandidateQueries01 = new ArrayList<String>();
	// without sample
	worldCandidateQueries01.add("select Name from Country where Region like 'Western Europe'"); // "like" is supported
	worldCandidateQueries01.add("select Name from Country where Region = 'Western Europe'");
	worldCandidateQueries01.add("select Country.Name from Country where Country.Region = 'Western Europe'");
	worldCandidateQueries01.add("select Region from Country where Region='Europe'");
	worldCandidateQueries01.add("select Name from Country where Region='Europe'");
	worldCandidateQueries01.add("SELECT * FROM Country WHERE Region='Western Europe'");
	worldCandidateQueries01.add("SELECT Name from Country WHERE Region='Western Europe'");
	worldCandidateQueries01.add("select Country.Name  From Country  Where Region = 'Western Europe'");
	worldCandidateQueries01.add("select Name from Country where Region like 'Western Europe'");
	worldCandidateQueries01.add("select Name from Country where Region='Western Europe'");
	// with sample
	worldCandidateQueries01.add("select CountryCode from CountryLanguage where Language like 'French'");
	worldCandidateQueries01.add("select CountryCode from CountryLanguage where Language = 'French'");
	worldCandidateQueries01.add("Select CountryLanguage.CountryCode from CountryLanguage where CountryLanguage.Language ='French'");
	worldCandidateQueries01.add("select CountryCode from CountryLanguage where Language='French';");
	worldCandidateQueries01.add("select CountryCode from CountryLanguage where Language='French'");
	worldCandidateQueries01.add("SELECT * FROM CountryLanguage WHERE Language='French'");
	worldCandidateQueries01.add("SELECT CountryCode FROM CountryLanguage WHERE Language='French'");
	worldCandidateQueries01.add("select CountryLanguage.CountryCode  From CountryLanguage where Language = 'French'");
	worldCandidateQueries01.add("select CountryCode from CountryLanguage where Language like 'French'");
	worldCandidateQueries01.add("select CountryCode from CountryLanguage where Language='French'");
	// block the authers
	String[] blockedRankerIdsWorld01 = {"A3988MX4PJCUJW", "A355UW5P1BF6M", 
		"A16FY9L7QTDNRW", "A2NHNDD0W2KPR3", "A3IL3HGJW7K6Q1" , 
		"AVRG4735RNH2D", "A1JS6BDUG7WWYP", "A2D3TM156X7Y61", 
		"A1KU2J85SQLP0U", "A3L8RYKHDDHZ10"};
	
	/*
	 * Dataset: demograpgics/word
	 * Questions: 2, 3
	 * HIT: 2ETAT2D5NQYNZXPGN075XYMOEE4843
	 */
	ArrayList<String> worldCandidateQueries23 = new ArrayList<String>();
	// with sample
	worldCandidateQueries23.add("select a.Name,b.Name from Country a, City b where a.Capital = b.ID ;");
	worldCandidateQueries23.add("select Name,Capital from Country");
	worldCandidateQueries23.add("select Country.Name,Country.Capital from Country; ");
	worldCandidateQueries23.add("select Name,Capital from Country");
	worldCandidateQueries23.add("select Name, Capital from Country");
	worldCandidateQueries23.add("SELECT Country.Name,Country.Capital FROM City,Country WHERE City.CountryCode = Country.Code");
	worldCandidateQueries23.add("select ctry.Name,cty.Name from Country as ctry,City as cty where ctry.Capital = cty.ID");
	worldCandidateQueries23.add("SELECT Country.Name, City.Name  FROM Country, City  WHERE Country.Capital= City.ID");
	worldCandidateQueries23.add("select Name,Capital from Country");
	worldCandidateQueries23.add("select c2.name Country, c1.name, Capital  from City c1, Country c2  where c1.ID=c2.Capital");
	// without sample
	worldCandidateQueries23.add("select a.Name,b.Language from Country a, CountryLanguage b where a.Code = b.CountryCode");
	worldCandidateQueries23.add("select Name from City where CountryCode in(select Language from CountryLanguage)"); // // the query is syntactically right and can be rewritten, can we have its provenance? and do we need to rewrite the sub-SPJ?
	worldCandidateQueries23.add("select Country.Name,CountryLanguage.Language from Country,CountryLanguage where Country.Code = CountryLanguage.CountryCode;");
	worldCandidateQueries23.add("select C.Name,CL.Language from Country C,CountryLanguage CL where C.Code=CL.CountryCode");
	worldCandidateQueries23.add("select c.Name, cl.Language from Country c, CountryLanguage cl  where c.Code=cl.CountryCode");
	worldCandidateQueries23.add("SELECT Country.Name,CountryLanguage.Language FROM Country,CountryLanguage WHERE Country.Code = CountryLanguage.CountryCode ORDER BY Country.Name"); // correct
	worldCandidateQueries23.add("select ctry.Name,lan.Language from Country as ctry, CountryLanguage as lan where lan.CountryCode = ctry.Code");
	worldCandidateQueries23.add("SELECT Country.Name, CountryLanguage.Language   FROM Country, CountryLanguage  WHERE Country.Code = CountryLanguage.CountryCode");
	worldCandidateQueries23.add("Select c.Name,cl.Language from Country c,CountryLanguage cl where c.Code=cl.CountryCode");
	worldCandidateQueries23.add("select c2.name Country, c3.language Language  from  Country c2, CountryLanguage c3  where c3.CountryCode= c2.Code");
	// block the authers
	String[] blockedRankerIdsWorld23 = {"A355UW5P1BF6M", "A3988MX4PJCUJW", 
			"A16FY9L7QTDNRW", "A3IL3HGJW7K6Q1", "A1JS6BDUG7WWYP" , 
			"A88XXN3BWMTVD", "A1KU2J85SQLP0U", "A2D3TM156X7Y61", 
			"A3VAX2NYGYMTP8", "A3L8RYKHDDHZ10"};
	
	/*
	 * Dataset: demograpgics/word
	 * Questions: 4, 5
	 * HIT: 2W151Y7QEOZFS8ERZ9KMSFXSFAADG1
	 */
	ArrayList<String> worldCandidateQueries45 = new ArrayList<String>();
	// without sample
	worldCandidateQueries45.add("select Name from Country where code in(select CountryCode from CountryLanguage where Language like 'Swedish')"); // the query is syntactically right and can be rewritten, can we have its provenance? and do we need to rewrite the sub-SPJ?
	worldCandidateQueries45.add("select a.Name from Country a, CountryLanguage c where c.CountryCode = a.Code and c.Language = 'Swedish' and c.IsOfficial = 'T'");
	worldCandidateQueries45.add("Select a.Name,b.IsOfficial from Country as a,CountryLanguage as b where a.Name='Swedish'");
	worldCandidateQueries45.add("select Country.Name from Country,CountryLanguage  where (Country.Code = CountryLanguage.CountryCode) and (CountryLanguage.Language = 'Swedish') and (CountryLanguage.IsOfficial = 'T')"); // correct
	worldCandidateQueries45.add("select C.Name from Country C,CountryLanguage CL where CL.Language='Swedish' and CL.IsOfficial='T'");
	worldCandidateQueries45.add("SELECT Name from Country C, CountryLanguage CL   WHERE C.Code = CL.CountryCode  AND CL.IsOfficial is true  and CL.Language='Swedish'");
	worldCandidateQueries45.add("select ctry.Name from Country as ctry,CountryLanguage as lan where lan.Language like 'Swedish' and lan.IsOfficial = 'Y' and lan.CountryCode = ctry.Code"); // can rewrite it, but lan.IsOfficial should be 'T'... not 'Y'. so it is empty. 
	worldCandidateQueries45.add("select c2.name Name  from Country c2, CountryLanguage c3  where c3.CountryCode= c2.Code and  c3.Language='Swedish'");
	worldCandidateQueries45.add("Select Country.Name from Country, CountryLanguage where CountryLanguage.Language = 'Swedish' and Country.Code = CountryLanguage.CountryCode");
	worldCandidateQueries45.add("SELECT Country.Name FROM Country, CountryLanguage WHERE CountryLanguage.CountryCode = Country.Code AND CountryLanguage.Language = 'Swedish' AND CountryLanguage.IsOfficial = 'T'");
	// with sample
	worldCandidateQueries45.add("select District from City where Population>3000000 and CountryCode in(select CountryCode from CountryLanguage where Language like 'Chinese')");
	worldCandidateQueries45.add("select b.Name from City b, CountryLanguage c where c.CountryCode = b.CountryCode and c.Language = 'Chinese' and b.Population > 3000000");
	worldCandidateQueries45.add("select name from City where Population>=3000000;");
	worldCandidateQueries45.add("select City.Name from City, CountryLanguage where (City.CountryCode = CountryLanguage.CountryCode) and (CountryLanguage.Language = 'Chinese') and (City.Population > 3000000)"); // the query is syntactically right and can be rewritten, can we have its provenance? and do we need to rewrite the sub-SPJ?
	worldCandidateQueries45.add("select C.Name from City C,Country CU,CountryLanguage CL where CU.Population>3000000 and CL.Language='Chinese'");
	worldCandidateQueries45.add("SELECT Cy.Name from City Cy, Country C, CountryLanguage CL  where Cy.CountryCode = C.Code and C.Code=CL.CountryCode  and CL.Language='Chinese'  and Cy.population>3000000");
	worldCandidateQueries45.add("select cty.Name from City as cty,CountryLanguage as lan where lan.Language like 'Chinese' and cty.Population > 3000000");
	worldCandidateQueries45.add("select c1.name Name  from City c1, Country c2, CountryLanguage c3  where c1.CountryCode=c2.Code and  c3.CountryCode= c2.Code and  c3.language='Chinese' and   c1.Population>3000000");
	worldCandidateQueries45.add("Select City.Name  From City, CountryLanguage, Country  Where CountryLanguage.Language = 'Chinese'  and City.Population > 3000000 and Country.Code = CountryLanguage.CountryCode  and City.CountryCode = Country.Code");
	worldCandidateQueries45.add("SELECT City.Name FROM City, Country, CountryLanguage WHERE CountryLanguage.CountryCode = Country.Code AND Country.Code = City.CountryCode AND CountryLanguage.Language = 'Chinese' AND City.Population > 3000000");
	// block the authers
	String[] blockedRankerIdsWorld45 = {"A3988MX4PJCUJW", "A355UW5P1BF6M", 
				"A2NHNDD0W2KPR3", "A16FY9L7QTDNRW", "A3IL3HGJW7K6Q1" , 
				"A1JS6BDUG7WWYP", "A1KU2J85SQLP0U", "A3L8RYKHDDHZ10", 
				"A2XSBL2XUZG9JE", "A27Y4PDYXFF976"};
	
	/*
	 * Dataset: demograpgics/word
	 * Questions: 6, 7
	 * HIT: 2162L92HECWA0FPOBT3F4CP6MFVIH7
	 */
	ArrayList<String> worldCandidateQueries67 = new ArrayList<String>();
	// without sample
	worldCandidateQueries67.add("select Name,Capital from Country where code in(select CountryCode from CountryLanguage where language like 'English' and Percentage<=50)"); // can rewrite it, and no syntax error... but the rewritten query is almost the same, because it has no foreign keys.
	worldCandidateQueries67.add("select a.Name,b.Name from Country a, City b, CountryLanguage c where a.Capital = b.ID and c.CountryCode = a.Code and c.Language = 'English' and c.Percentage >= 50");
	worldCandidateQueries67.add("select Name, Capital from Country, CountryLanguage where (Country.Code = CountryLanguage.CountryCode) and (CountryLanguage.Language = 'English' and CountryLanguage.Percentage >= 50)");
	worldCandidateQueries67.add("Select C.Name,C.Capital from Country C,CountryLanguage CL where CL.Language='English' and CL.Percentage>'50.0'");
	worldCandidateQueries67.add("Select Country.Name, Country.Capital  From Country , CountryLanguage   Where CountryLanguage.Language = 'English' and CountryLanguage.Percentage >= 50  and CountryLanguage.CountryCode = Country.Code");
	worldCandidateQueries67.add("select C.Name, Cy.Name from Country C, City Cy WHERE C.Capital = Cy.ID  AND EXISTS (SELECT * FROM CountryLanguage WHERE CountryCode = C.Code AND Language='English' AND Percentage>='50.0')"); // can rewrite it and no syntax error... don't need to rewrite the sub-SPJ because CountryLanguage has no foreign keys
	worldCandidateQueries67.add("select c2.name Country, c1.name Capital from City c1, Country c2, CountryLanguage c3  where c1.id=c2.Capital and   c3.CountryCode= c2.Code and  c3.Language='English' and   c3.Percentage >=50");
	worldCandidateQueries67.add("select Name, Capital from Country where Code in (select CountryCode from CountryLanguage where Language = 'English' and Percentage >= 50.0)");
	worldCandidateQueries67.add("SELECT C.Name,C.LocalName FROM City CC,Country C,CountryLanguage CCC WHERE CC.CountryCode=C.Code AND C.Code=CCC.CountryCode AND CCC.Language='English' AND C.Population > 50");
	worldCandidateQueries67.add("SELECT Name ,Capital FROM Country , CountryLanguage where Language = 'English' and Percentage >= 50");
	// with sample
	worldCandidateQueries67.add("select Name,Population from City where Population<200000 and CountryCode in(select CountryCode from CountryLanguage where Language like 'Spanish')"); // can rewrite it, and no syntax error... but the rewritten query is almost the same, because it has no foreign keys.
	worldCandidateQueries67.add("select b.Name,b.Population from Country a ,City b, CountryLanguage c where a.Capital = b.ID and a.Continent = 'Europe' and a.Code = c.CountryCode and c.Language = 'Spanish' and c.IsOfficial = 'T'");
	worldCandidateQueries67.add("select City.Name,City.Population from City, Country,CountryLanguage where (Country.Code = City.CountryCode) and (Country.Code = CountryLanguage.CountryCode) and  (CountryLanguage.Language = 'Spanish') and (Country.Continent = 'Europe') and (City.Population < 200000)");
	worldCandidateQueries67.add("select C.Name,C.Population from City C,Country CU,CountryLanguage CL where CL.Language='Spanish' and CU.Region='Europe' and C.Population<200000");
	worldCandidateQueries67.add("Select City.Name, City.Population  From City, Country, CountryLanguage   Where Country.Continent = 'Europe' and CountryLanguage.Language = 'Spanish' and Country.Population < 200000  and CountryLanguage.CountryCode = Country.Code and City.CountryCode = Country.Code");
	/*
	 * The sub-SPJ should be rewritten.
	 */
	worldCandidateQueries67.add("SELECT Cy.Name, Cy.Population FROM City Cy  WHERE Cy.CountryCode IN (Select C.Code FROM Country C, CountryLanguage CL WHERE C.Code = CL.CountryCode AND CL.IsOfficial='T' AND CL.Language='Spanish' AND C.Continent='Europe') AND Cy.Population<200000");
	worldCandidateQueries67.add("select c1.name Name, c1.population Population  from City c1, Country c2, CountryLanguage c3  where c1.id=c2.Capital and  c3.CountryCode= c2.Code and  c2.Continent='Europe' and   c3.language='Spanish' and   c1.Population<200000");
	worldCandidateQueries67.add("select Name, Population from City where CountryCode in (select CountryCode from CountryLanguage where Language = 'Spanish') and Population <= 200000");// can rewrite it and no syntax error... don't need to rewrite the sub-SPJ because CountryLanguage has no foreign keys
	worldCandidateQueries67.add("SELECT C.Name,C.Population FROM City C,Country CC,CountryLanguage CL WHERE C.CountryCode=CC.Code AND CL.CountryCode=C.CountryCode AND CC.Continent='Europe' AND CL.Language='Spanish' AND CL.IsOfficial='T' AND  C.Population < 200000");
	worldCandidateQueries67.add("Select Name , Population FROM Country , CountryLanguage where Continent = 'Europe' and Language = 'Spanish' and IsOfficial = 'T' and Population  < 200000");
	String[] blockedRankerIdsWorld67 = {"A3988MX4PJCUJW", "A355UW5P1BF6M", 
		"A16FY9L7QTDNRW", "A3IL3HGJW7K6Q1", "A2D3TM156X7Y61" , 
		"A1JS6BDUG7WWYP", "A3L8RYKHDDHZ10", "A3VP83YCQLJ618", 
		"A3MUWXFV6C8PPP", "AG1J7P4XG8BSL"};
	
	/*
	 * Dataset: tpch
	 * Questions: 0, 1
	 * HIT: 2ZRNZW6HEZ6O8MCMUAZ6DGUN4TQZPT
	 */
	ArrayList<String> tpchCandidateQueries01 = new ArrayList<String>();
	// without sample
	tpchCandidateQueries01.add("select O_CLERK from ORDERS O where O_TOTALPRICE >= 500000");
	tpchCandidateQueries01.add("SELECT O_CLERK from ORDERS where O_TOTALPRICE > 500000");
	tpchCandidateQueries01.add("SELECT O_CLERK FROM ORDERS WHERE O_TOTALPRICE>500000");
	tpchCandidateQueries01.add("SELECT O_CLERK FROM ORDERS WHERE O_TOTALPRICE>500000;");
	tpchCandidateQueries01.add("Select O_CLERK from ORDERS where O_TOTALPRICE>500000");
	tpchCandidateQueries01.add("select O_CLERK from ORDERS where O_TOTALPRICE>=500000");
	tpchCandidateQueries01.add("SELECT DISTINCT O_CLERK FROM ORDERS WHERE O_TOTALPRICE>500000");
	tpchCandidateQueries01.add("select O_CLERK From ORDERS where O_TOTALPRICE > 500000");
	tpchCandidateQueries01.add("SELECT O_CLERK FROM ORDERS WHERE O_TOTALPRICE > 500000");
	tpchCandidateQueries01.add("SELECT O_CLERK FROM ORDERS WHERE O_TOTALPRICE > 500000");
	// with sample
	tpchCandidateQueries01.add("select S_NAME from SUPPLIER S where S_ACCTBAL >= 9960");
	tpchCandidateQueries01.add("SELECT S_NAME from SUPPLIER where S_ACCTBAL > 9960");
	tpchCandidateQueries01.add("SELECT S_NAME FROM SUPPLIER WHERE S_ACCTBAL>9960");
	tpchCandidateQueries01.add("SELECT S_NAME FROM SUPPLIER WHERE S_ACCTBAL>9960");
	tpchCandidateQueries01.add("select S_NAME from SUPPLIER where S_ACCTBAL>9960.0");
	tpchCandidateQueries01.add("select S_NAME from SUPPLIER where S_ACCTBAL>=9960");
	tpchCandidateQueries01.add("SELECT S_NAME FROM SUPPLIER WHERE S_ACCTBAL > 9960");
	tpchCandidateQueries01.add("select S_NAME From SUPPLIER where S_ACCTBAL > 9960");
	tpchCandidateQueries01.add("SELECT S_NAME FROM SUPPLIER WHERE S_ACCTBAL > 9960");
	tpchCandidateQueries01.add("SELECT S_NAME FROM SUPPLIER WHERE S_ACCTBAL >9960;");
	String[] blockedRankerIdsTpch01 = {"AI2ASXAI9ZFDE", "AG1J7P4XG8BSL", 
		"A1JS6BDUG7WWYP", "A2NHNDD0W2KPR3", "A3IL3HGJW7K6Q1" , 
		"A3988MX4PJCUJW", "A1GSWHQ1BXWVE7", "A3L8RYKHDDHZ10", 
		"A1957F34UKAYHY", "A125ODOYS2ZZDL"};
	
	/*
	 * Dataset: tpch
	 * Questions: 2, 3
	 * HIT: 255NVRH1KHO98YD1UNEVDLKX9PCZQK
	 */
	ArrayList<String> tpchCandidateQueries23 = new ArrayList<String>();
	// without sample
	tpchCandidateQueries23.add("SELECT N_NATIONKEY,R_REGIONKEY FROM NATION,REGION WHERE N_REGIONKEY=R_REGIONKEY");
	tpchCandidateQueries23.add("select N_NAME, R_NAME from NATION, REGION  where N_REGIONKEY = R_REGIONKEY");
	tpchCandidateQueries23.add("SELECT N_NAME, R_NAME FROM NATION, REGION  WHERE N_REGIONKEY=R_REGIONKEY");
	tpchCandidateQueries23.add("SELECT A.N_NATIONKEY,A.N_NAME,A.N_REGIONKEY,A.N_COMMENT,B.R_REGIONKEY,B.R_NAME,B.R_COMMENT FROM NATION AS A,REGION AS B WHERE A.N_NAME=B.R_NAME");
	tpchCandidateQueries23.add("select N.N_NAME,R.R_NAME from REGION R,NATION N where R.R_REGIONKEY=N.N_REGIONKEY");
	tpchCandidateQueries23.add("SELECT NATION.N_NAME, REGION.R_NAME FROM NATION, REGION WHERE NATION.N_REGIONKEY = REGION.R_REGIONKEY;");
	tpchCandidateQueries23.add("select N_NATIONKEY,N_NAME,N_REGIONKEY from NATION ");
	tpchCandidateQueries23.add("SELECT NATION.N_NAME,REGION.R_NAME FROM NATION,REGION");
	tpchCandidateQueries23.add("SELECT N_NAME, R_NAME FROM NATION, REGION");
	tpchCandidateQueries23.add("SELECT N_NAME, R_NAME FROM NATION, REGION WHERE N_REGIONKEY=R_REGIONKEY");
	// with sample
	tpchCandidateQueries23.add("SELECT S_SUPPKEY,N_NAME FROM SUPPLIER ,NATION  WHERE S_NATIONKEY=N_NATIONKEY");
	tpchCandidateQueries23.add("SELECT S_NAME , N_NAME FROM SUPPLIER , NATION WHERE S_NATIONKEY = N_NATIONKEY");
	tpchCandidateQueries23.add("SELECT S_NAME, N_NAME FROM SUPPLIER, NATION  WHERE S_NATIONKEY=N_NATIONKEY  ");
	tpchCandidateQueries23.add("SELECT A.S_NAME,B.N_NAME FROM SUPPLIER AS A,NATION AS B WHERE A.S_NAME=B.N_NAME;  ");
	tpchCandidateQueries23.add("select S.S_NAME,N.N_NAME from SUPPLIER S,NATION N where S.S_NATIONKEY=N.N_NATIONKEY");
	tpchCandidateQueries23.add("SELECT SUPPLIER.S_NAME, REGION.R_NAME FROM SUPPLIER, REGION, NATION   WHERE SUPPLIER.S_NATIONKEY = NATION.N_NATIONKEY  AND NATION.N_REGIONKEY = REGION.R_REGIONKEY");
	tpchCandidateQueries23.add("select S_NAME,S_NATIONKEY from SUPPLIER");
	tpchCandidateQueries23.add("SELECT S_NAME,N_NAME FROM SUPPLIER,NATION");
	tpchCandidateQueries23.add("SELECT S_NAME, N_NAME FROM SUPPLIER, NATION WHERE SUPPLIER.S_NATIONKEY =  NATION.N_NATIONKEY");
	tpchCandidateQueries23.add("SELECT S_NAME FROM SUPPLIER, NATION WHERE S_NATIONKEY=N_NATIONKEY");
	String[] blockedRankerIdsTpch23 = {"A3MUWXFV6C8PPP", "AG1J7P4XG8BSL", 
		"A1JS6BDUG7WWYP", "A2NHNDD0W2KPR3", "A3IL3HGJW7K6Q1" , 
		"A4HUZZCO72VIN", "A3988MX4PJCUJW", "A1K50CQRFECIUI", 
		"A1NCIR8ZUDCRJW", "A1GSWHQ1BXWVE7"};
	
	/*
	 * Dataset: tpch
	 * Questions: 4, 5
	 * HIT: 2CIFK0COK2JEV4TF710LVW6P121RKR
	 */
	ArrayList<String> tpchCandidateQueries45 = new ArrayList<String>();
	// without sample
	tpchCandidateQueries45.add("select O_ORDERKEY , O_ORDERSTATUS from ORDERS , LINEITEM  where O_TOTALPRICE > 530000 and L_DISCOUNT = 0");
	tpchCandidateQueries45.add("SELECT DISTINCT O_ORDERKEY, O_ORDERSTATUS  FROM ORDERS, LINEITEM  WHERE L_ORDERKEY=O_ORDERKEY  AND L_DISCOUNT=0  AND O_TOTALPRICE>530000");
	tpchCandidateQueries45.add("SELECT A.O_TOTALPRICE FROM ORDERS AS A,LINEITEM WHERE A.O_TOTALPRICE>=530000 AND LINEITEM.L_DISCOUNT=0");
	tpchCandidateQueries45.add("SELECT O_ORDERKEY, O_ORDERSTATUS FROM ORDERS, LINEITEM WHERE  L_ORDERKEY = O_ORDERKEY AND O_TOTALPRICE > 530000 AND L_DISCOUNT = 0");
	tpchCandidateQueries45.add("select O_ORDERKEY,O_ORDERSTATUS FROM ORDERS O,LINEITEM L where (O_TOTALPRICE > 530000 AND L_DISCOUNT=0 AND O_ORDERKEY=L_ORDERKEY)");
	tpchCandidateQueries45.add("select O.O_ORDERKEY,O.O_ORDERSTATUS from ORDERS O,LINEITEM LI where O_TOTALPRICE>530000 and LI.L_ORDERKEY=O.O_ORDERKEY and LI.L_DISCOUNT=0");
	/*
	 * In clause
	 * cannot run?
	 */
	tpchCandidateQueries45.add("select O_CUSTKEY,O_ORDERSTATUS from ORDERS where O_TOTALPRICE<530000 in(select L_DISCOUNT from LINEITEM where L_DISCOUNT=0)");
	tpchCandidateQueries45.add("SELECT O_ORDERKEY, O_ORDERSTATUS FROM ORDERS, LINEITEM WHERE O_TOTALPRICE>530000 AND L_DISCOUNT=0");
	tpchCandidateQueries45.add("select L_ORDERKEY,O_ORDERSTATUS From ORDERS , LINEITEM  where L_ORDERKEY=O_ORDERKEY and  O_TOTALPRICE > 530000 and  L_DISCOUNT = 0");
	tpchCandidateQueries45.add("SELECT ORDERS.O_ORDERKEY, ORDERS.O_ORDERSTATUS FROM ORDERS,LINEITEM WHERE ORDERS.O_TOTALPRICE>530000 AND LINEITEM.L_DISCOUNT=0 AND LINEITEM.L_ORDERKEY=ORDERS.O_ORDERKEY");
	// with sample
	tpchCandidateQueries45.add("select S_NAME , S_PHONE FROM SUPPLIER , NATION where N_NAME = 'CHINA' and S_ACCTBAL > 9000");
	tpchCandidateQueries45.add("SELECT S_NAME, S_PHONE FROM SUPPLIER, NATION  WHERE S_ACCTBAL>9000  AND S_NATIONKEY=N_NATIONKEY  AND LOWER(N_NAME)='china'");
	tpchCandidateQueries45.add("SELECT A.S_NAME,A.S_PHONE FROM SUPPLIER AS A,CUSTOMER AS B WHERE B.C_ADDRESS='CHINA' AND B.C_ACCTBAL>=9000");
	/*
	 * typo: but if we change s_PHONE to S_PHONE, it works!
	 */
	tpchCandidateQueries45.add("SELECT S_NAME, s_PHONE, S_ACCTBAL FROM SUPPLIER, NATION  WHERE N_NATIONKEY = s_NATIONKEY AND N_NATIONKEY = 'CHINA' AND s_ACCTBAL > 9000");
	tpchCandidateQueries45.add("select S_NAME,S_PHONE from SUPPLIER S,NATION N where (S_ACCTBAL >= 9000 and N_NAME='CHINA' and S_NATIONKEY=N_NATIONKEY)");
	tpchCandidateQueries45.add("select S.S_NAME,S.S_PHONE from SUPPLIER S  where S.S_ACCTBAL>9000 and S.S_NATIONKEY in (select N.N_NATIONKEY from NATION N,REGION R where R.R_NAME='CHINA' and N.N_REGIONKEY=R.R_REGIONKEY)"); // don't rewrite the sub-SPJ. However, the result (no matter rewritting it or not) is empty set
	tpchCandidateQueries45.add("select S_NAME,S_PHONE from SUPPLIER where S_ACCTBAL>9000 in(select N_NATIONKEY from NATION where N_NAME like 'CHINA')"); // don't rewrite sub-SPJ, can rewrite and run
	tpchCandidateQueries45.add("SELECT S_NAME, S_PHONE FROM SUPPLIER, NATION WHERE N_NAME='CHINA' AND S_ACCTBAL>9000");
	tpchCandidateQueries45.add("select S_NAME,S_PHONE from SUPPLIER ,NATION   where N_NATIONKEY=S_NATIONKEY and  N_NAME = 'CHINA' and  S_ACCTBAL > 9000");
	tpchCandidateQueries45.add("SELECT SUPPLIER.S_NAME, SUPPLIER.S_PHONE FROM SUPPLIER, NATION  WHERE NATION.N_NATIONKEY=SUPPLIER.S_NATIONKEY AND NATION.N_NAME='CHINA' AND SUPPLIER.S_ACCTBAL>9000");
	String[] blockedRankerIdsTpch45 = {"AG1J7P4XG8BSL", "A1JS6BDUG7WWYP", 
		"A2NHNDD0W2KPR3", "A15A7KUOBMIQVU", "AI2ASXAI9ZFDE" , 
		"A3IL3HGJW7K6Q1", "A3988MX4PJCUJW", "A1GSWHQ1BXWVE7", 
		"A3L8RYKHDDHZ10", "A26Q3BHSFOW110"};
	
	/*
	 * Dataset: tpch
	 * Questions: 6, 7
	 * HIT: 2K9Y0LWSBRI8CCXSLB08NHSUN3TC01
	 */	
	ArrayList<String> tpchCandidateQueries67 = new ArrayList<String>();
	// without sample
	tpchCandidateQueries67.add("SELECT C_NAME , C_ACCTBAL FROM CUSTOMER , NATION  WHERE N_NAME ='EUROPE' AND C_ACCTBAL > 9990");
	tpchCandidateQueries67.add("SELECT C_NAME, C_ACCTBAL FROM CUSTOMER C, NATION N, REGION R  WHERE C.C_ACCTBAL>9990  AND C.c_NATIONKEY=N.N_NATIONKEY  AND N.N_REGIONKEY=R.R_REGIONKEY  AND lower(R_NAME)='europe'"); // good query
	tpchCandidateQueries67.add("SELECT C_NAME,C_ACCTBAL FROM CUSTOMER WHERE C_ADDRESS='EUROPE'  AND C_ACCTBAL>=9990;");
	tpchCandidateQueries67.add("select C_NAME, C_ACCTBAL   from CUSTOMER  where C_NATIONKEY in  (select N_NATIONKEY from NATION where N_NAME = 'EUROPE')"); // don't rewrite sub-SPJ, but empty set no matter rewritting it or not...
	tpchCandidateQueries67.add("select C.C_NAME,C.C_ACCTBAL from CUSTOMER C where C_NATIONKEY in (select N.N_NATIONKEY from NATION N,REGION R where R.R_NAME='EUROPE') and C.C_ACCTBAL>9990");
	tpchCandidateQueries67.add("select C_NAME,C_ACCTBAL from CUSTOMER where C_NATIONKEY like 'EUROPE' and C_ACCTBAL>9990");
	tpchCandidateQueries67.add("SELECT CUSTOMER.C_NAME,NATION.N_NAME FROM CUSTOMER,NATION WHERE NATION.N_NAME='EUROPE' AND CUSTOMER.C_ACCTBAL>9990");
	tpchCandidateQueries67.add("SELECT C_NAME,C_ACCTBAL FROM CUSTOMER WHERE C_ADDRESS = 'EUROPE' AND C_ACCTBAL > 9990");
	tpchCandidateQueries67.add("SELECT C_NAME  FROM CUSTOMER C, NATION N  WHERE C.C_NATIONKEY = N.N_NATIONKEY  AND N.N_NAME = 'EUROPE'  AND C_ACCTBAL > 9990");
	tpchCandidateQueries67.add("SELECT C_NAME, C_ACCTBAL FROM CUSTOMER, NATION WHERE N_NAME='EUROPE' AND C_ACCTBAL>9990");
	// with sample
	tpchCandidateQueries67.add("SELECT S_NAME , S_PHONE FROM SUPPLIER , PART WHERE S_ACCTBAL > 1000 AND 	P_RETAILPRICE > 2096");
	tpchCandidateQueries67.add("SELECT S_NAME, S_PHONE FROM SUPPLIER, PARTSUPP WHERE PS_SUPPKEY=S_SUPPKEY  AND S_ACCTBAL>1000  AND PS_SUPPLYCOST>2096");
	tpchCandidateQueries67.add("SELECT A.S_NAME,A.S_PHONE FROM SUPPLIER AS A,PART AS B WHERE A.S_ACCTBAL>1000 AND B.P_RETAILPRICE>2096;");
	tpchCandidateQueries67.add("select S_NAME, S_PHONE from SUPPLIER  where (S_ACCTBAL > '1000' and   S_SUPPKEY in   (select PS_SUPPKEY from PARTSUPP where PS_PARTKEY in (  select P_PARTKEY from PART where P_RETAILPRICE > '2096')))"); // nested too much...
	tpchCandidateQueries67.add("select S.S_NAME,S.S_PHONE from SUPPLIER S where S.S_ACCTBAL>1000.0 and S.S_SUPPKEY in (select PS.PS_SUPPKEY from PARTSUPP PS,PART P where P.P_PARTKEY=PS.PS_PARTKEY and P.P_RETAILPRICE>2096.0)"); // nested
	tpchCandidateQueries67.add("select S_NAME,S_PHONE from SUPPLIER WHERE S_ACCTBAL>1000 in (select PS_SUPPKEY from PARTSUPP where PS_SUPPLYCOST>2096)"); // nested
	tpchCandidateQueries67.add("SELECT S_NAME,S_PHONE FROM SUPPLIER WHERE S_ACCTBAL >1000");
	tpchCandidateQueries67.add("SELECT S_NAME,S_PHONE FROM SUPPLIER, PARTSUPP WHERE SUPPLIER.S_ACCTBAL > 1000 AND PARTSUPP.PS_SUPPLYCOST > 2096");
	tpchCandidateQueries67.add("SELECT S.S_NAME, S.S_PHONE  FROM SUPPLIER  S, PART P, PARTSUPP PS  WHERE S_ACCTBAL > 1000   AND P.P_PARTKEY = PS.PS_PARTKEY  AND S.S_SUPPKEY = PS.PS_SUPPKEY  AND P_RETAILPRICE > 2096");
	tpchCandidateQueries67.add("SELECT DISTINCT S_NAME, S_PHONE FROM SUPPLIER, PART, PARTSUPP WHERE PS_PARTKEY = P_PARTKEY AND PS_SUPPKEY=S_SUPPKEY AND P_RETAILPRICE>2096");
	String[] blockedRankerIdsTpch67 = {"AG1J7P4XG8BSL", "A1JS6BDUG7WWYP", 
		"A2NHNDD0W2KPR3", "A2I93BY3DHX3MB", "A3IL3HGJW7K6Q1" , 
		"A3988MX4PJCUJW", "A1K50CQRFECIUI", "A1NCIR8ZUDCRJW", 
		"ABR4TOFXCLK8J", "A1GSWHQ1BXWVE7"};
	
	/**
	 * NOTICE: each time, we need to change it manually!
	 */
	
	for (String blockedId : blockedRankerIdsTpch01) {
	    service.blockWorker(blockedId, reason);
	}
	RankingManager.createANewHit(service, "tpch", 0, 1, tpchCandidateQueries01);
    }

}
