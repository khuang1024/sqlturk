package sqlturk.experiment.provenance;

import java.util.ArrayList;
import java.util.Hashtable;

import sqlturk.experiment.mturk.SQLTurkHITs;

public class QueryManager {
    private static boolean isInitialized = false;
    private static Hashtable<String, ArrayList<ArrayList<String>>> allQueries = new Hashtable<String, ArrayList<ArrayList<String>>>();
    private static Hashtable<String, ArrayList<String>> standardAnswers = new Hashtable<String, ArrayList<String>>();
    
    private QueryManager() {
	throw new AssertionError();
    }
    
    /**
     * 
     * @param datasetName the name of the dataset, can be either "world" or "tpch"
     * @param queryIndex the index of the query of the dataset, ranging from 0 to 7
     * @return
     */
    public static String getStandardAnswer(String datasetName, int queryIndex) {
	String standardAnswer = "";
	if (!isInitialized) {
	    initializeRankedQueries();
	}
	standardAnswer = standardAnswers.get(datasetName).get(queryIndex);
	return standardAnswer;
    }

    /**
     * 
     * @param datasetName the name of the dataset, can be either "world" or "tpch"
     * @param queryIndex the index of the query of the dataset, ranging from 0 to 7
     * @param answerIndex the indices of the answer queries that extracted, each item ranges from 1 to 10
     * @return
     */
    public static ArrayList<String> getRankedQueries(String datasetName, int queryIndex, int[] answerIndices){
	ArrayList<String> rankedQueries = new ArrayList<String>();
	if (!isInitialized) {
	    initializeRankedQueries();
	}
	for (int i = 0; i < answerIndices.length; i++) {
	    rankedQueries.add(allQueries.get(datasetName).get(queryIndex).get(answerIndices[i]-1));
	}
	return rankedQueries;
    }
    
    /*
     * All arrays start from 0!
     */
    private static void initializeRankedQueries() {
	ArrayList<ArrayList<String>> worldQueries = new ArrayList<ArrayList<String>>();
	ArrayList<ArrayList<String>> tpchQueries = new ArrayList<ArrayList<String>>();

	worldQueries.add(SQLTurkHITs.get("world", 0));
	worldQueries.add(SQLTurkHITs.get("world", 1));
	worldQueries.add(SQLTurkHITs.get("world", 2));
	worldQueries.add(SQLTurkHITs.get("world", 3));
	worldQueries.add(SQLTurkHITs.get("world", 4));
	worldQueries.add(SQLTurkHITs.get("world", 5));
	worldQueries.add(SQLTurkHITs.get("world", 6));
	worldQueries.add(SQLTurkHITs.get("world", 7));
	
	tpchQueries.add(SQLTurkHITs.get("tpch", 0));
	tpchQueries.add(SQLTurkHITs.get("tpch", 1));
	tpchQueries.add(SQLTurkHITs.get("tpch", 2));
	tpchQueries.add(SQLTurkHITs.get("tpch", 3));
	tpchQueries.add(SQLTurkHITs.get("tpch", 4));
	tpchQueries.add(SQLTurkHITs.get("tpch", 5));
	tpchQueries.add(SQLTurkHITs.get("tpch", 6));
	tpchQueries.add(SQLTurkHITs.get("tpch", 7));
	
	allQueries.put("world", worldQueries);
	allQueries.put("tpch", tpchQueries);
	
	ArrayList<String> worldAnswers = new ArrayList<String>();
	worldAnswers.add("select Name as Country_Name from Country where Region ='Western Europe'");
	worldAnswers.add("select CountryCode as CountryLanguage_CountryCode from CountryLanguage where Language='French'");
	worldAnswers.add("select City.Name as City_Name, Country.Name as Country_Name from Country, City Where Country.Capital = City.ID");
	worldAnswers.add("select CountryLanguage.Language as CountryLanguage_Language, Country.Name as Country_Name from Country, CountryLanguage Where Country.Code = CountryLanguage.CountryCode");
	worldAnswers.add("select Country.Name as Country_Name from CountryLanguage, Country where Language='Swedish' AND IsOfficial=true AND Country.Code=CountryLanguage.CountryCode");
	worldAnswers.add("select City.Name as City_Name from City, CountryLanguage where CountryLanguage.Language='Chinese' and City.Population>3000000 and City.CountryCode=CountryLanguage.CountryCode");
	worldAnswers.add("select City.Name as City_Name, Country.Name as Country_Name from Country, CountryLanguage, City Where Country.Code = CountryLanguage.CountryCode and City.ID = Country.Capital and CountryLanguage.Language = 'English' and CountryLanguage.Percentage >=.5");
	
	
	ArrayList<String> tpchAnswers = new ArrayList<String>();
	tpchAnswers.add("select O_CLERK as ORDERS_O_CLERK from ORDERS where O_TOTALPRICE>500000");
	tpchAnswers.add("select S_NAME as SUPPLIER_S_NAME from SUPPLIER where S_ACCTBAL>9960");
	tpchAnswers.add("select N_NAME as NATION_N_NAME, R_NAME as REGION_R_NAME from NATION, REGION where N_REGIONKEY=R_REGIONKEY");
	tpchAnswers.add("select S_NAME as SUPPLIER_S_NAME, N_NAME as NATION_N_NAME from NATION, SUPPLIER where N_NATIONKEY=S_NATIONKEY");
	tpchAnswers.add("select O_ORDERKEY as ORDERS_O_ORDERKEY, O_ORDERSTATUS as ORDERS_O_ORDERSTATUS, O_ORDERPRIORITY as ORDERS_O_ORDERPRIORITY from ORDERS, LINEITEM where L_DISCOUNT=0 and O_TOTALPRICE>530000 and O_ORDERKEY=L_ORDERKEY");
	tpchAnswers.add("select C_NAME as CUSTOMER_C_NAME, C_ACCTBAL as CUSTOMER_C_ACCTBAL from CUSTOMER, NATION, REGION where R_NAME='EUROPE' and C_ACCTBAL>9990 and R_REGIONKEY=N_REGIONKEY and N_NATIONKEY=C_NATIONKEY");
	tpchAnswers.add("select S_NAME as SUPPLIER_S_NAME, S_PHONE as SUPPLIER_S_PHONE from PARTSUPP, PART, SUPPLIER where P_RETAILPRICE>2096 and P_PARTKEY=PS_PARTKEY and PS_SUPPKEY=S_SUPPKEY and S_ACCTBAL>1000");
	
	standardAnswers.put("world", worldAnswers);
	standardAnswers.put("tpch", tpchAnswers);
	
	isInitialized = true;
    }
    
    public static void main(String[] agrs) {
	String dataset = "tpch";
	int queryIndex = 0;
	int[] answerIndices = {4, 2, 6, 8, 7};
	ArrayList<String> top3 = getRankedQueries(dataset, queryIndex, answerIndices);
	for (String q : top3) {
	    System.out.println(q);
	}
	System.out.println();
	System.out.println(getStandardAnswer(dataset, queryIndex));
    }
}
