package sqlturk.experiment.mturk;

import java.util.ArrayList;

import sqlturk.configuration.Parameters;
import sqlturk.experiment.mturk.SQLTurkHITs;
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

    @SuppressWarnings("unused")
    public static void main(String[] args) {
	RequesterService service;
	if (Parameters.CURRENT_MODE == Parameters.PRODUCTION_MODE) {
	    service = new RequesterService(new PropertiesClientConfig(
		    Parameters.MTURK_PROPS_FILE_PRODUCTION));
	    System.out.println("Production mode.");
	} else {
	    service = new RequesterService(new PropertiesClientConfig(
		    Parameters.MTURK_PROPS_FILE_SANDBOX));
	    System.out.println("Sandbox mode.");
	}
	String reason = "Sorry, since you were one of the authors writing these queries, you are not allowed to rank these queries.";

	String[] blockedRankerIdsWorld01 = {"A3988MX4PJCUJW", "A355UW5P1BF6M", 
		"A16FY9L7QTDNRW", "A2NHNDD0W2KPR3", "A3IL3HGJW7K6Q1" , 
		"AVRG4735RNH2D", "A1JS6BDUG7WWYP", "A2D3TM156X7Y61", 
		"A1KU2J85SQLP0U", "A3L8RYKHDDHZ10"};
	
	
	String[] blockedRankerIdsWorld23 = {"A355UW5P1BF6M", "A3988MX4PJCUJW", 
		"A16FY9L7QTDNRW", "A3IL3HGJW7K6Q1", "A1JS6BDUG7WWYP" , 
		"A88XXN3BWMTVD", "A1KU2J85SQLP0U", "A2D3TM156X7Y61", 
		"A3VAX2NYGYMTP8", "A3L8RYKHDDHZ10"};
	
	String[] blockedRankerIdsWorld45 = {"A3988MX4PJCUJW", "A355UW5P1BF6M", 
		"A2NHNDD0W2KPR3", "A16FY9L7QTDNRW", "A3IL3HGJW7K6Q1" , 
		"A1JS6BDUG7WWYP", "A1KU2J85SQLP0U", "A3L8RYKHDDHZ10", 
		"A2XSBL2XUZG9JE", "A27Y4PDYXFF976"};
	
	String[] blockedRankerIdsWorld67 = {"A3988MX4PJCUJW", "A355UW5P1BF6M", 
		"A16FY9L7QTDNRW", "A3IL3HGJW7K6Q1", "A2D3TM156X7Y61" , 
		"A1JS6BDUG7WWYP", "A3L8RYKHDDHZ10", "A3VP83YCQLJ618", 
		"A3MUWXFV6C8PPP", "AG1J7P4XG8BSL"};
	
	String[] blockedRankerIdsTpch01 = {"AI2ASXAI9ZFDE", "AG1J7P4XG8BSL", 
		"A1JS6BDUG7WWYP", "A2NHNDD0W2KPR3", "A3IL3HGJW7K6Q1" , 
		"A3988MX4PJCUJW", "A1GSWHQ1BXWVE7", "A3L8RYKHDDHZ10", 
		"A1957F34UKAYHY", "A125ODOYS2ZZDL"};
	
	String[] blockedRankerIdsTpch23 = {"A3MUWXFV6C8PPP", "AG1J7P4XG8BSL", 
		"A1JS6BDUG7WWYP", "A2NHNDD0W2KPR3", "A3IL3HGJW7K6Q1" , 
		"A4HUZZCO72VIN", "A3988MX4PJCUJW", "A1K50CQRFECIUI", 
		"A1NCIR8ZUDCRJW", "A1GSWHQ1BXWVE7"};
	
	String[] blockedRankerIdsTpch45 = {"AG1J7P4XG8BSL", "A1JS6BDUG7WWYP", 
		"A2NHNDD0W2KPR3", "A15A7KUOBMIQVU", "AI2ASXAI9ZFDE" , 
		"A3IL3HGJW7K6Q1", "A3988MX4PJCUJW", "A1GSWHQ1BXWVE7", 
		"A3L8RYKHDDHZ10", "A26Q3BHSFOW110"};
	
	String[] blockedRankerIdsTpch67 = {"AG1J7P4XG8BSL", "A1JS6BDUG7WWYP", 
		"A2NHNDD0W2KPR3", "A2I93BY3DHX3MB", "A3IL3HGJW7K6Q1" , 
		"A3988MX4PJCUJW", "A1K50CQRFECIUI", "A1NCIR8ZUDCRJW", 
		"ABR4TOFXCLK8J", "A1GSWHQ1BXWVE7"};
	
//	for (String blockedId : blockedRankerIdsWorld01) {
//	    service.unblockWorker(blockedId, reason);
//	}
	
//	ArrayList<String> test = SQLTurkHITs.get("tpch", 0);
//	test.addAll(SQLTurkHITs.get("tpch", 7));
//	for (String t:test) {
//	    System.out.println(t);
//	}
//	    System.out.println("\n");
//	for (String t:SQLTurkHITs.get("tpch", 0)) {
//	    System.out.println(t);
//	}
	
	ArrayList<String> world23 = SQLTurkHITs.get("world", 2);
	world23.addAll(SQLTurkHITs.get("world", 3));
	RankingManager.createANewHit(service, "world", 2, 3, world23);
	
	System.out.println("Done.");
    }

}
