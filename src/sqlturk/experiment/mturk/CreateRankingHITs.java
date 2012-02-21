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
	
	ArrayList<String> tpch = SQLTurkHITs.get("tpch", 6);
	tpch.addAll(SQLTurkHITs.get("tpch", 7));
	RankingManager.createANewHit(service, "tpch", 6, 7, tpch);
	
	System.out.println("Done.");
    }

}
