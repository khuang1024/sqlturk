package sqlturk.mturk;

import sqlturk.configuration.Parameters;

import com.amazonaws.mturk.service.axis.RequesterService;
import com.amazonaws.mturk.util.PropertiesClientConfig;
import com.amazonaws.mturk.requester.HIT;

public class ClearAllMyHits {
    
    private ClearAllMyHits() {
	throw new AssertionError();
    }
    
    public static void main(String[] args) {
	RequesterService service;
	if (Parameters.CURRENT_MODE == Parameters.PRODUCTION_MODE) {
	    service = new RequesterService(new PropertiesClientConfig(
		    Parameters.MTURK_PROPS_FILE_PRODUCTION));
	} else {
	    service = new RequesterService(new PropertiesClientConfig(
		    Parameters.MTURK_PROPS_FILE_SANDBOX));
	}
	System.out.println("Number of active HITs in this account: "
		+ service.getTotalNumHITsInAccount());
	HIT[] allHits = service.searchAllHITs();
	for (HIT hit : allHits) {
	    String hitId = hit.getHITId();
	    service.disableHIT(hitId);
	}
	System.out.println("Number of active HITs in this account: "
		+ service.getTotalNumHITsInAccount());
    }
}
