package sqlturk.experiment.mturk;

import sqlturk.configuration.Parameters;
import sqlturk.mturk.AuthoringManager;

import com.amazonaws.mturk.service.axis.RequesterService;
import com.amazonaws.mturk.util.PropertiesClientConfig;

/**
 * This class is for deploying tasks on MTurk.
 * 
 * @author Kerui Huang
 * 
 */
public class CreateAuthoringHITs {

    private CreateAuthoringHITs() {
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

	// "world"/"tpch" is the name of the dataset used for this HIT
	// the index starts from 0
	// 0 is the index of question
	// 1 is the index of a similar question to the previous one
//	AuthoringManager.createANewHit(service, "world", 0, 1);
//	AuthoringManager.createANewHit(service, "world", 2, 3);
//	AuthoringManager.createANewHit(service, "world", 4, 5);
//	AuthoringManager.createANewHit(service, "world", 6, 7);
//	AuthoringManager.createANewHit(service, "tpch", 0, 1);
//	AuthoringManager.createANewHit(service, "tpch", 2, 3);
//	AuthoringManager.createANewHit(service, "tpch", 4, 5);
	AuthoringManager.createANewHit(service, "tpch", 6, 7);
    }

}
