package sqlturk;

import sqlturk.configuration.Parameters;
import sqlturk.mturk.AuthoringManager;

import com.amazonaws.mturk.service.axis.RequesterService;
import com.amazonaws.mturk.util.PropertiesClientConfig;

public class GetHitStatus {
    
    private GetHitStatus() {
	throw new AssertionError();
    }

    /**
     * @param args
     */
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
	AuthoringManager.getAllHitStatus(service);
    }

}
