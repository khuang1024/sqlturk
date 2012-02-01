package sqlturk.mturk;

import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Ignore;

import sqlturk.configuration.Parameters;

import com.amazonaws.mturk.service.axis.RequesterService;
import com.amazonaws.mturk.util.PropertiesClientConfig;

//import sqlturk.mturk.BasicWorkflow;

public class SQLTurkTaskManagerTest {

    private static RequesterService service;

    // private static Connection dbConn;

    @SuppressWarnings("unused")
    @BeforeClass
    public static void initialize() throws SQLException {
	if (Parameters.CURRENT_MODE == Parameters.PRODUCTION_MODE) {
	    service = new RequesterService(new PropertiesClientConfig(
		    Parameters.MTURK_PROPS_FILE_PRODUCTION));
	} else {
	    service = new RequesterService(new PropertiesClientConfig(
		    Parameters.MTURK_PROPS_FILE_SANDBOX));
	}
    }

    @Ignore
    public void testCreateNewHit() {
	String datasetName = "world";
	int questionQueryIndex = 1;
	AuthoringManager.createANewHit(service, datasetName,
		questionQueryIndex, questionQueryIndex);
    }

    @Ignore
    public void testDownloadHitAnswers() {

    }

    // @Test
    // public void testGetAllHitIds() {
    // ArrayList<String> allHitIds = SQLTurkTaskManager.getAllHitIds(service);
    // for (String hitId : allHitIds) {
    // SQLTurkTaskManager.downloadHitAnswers(service, hitId, dbConn);
    // }
    // }

    // @Test
    // public void testGetAllReviewableHitIds() {
    // ArrayList<String> allReviewableHitIds =
    // SQLTurkTaskManager.getAllReviewableHitIds(service);
    // System.out.println("allReviewableHitIds.size=" +
    // allReviewableHitIds.size());
    // for (String hitId : allReviewableHitIds) {
    // SQLTurkTaskManager.downloadHitAnswers(service, hitId, dbConn);
    // }
    // }

}
