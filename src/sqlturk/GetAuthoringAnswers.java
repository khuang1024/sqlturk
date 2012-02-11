package sqlturk;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import sqlturk.configuration.Parameters;
import sqlturk.mturk.AuthoringManager;

import com.amazonaws.mturk.service.axis.RequesterService;
import com.amazonaws.mturk.util.PropertiesClientConfig;

/**
 * This class is for retrieving the answers for a given HIT.
 * 
 * @author Kerui
 *
 */
public class GetAuthoringAnswers {
    
    private GetAuthoringAnswers() {
	throw new AssertionError();
    }

    @SuppressWarnings("unused")
    public static void main(String[] args) throws SQLException {
	RequesterService service;
	if (Parameters.CURRENT_MODE == Parameters.PRODUCTION_MODE) {
	    service = new RequesterService(new PropertiesClientConfig(Parameters.MTURK_PROPS_FILE_PRODUCTION));
	} else {
	    service = new RequesterService(new PropertiesClientConfig(Parameters.MTURK_PROPS_FILE_SANDBOX));
	}
	
	Connection dbConn;
	if (Parameters.USE_SERVER) {
	    dbConn = DriverManager
		    .getConnection(Parameters.MYSQL_CONNECTION_STRING);
	    System.out.println("Using server.");
	} else {
	    dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL,
		    Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
	    System.out.println("Using local.");
	}
	
	AuthoringManager.downloadHitAnswers(service, "2ZRNZW6HEZ6O8MCMUAZ6DGUN4TQZPT", dbConn);
//	AuthoringManager.forceDownloadHitAnswers(service, "2ZFKO2L92HECQSSNQFFCGF0DS58GFY", dbConn);
//	AuthoringManager.forceDownloadHitAnswers(service, "20R2NJKM05BMDBG7LDTGXP9SD7GBGB", dbConn);
    }
    

}
