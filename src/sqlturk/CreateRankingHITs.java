package sqlturk;

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

	/*
	 * "world"/"tpch" is the name of the dataset used for this HIT
	 * the index starts from 0
	 * 	- 0 is the index of question
	 * 	- 1 is the index of a similar question to the previous one
	 */
	
	/*
	 * The answers to the first HIT.
	 * Dataset: demograpgics/word
	 * Questions: 0, 1
	 */
	ArrayList<String> candidateQueries1 = new ArrayList<String>();
	// without sample
	candidateQueries1.add("select Name from Country where Region like 'Western Europe'");
	candidateQueries1.add("select Name from Country where Region = 'Western Europe'");
	candidateQueries1.add("select Country.name from Country where Country.Region = 'Western Europe'");
	candidateQueries1.add("select Region from Country where Region='Europe'");
	candidateQueries1.add("select Name from Country where Region='Europe'");
	candidateQueries1.add("SELECT * FROM Country WHERE Region='Western Europe'");
	candidateQueries1.add("SELECT Name from Country WHERE Region='Western Europe'");
	candidateQueries1.add("select Country.Name  From Country  Where Region = 'Western Europe'");
	candidateQueries1.add("select Name from Country where Region like 'Western Europe'");
	candidateQueries1.add("select Name from Country where Region='Western Europe'");
	// with sample
	candidateQueries1.add("select CountryCode from CountryLanguage where Language like 'French'");
	candidateQueries1.add("select CountryCode from CountryLanguage where Language = 'French'");
	candidateQueries1.add("Select CountryLanguage.CountryCode from CountryLanguage where CountryLanguage.Language ='French'");
	candidateQueries1.add("select CountryCode from CountryLanguage where Language='French';");
	candidateQueries1.add("select CountryCode from CountryLanguage where Language='French'");
	candidateQueries1.add("SELECT * FROM CountryLanguage WHERE Language='French'");
	candidateQueries1.add("SELECT CountryCode FROM CountryLanguage WHERE Language='French'");
	candidateQueries1.add("select CountryLanguage.CountryCode  From CountryLanguage   where Language = 'French'");
	candidateQueries1.add("select CountryCode from CountryLanguage where Language like 'French'");
	candidateQueries1.add("select CountryCode from CountryLanguage where Language='French'");
	// block the authers
	String[] blockedAuthorIds1 = {"A3988MX4PJCUJW", "A355UW5P1BF6M", 
		"A16FY9L7QTDNRW", "A2NHNDD0W2KPR3", "A3IL3HGJW7K6Q1" , 
		"AVRG4735RNH2D", "A1JS6BDUG7WWYP", "A2D3TM156X7Y61", 
		"A1KU2J85SQLP0U", "A3L8RYKHDDHZ10"};
	String reason = "Sorry, since you were one of the authors writing these queries, you are not allowed to rank these queries.";
	for (String blockedId : blockedAuthorIds1) {
	    service.blockWorker(blockedId, reason);
	}
	
	/**
	 * NOTE: the safe way to run this program is to run it multiple times,
	 * rather than start them at the same time.
	 * The reason is: WE BLOCK SOME AUTHORS!! BUT THEY SHOULDN'T BE BLOCKED
	 * FOR SOME OTHER QUERIES.
	 */
	
	RankingManager.createANewHit(service, "world", 0, 1, candidateQueries1);
    }

}
