package sqlturk.mturk;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLEncoder;

import sqlturk.configuration.Parameters;

public class AuthoringQuestion {

    private AuthoringQuestion() {
	throw new AssertionError();
    }

    private static String question = "";

    static String getQuestion(String datasetName, int questionIndexWithoutSample,
	    int questionIndexWithSample, int execMode) throws IOException {
	question = "<?xml version=\"1.0\"?>\n"; // NOTE: cannot use question +=
	question += "<ExternalQuestion xmlns=\"http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2006-07-14/ExternalQuestion.xsd\">\n";
	question += "<ExternalURL>http://users.soe.ucsc.edu/~khuang/basicQuestion.php?";
	question += "rels=" + getEncodedRelation(datasetName);
	question += "&#x26;nlQuery1="
		+ getEncodedQueryNL(datasetName, questionIndexWithoutSample);
	question += "&#x26;nlQuery2="
		+ getEncodedQueryNL(datasetName, questionIndexWithSample);
	question += "&#x26;execMode=" + execMode;
	question += "</ExternalURL>\n";
	question += "<FrameHeight>1180</FrameHeight>\n";
	question += "</ExternalQuestion>";
	return question;
    }

    static String getEncodedQueryNL(String datasetName, int questionQueryIndex)
	    throws IOException {
	String queryNL = getQueryNL(datasetName, questionQueryIndex);
	return URLEncoder.encode(queryNL, "UTF-8");
    }

    static String getQueryNL(String datasetName, int questionQueryIndex)
	    throws IOException {
	String nlQuery = "";

	String file = System.getProperty("user.dir") + java.io.File.separator
		+ Parameters.DATASET_PATH_PREFIX + java.io.File.separator
		+ datasetName + java.io.File.separator
		+ Parameters.QUERIES_LIST_FILE;
	BufferedReader br = new BufferedReader(new FileReader(file));
	int index = 0;
	String query = "";
	while ((query = br.readLine()) != null && index < questionQueryIndex) {
	    index++;
	}

	String[] str = query.split(Parameters.QUERY_DATA_DELIM);
	nlQuery = str[1].trim();

	return nlQuery;
    }

    static String getQueryId(String datasetName, int questionQueryIndex)
	    throws IOException {
	String nlQuery = "";

	String file = System.getProperty("user.dir") + java.io.File.separator
		+ Parameters.DATASET_PATH_PREFIX + java.io.File.separator
		+ datasetName + java.io.File.separator
		+ Parameters.QUERIES_LIST_FILE;
	BufferedReader br = new BufferedReader(new FileReader(file));
	int index = 0;
	String query = "";
	while ((query = br.readLine()) != null && index < questionQueryIndex) {
	    index++;
	}

	String[] str = query.split(Parameters.QUERY_DATA_DELIM);
	nlQuery = str[0].trim();

	return nlQuery;
    }

    static String getQueryAnswer(String datasetName, int questionQueryIndex)
	    throws IOException {
	String nlQuery = "";

	String file = System.getProperty("user.dir") + java.io.File.separator
		+ Parameters.DATASET_PATH_PREFIX + java.io.File.separator
		+ datasetName + java.io.File.separator
		+ Parameters.QUERIES_LIST_FILE;
	BufferedReader br = new BufferedReader(new FileReader(file));
	int index = 0;
	String query = "";
	while ((query = br.readLine()) != null && index < questionQueryIndex) {
	    index++;
	}

	String[] str = query.split(Parameters.QUERY_DATA_DELIM);
	nlQuery = str[2].trim();

	return nlQuery;
    }

    static String getEncodedRelation(String datasetName) throws IOException {
	String relationString = "";

	String file = System.getProperty("user.dir") + java.io.File.separator
		+ Parameters.DATASET_PATH_PREFIX + java.io.File.separator
		+ datasetName + java.io.File.separator
		+ Parameters.RELATION_LIST_FILE;
	BufferedReader br = new BufferedReader(new FileReader(file));
	String relation = "";
	while ((relation = br.readLine()) != null) {
	    relationString += relation + " ";
	}
	return URLEncoder.encode(relationString.trim(), "UTF-8");
    }

}
