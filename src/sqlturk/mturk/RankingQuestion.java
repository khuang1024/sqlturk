package sqlturk.mturk;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;

public class RankingQuestion {

    private RankingQuestion() {
	throw new AssertionError();
    }

    private static String question = "";

    static String getQuestion(String datasetName, int questionIndexWithoutSample,
	    int questionIndexWithSample, int execMode, ArrayList<String> candidateQueries) throws IOException {
	question = "<?xml version=\"1.0\"?>\n"; // NOTE: cannot use question +=
	question += "<ExternalQuestion xmlns=\"http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2006-07-14/ExternalQuestion.xsd\">\n";
	question += "<ExternalURL>http://users.soe.ucsc.edu/~khuang/Vote.php?";
	question += "rels=" + getEncodedRelation(datasetName);
	question += "&#x26;nlQuery1="
		+ getEncodedQueryNL(datasetName, questionIndexWithoutSample);
	question += "&#x26;nlQuery2="
		+ getEncodedQueryNL(datasetName, questionIndexWithSample);
	question += "&#x26;execMode=" + execMode;
	question += "&#x26;" + getEncodedCandidateQueries(candidateQueries);
	question += "</ExternalURL>\n";
	question += "<FrameHeight>1300</FrameHeight>\n";
	question += "</ExternalQuestion>";
	return question;
    }
    
    
    static String getEncodedCandidateQueries(ArrayList<String> candidateQueries) throws UnsupportedEncodingException {
	String encodedCandidateQueries = "";
	for (int i = 0; i < candidateQueries.size(); i++) {
	    if (i < candidateQueries.size() - 1) {
		encodedCandidateQueries += "q" + (i+1) + "=" + URLEncoder.encode(candidateQueries.get(i), "UTF-8") + "&#x26;";
	    } else {
		encodedCandidateQueries += "q" + (i+1) + "=" + URLEncoder.encode(candidateQueries.get(i), "UTF-8");
	    }
	}
	return encodedCandidateQueries;
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
