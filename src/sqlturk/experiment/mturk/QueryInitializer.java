package sqlturk.experiment.mturk;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;

@Deprecated
public class QueryInitializer {
    
    private QueryInitializer() {
	throw new AssertionError();
    }
    
    static ArrayList<String> getAllAnswers(String hitId) throws IOException {
	ArrayList<String> allAnswers = new ArrayList<String>();
	
	String file = System.getProperty("user.dir") + java.io.File.separator + Parameters.ANSWER_LOG;
	BufferedReader br = new BufferedReader(new FileReader(file));
	String line = "";
	while ((line = br.readLine())!= null) {
	    if (line.contains(hitId)) {
		String[] elements = line.split("&");
		allAnswers.add(elements[1]);
	    }
	}
	return allAnswers;
    }
    
    static ArrayList<String> getAllHitIds() throws IOException{
	ArrayList<String> allHitIds = new ArrayList<String>();
	
	String file = System.getProperty("user.dir") + java.io.File.separator + Parameters.HIT_LOG;
	BufferedReader br = new BufferedReader(new FileReader(file));
	String line = "";
	while ((line = br.readLine())!= null) {
	    String[] elements = line.split("&");
	    for (String element : elements) {
		if (element.contains(Parameters.HIT_LOG_HIT_ID)) {
		    String[] str = element.split("=");
		    allHitIds.add(str[1]);
		}
	    }
	}
	return allHitIds;
    }
}
