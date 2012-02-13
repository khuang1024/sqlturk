package sqlturk.experiment.mturk;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.Test;

import sqlturk.experiment.mturk.QueryInitializer;

public class QueryInitializerTest {

    @Test
    public void testGetAllAnswers() throws IOException {
	ArrayList<String> answers = QueryInitializer
		.getAllAnswers("2F736XBAT2D52HTACBUQ3JG5X4R042");
	for (String answer : answers) {
	    System.out.println("Answer = " + answer);
	}
    }

    @Test
    public void testGetAllHitIds() throws IOException {
	ArrayList<String> hitIds = QueryInitializer.getAllHitIds();
	for (String hitId : hitIds) {
	    System.out.println("hitId = " + hitId);
	}
    }

}
