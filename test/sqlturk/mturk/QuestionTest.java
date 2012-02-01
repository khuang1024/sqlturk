package sqlturk.mturk;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

public class QuestionTest {

    @Test
    public void testGetQuestion() throws IOException {
	System.out.println(AuthoringQuestion.getQuestion("world", 0, 1, 0));
    }

    @Test
    public void testGetEncodedQueryNL() throws IOException {
	assertEquals(
		"List+the+names+of+all+cities+having+a+population+above+1+million.",
		AuthoringQuestion.getEncodedQueryNL("world", 0));
    }

    @Test
    public void testGetQueryNL() throws IOException {
	assertEquals(
		"List the names of all cities having a population above 1 million.",
		AuthoringQuestion.getQueryNL("world", 0));
	assertEquals(
		"List the city with the highest population in each country where the government is Republic and the life expectancy is below 60.",
		AuthoringQuestion.getQueryNL("world", 6));
    }

    @Test
    public void testGetQueryId() throws IOException {
	assertEquals("Q0", AuthoringQuestion.getQueryId("world", 0));
	assertEquals("Q6", AuthoringQuestion.getQueryId("world", 6));
    }

    @Test
    public void testGetQueryAnswer() throws IOException {
	assertEquals("select Name from City where Population > 1000000",
		AuthoringQuestion.getQueryAnswer("world", 0));
	assertEquals(
		"select ci.Name from Country c, City ci Where c.GovernmentForm=\"Republic\" AND c.LifeExpectancy < 60 AND c.Code = ci.CountryCode GROUP BY c.Name",
		AuthoringQuestion.getQueryAnswer("world", 6));
    }

    @Test
    public void testGetRelationString() throws IOException {
	assertEquals("City+Country+CountryLanguage",
		AuthoringQuestion.getEncodedRelation("world"));
	assertEquals("Customers+Employees+Offices+OrderDetails+Orders",
		AuthoringQuestion.getEncodedRelation("store"));
    }

}
