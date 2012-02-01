package sqlturk.mturk;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import sqlturk.configuration.Parameters;

import com.amazonaws.mturk.dataschema.QuestionFormAnswers;
import com.amazonaws.mturk.dataschema.QuestionFormAnswersType;
import com.amazonaws.mturk.requester.Assignment;
import com.amazonaws.mturk.requester.AssignmentStatus;
import com.amazonaws.mturk.requester.HIT;
import com.amazonaws.mturk.requester.HITStatus;
import com.amazonaws.mturk.service.axis.RequesterService;
import com.amazonaws.mturk.service.exception.ServiceException;

public class RankingManager {

    private RankingManager() {
	throw new AssertionError();
    }

    /**
     * 
     * @param service
     * @param datasetName
     *            the name of the dataset used here (can be "world", "tpch" or
     *            "store". But here, we don't use "store" dataset.)
     * @param questionQueryIndex
     *            the index of the query in the queries file
     * @param execMode
     *            can be Parameters.SANDBOX_MODE or Parameters.RPODUCTION_MODE
     */
    public static void createANewHit(RequesterService service,
	    String datasetName, int questionIndexWithoutSample,
	    int questionIndexWithSample, ArrayList<String> candidateQueries) {
	HIT hit;
	try {
	    hit = service.createHIT(null, Parameters.RANKING_HIT_TITLE,
		    Parameters.RANKING_HIT_DESCRIPTION, Parameters.RANKING_HIT_KEYWORDS,
		    RankingQuestion.getQuestion(datasetName,
			    questionIndexWithoutSample,
			    questionIndexWithSample, Parameters.CURRENT_MODE, candidateQueries),
		    Parameters.RANKING_HIT_REWARD, Parameters.RANKING_HIT_DURATION,
		    Parameters.RANKING_HIT_AUTOAPPROVE_DELAY, Parameters.RANKING_TASK_LIFETIME,
		    Parameters.RANKING_HIT_ASSIGNMENT_NUM,
		    Parameters.RANKING_HIT_REQUESTERANNOTATION, RankingQualification
			    .createQualReqs(), null // responseGroup
		    );

	    // The following is for log use.
	    String hitId = hit.getHITId();
	    String hitUrl = service.getWebsiteURL() + "/mturk/preview?groupId="
		    + hit.getHITTypeId();
	    String questionInfo1 = "dataset:" + datasetName + ";index:"
		    + questionIndexWithoutSample + ";execMode:"
		    + Parameters.CURRENT_MODE;
	    String questionInfo2 = "dataset:" + datasetName + ";index:"
		    + questionIndexWithSample + ";execMode:"
		    + Parameters.CURRENT_MODE;
	    String content = hitId
		    + "&"
		    + questionInfo1
		    + "&"
		    + questionInfo2
		    + "&"
		    + getFlatArray(candidateQueries, "&") //e.g. q1&q2&...qn&
		    + new Date().toString() + "&" + hitUrl;
	    appendToLog(content, Parameters.RANKING_HIT_LOG);
	    System.out.println(hitId);
	    System.out.println(hitUrl);
	} catch (ServiceException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    /**
     * Download the answers of a HIT to the log file, but the HIT has to be
     * "reviewable".
     * 
     * @param service
     * @param hitId
     * @param dbConn
     */
    public static void downloadHitAnswers(RequesterService service,
	    String hitId, Connection dbConn) {
	HIT hit = service.getHIT(hitId);
	if (hit.getHITStatus() == HITStatus.Reviewable) {
	    downloadAssignmentAnswers(service, hitId, dbConn);
	    // service.disableHIT(hitId);
	} else {
	    System.out.println();
	    System.out.println("HIT " + hitId + " is not reviewable yet.");
	    System.out.println();
	}
    }

    /**
     * Forcely download the answers of a HIT to the log file, even the HIT is
     * not "reviewable".
     * 
     * @param service
     * @param hitId
     * @param dbConn
     */
    public static void forceDownloadHitAnswers(RequesterService service,
	    String hitId, Connection dbConn) {
	downloadAssignmentAnswers(service, hitId, dbConn);
    }

    /**
     * Download the answers of a HIT to the log file, and the HIT does not need
     * to be "reviewable". This function also approve and reject HITs
     * automatically based on our validation program.
     * 
     * @param service
     * @param hitId
     * @param dbConn
     */
    private static void downloadAssignmentAnswers(RequesterService service,
	    String hitId, Connection dbConn) {
	Assignment[] assignments = service.getAllAssignmentsForHIT(hitId,
		new AssignmentStatus[] { AssignmentStatus.Submitted });
	for (Assignment assignment : assignments) {
	    
	    ArrayList<String> rankingAnswers = new ArrayList<String>();

	    String answerXML = assignment.getAnswer();
	    QuestionFormAnswers qfa = RequesterService.parseAnswers(answerXML);
	    @SuppressWarnings("unchecked")
	    List<QuestionFormAnswersType.AnswerType> answers = (List<QuestionFormAnswersType.AnswerType>) qfa
		    .getAnswer();

	    for (QuestionFormAnswersType.AnswerType answer : answers) {
		String questionId = answer.getQuestionIdentifier();
		String questionAnswer = RequesterService.getAnswerValue(
			    assignment.getAssignmentId(), answer);
		
		rankingAnswers.add(questionId + "=" + questionAnswer);
	    }

	    String workerId = assignment.getWorkerId();
	    String content = hitId + 
		    "&" + getFlatArray(rankingAnswers, "&")
		    + workerId + 
		    "&" + assignment.getSubmitTime().getTime().toString();
	    
	    // manually approve for the time being
	    appendToLog(content, Parameters.RANKING_ANSWER_LOG);
	}
    }

    /**
     * Print out all my HITs' status into stdout.
     * 
     * @param service
     */
    public static void getAllHitStatus(RequesterService service) {
	HIT[] allHits = service.searchAllHITs();
	System.out.println("The status of HITs: (hitId, status, "
		+ "# available assignments, " + "# submitted assignments, "
		+ "# completed assignments)");
	for (HIT hit : allHits) {
	    String hitId = hit.getHITId();
	    System.out.println(" " + hitId + ", " + hit.getHITStatus() + ", "
		    + hit.getNumberOfAssignmentsAvailable() + ", "
		    + getNSubmittedAssignments(service, hitId) + ", "
		    + hit.getNumberOfAssignmentsCompleted());
	}
    }

    private static int getNSubmittedAssignments(RequesterService service,
	    String hitId) {
	Assignment[] assignments = service.getAllAssignmentsForHIT(hitId,
		new AssignmentStatus[] { AssignmentStatus.Submitted });
	return assignments.length;
    }

    /**
     * This function is very dangerous, since it will get all the HITs of this
     * account, including the HITs other than SQLTurk HITs.
     * 
     * @param service
     * @return
     */
    @Deprecated
    public static ArrayList<String> getAllHitIds(RequesterService service) {
	ArrayList<String> allHitIds = new ArrayList<String>();
	HIT[] allHits = service.searchAllHITs();
	System.out.println("Number of active HITs in this account: "
		+ allHits.length);
	for (HIT hit : allHits) {
	    allHitIds.add(hit.getHITId());
	    String content = "";
	    content += "HitId=" + hit.getHITId() + "\n";
	    content += "Status=" + hit.getHITStatus().toString() + "\n";
	    content += "#AvailableAssignment="
		    + hit.getNumberOfAssignmentsAvailable() + "\n";
	    content += "#CompletedAssignment="
		    + hit.getNumberOfAssignmentsCompleted() + "\n";
	    content += "#PendingAssignment="
		    + hit.getNumberOfAssignmentsPending() + "\n";
	    System.out.println("debug:\t" + content);
	}

	return allHitIds;
    }

    /**
     * This function is very dangerous, since it will get all the HITs of this
     * account, including the HITs other than SQLTurk HITs.
     * 
     * @param service
     * @return
     */
    @Deprecated
    public static ArrayList<String> getAllReviewableHitIds(
	    RequesterService service) {
	ArrayList<String> allReviewableHitIds = new ArrayList<String>();

	HIT[] allHits = service.searchAllHITs();
	for (HIT hit : allHits) {
	    if (hit.getHITStatus().toString().equals(HITStatus._Reviewable)) {
		allReviewableHitIds.add(hit.getHITId());
		String content = "";
		content += "HitId=" + hit.getHITId() + "\n";
		content += "Status=" + hit.getHITStatus().toString() + "\n";
		content += "#AvailableAssignment="
			+ hit.getNumberOfAssignmentsAvailable() + "\n";
		content += "#CompletedAssignment="
			+ hit.getNumberOfAssignmentsCompleted() + "\n";
		content += "#PendingAssignment="
			+ hit.getNumberOfAssignmentsPending() + "\n";
		System.out.println("debug:\t" + content);
	    }
	}
	return allReviewableHitIds;
    }

    private static void appendToLog(String content, String file) {
	BufferedWriter bw = null;
	try {
	    bw = new BufferedWriter(new FileWriter(file, true));
	    bw.write(content);
	    bw.newLine();
	    bw.flush();
	} catch (IOException e) {
	    e.printStackTrace();
	} finally {
	    try {
		bw.close();
	    } catch (IOException e) {
		e.printStackTrace();
	    }
	}
    }
    
    private static String getFlatArray(ArrayList<String> array, String delimiter) {
	String flatQueries = "";
	for (int i = 0; i < array.size(); i++) {
	    flatQueries += array.get(i) + delimiter;
	}
	return flatQueries;
    }
}
