package sqlturk.experiment.provenance;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;
import sqlturk.experiment.provenance.QueryManager;
import sqlturk.util.cleaner.Cleaner;
import sqlturk.util.connected.Connected;
import sqlturk.util.equi.Equivalence;
import sqlturk.util.fd.normal.FD;
import sqlturk.util.fd.plus.FDPlus;
import sqlturk.util.intersection.Intersection;
import sqlturk.util.metric.Metric;
import sqlturk.util.provenance.Provenance;
import sqlturk.util.query.QueryExecutor;
import sqlturk.util.tableaux.TableauxRewriter;
import sqlturk.util.union.Union;

/**
 * This class is for testing the whole process of SQLTurk.
 * 
 * 
 */
public class SQLTurk34 {

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
    
    private static String getHeader(String datasetName, int queryIndex, int topN, int[] candidates) {
	String header = datasetName.toUpperCase() + "_QUERY" + queryIndex
		+ "_TOP" + topN + "_FROM" + candidates.length;
	return header;
    }
    
    public static String getFDName(String datasetName, int queryIndex, int topN, int[] candidates) {
	String fdName = getHeader(datasetName, queryIndex, topN, candidates) + "_FD";
	return fdName;
    }
    private static String getFDPlusName(String datasetName, int queryIndex, int topN, int[] candidates) {
	String fdpName = getHeader(datasetName, queryIndex, topN, candidates) + "_FD_PLUS";
	return fdpName;
    }
    
    private static void compute(String type, String datasetName, int queryIndex, int topN, 
	    int[] candidates, int limit) throws SQLException {
	Parameters.FD_REL_NAME = getFDName(datasetName, queryIndex, topN, candidates);
	Parameters.FD_PLUS_REL_NAME = getFDPlusName(datasetName, queryIndex, topN, candidates);
	
	try {
	    Class.forName("com.mysql.jdbc.Driver").newInstance();
	    Connection dbConn;

	    // use soe server or test locally
	    if (Parameters.USE_SERVER) {
		dbConn = DriverManager.getConnection(Parameters.MYSQL_CONNECTION_STRING);
		System.out.println("Using server.");
	    } else {
		dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL, Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
		System.out.println("Using local.");
	    }

	    // create the standard answer result table
	    String standardAnswer = QueryManager.getStandardAnswer(datasetName, queryIndex);
	    Statement stmt = dbConn.createStatement();
	    stmt.executeUpdate("DROP TABLE IF EXISTS " + Parameters.STANDARD_ANSWER_REL_NAME);
	    stmt.executeUpdate("CREATE TABLE " + Parameters.STANDARD_ANSWER_REL_NAME + " AS " + standardAnswer);
	    stmt.close();

	    // initialize the orginal query batch, odered by the their ranks
	    int[] answerIndices = new int[topN];
	    for (int i = 0; i < topN; i++) {
		answerIndices[i] = candidates[i];
	    }
	    ArrayList<String> originalQueries = QueryManager.getRankedQueries(datasetName, queryIndex, answerIndices);

	    System.out.println("standard answer: " + standardAnswer);
	    for (int i = 0; i < originalQueries.size(); i++) {
		System.out.println("candidate query: " + originalQueries.get(i));
	    }

	    // clean old tables
	    System.out.println("Start clearing old tables ......");
	    Cleaner.dropAll(dbConn);
	    System.out.println("Finish clearing old tables.\n");


	    // rewrite queries
	    ArrayList<String> rewriteQueries = new ArrayList<String>();
	    ArrayList<String> tempQueries = new ArrayList<String>();
	    System.out.println("Start rewriting queries ...............");
	    tempQueries = TableauxRewriter.getRewriteQueries(originalQueries, dbConn);
	    for (int i = 0; i < tempQueries.size(); i++) {
		rewriteQueries.add(tempQueries.get(i) + " ORDER BY RAND() LIMIT " + limit);
	    }
	    System.out.println("Finish rewriting queries.\n");


	    // execute the queries
	    System.out.println("Start executing queries ...............");
	    QueryExecutor.executeQueries(rewriteQueries, dbConn);
	    System.out.println("Finish executing queries.\n");
	    
	    
	    Equivalence.init(dbConn);
	    
	    String header = getHeader(datasetName, queryIndex, topN, candidates);
	    if (type.equals("intersection")) {
		System.out.println("Start creating " + Parameters.INTERSECTION_REL_NAME + " .............");
		Intersection.createIntersectionRelation(dbConn);
		System.out.println("Finish creating " + Parameters.INTERSECTION_REL_NAME + ".\n");
		double stdIntersec = Metric.sim(Parameters.STANDARD_ANSWER_REL_NAME, Parameters.INTERSECTION_REL_NAME, dbConn);
		double intersecStd = Metric.sim(Parameters.INTERSECTION_REL_NAME, Parameters.STANDARD_ANSWER_REL_NAME, dbConn);
		String result = header + ", Intersection, Precision=" + intersecStd + ", Recall=" + stdIntersec;
		System.out.println(result);
		appendToLog(result, Parameters.PERFORMANCE_LOG_NAME);
	    } else if (type.equals("union")) {
		System.out.println("Start creating " + Parameters.UNION_REL_NAME + " ............");
		Union.createUionRelation(dbConn);
		System.out.println("Finish creating " + Parameters.UNION_REL_NAME + ".\n");
		double stdUnion = Metric.sim(Parameters.STANDARD_ANSWER_REL_NAME, Parameters.UNION_REL_NAME, dbConn);
		double unionStd = Metric.sim(Parameters.UNION_REL_NAME, Parameters.STANDARD_ANSWER_REL_NAME, dbConn);
		String result = header + ", Union, Precision=" + unionStd + ", Recall=" + stdUnion;
		System.out.println(result);
		appendToLog(result, Parameters.PERFORMANCE_LOG_NAME);
	    } else if (type.equals("fd")) {
		System.out.println("Start creating " + Parameters.FD_REL_NAME);
		FD.createFDRelationNormally(dbConn);
//		FD.createFDRelationOptimally(datasetName, queryIndex, topN, candidates, dbConn);
		System.out.println("Finish creating " + Parameters.FD_REL_NAME + ".\n");
		double stdFD = Metric.sim(Parameters.STANDARD_ANSWER_REL_NAME, Parameters.FD_REL_NAME, dbConn);
		double fdStd = Metric.sim(Parameters.FD_REL_NAME, Parameters.STANDARD_ANSWER_REL_NAME, dbConn);
		String result = header + ", FD, Precision=" + fdStd + ", Recall=" + stdFD;
		System.out.println(result);
		appendToLog(result, Parameters.PERFORMANCE_LOG_NAME);
	    } else if (type.equals("fdplus")) {
		System.out.println("Start creating " + Parameters.PROV_REL_NAME);
		Provenance.createWhyProvenanceRelation(rewriteQueries, dbConn); // create PROV table
		System.out.println("Finish creating " + Parameters.PROV_REL_NAME + ".\n");
		System.out.println("Start creating " + Parameters.CONN_REL_NAME);
		Connected.createWhyConnectedRelation(dbConn); // create CONN table
		System.out.println("Finish creating " + Parameters.CONN_REL_NAME + ".\n");
		System.out.println("Start creating " + Parameters.FD_PLUS_REL_NAME);
		FDPlus.createFDPlusRelation(dbConn);
		System.out.println("Finish creating " + Parameters.FD_PLUS_REL_NAME + ".\n");
		double stdFDPlus = Metric.sim(Parameters.STANDARD_ANSWER_REL_NAME, Parameters.FD_PLUS_REL_NAME, dbConn);
		double fdplusStd = Metric.sim(Parameters.FD_PLUS_REL_NAME, Parameters.STANDARD_ANSWER_REL_NAME, dbConn);
		String result = header + ", FDP_Plus, Precision=" + fdplusStd + ", Recall=" + stdFDPlus;
		System.out.println(result);
		appendToLog(result, Parameters.PERFORMANCE_LOG_NAME);
	    } else {
		throw new RuntimeException("Wrong type.");
	    }
	    
	    System.out.println("Start clearing tables ............");
	    Cleaner.dropAll(dbConn);
	    System.out.println("Finish clearing tables.\n");

	    dbConn.close();
	    System.out.println("Done.");
	    
	} catch (InstantiationException e) {
	    e.printStackTrace();
	} catch (IllegalAccessException e) {
	    e.printStackTrace();
	} catch (ClassNotFoundException e) {
	    e.printStackTrace();
	}
    }


    /**
     * run it for QUERY#{queryIndex} with its candidates
     */
    public static void run(String type, String datasetName, int queryIndex, int topN, int[] candidates, int limit) throws SQLException,
	    IOException {

	System.out.println("SQLTurk is running.\n\n");
	if (!(datasetName.equals("world") || datasetName.equals("tpch"))) {
	    throw new RuntimeException(
		    "Invalid dataset name. Use 'world' or 'tpch'.");
	}
	for (int e : candidates) {
	    if (e < 1 || e > 10) {
		throw new RuntimeException("Invalid candidates. Some candidates are either less than 1 or greater than 10.");
	    }
	}
	
	compute(type, datasetName, queryIndex, topN, candidates, limit);
	
//	for (int topN = 2; topN <= candidates.length; topN++) {
//	    compute(datasetName, queryIndex, topN, candidates, limit);
//	}
    }

}
