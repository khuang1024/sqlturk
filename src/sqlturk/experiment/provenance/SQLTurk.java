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
public class SQLTurk {
    
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

    /**
     * @param args
     * @throws SQLException 
     * @throws IOException 
     */
    public static void main(String[] args) throws SQLException, IOException {
	
	String datasetName = args[0]; // "world" or "tpch"
	int queryIndex = Integer.parseInt(args[1]); // 0 - 7
	int topN = Integer.parseInt(args[2]); // top n
	int nCandidates = Integer.parseInt(args[3]); // the size of the candidate pool
	int[] candidates = new int[args.length - 4];
	System.out.println(args.length);
	for (int i = 4; i < args.length; i++) {
	    candidates[i-4] = Integer.parseInt(args[i]);
	}
	
	// check the input
	if (nCandidates != candidates.length) {
	    throw new RuntimeException("Number of candidates does not match the real size of candidates.");
	}
	if (topN > nCandidates) {
	    throw new RuntimeException("N (in Top N) is greater than the size of candidates.");
	}
	if (!(datasetName.equals("world")
		|| datasetName.equals("tpch"))) {
	    throw new RuntimeException("Invalid dataset name. Use 'world' or 'tpch'.");
	}
	if (queryIndex < 0 || queryIndex > 7) {
	    throw new RuntimeException("Invalid query index. Query index must be between 0 and 7.");
	}
	for (int e : candidates) {
	    if (e < 1 || e > 10) {
		throw new RuntimeException("Invalid candidates. Some candidates are either less than 1 or greater than 10.");
	    }
	}
	
	// rename the relation names
	String header = datasetName.toUpperCase() + "_QUERY" + queryIndex + "_TOP" + topN + "_FROM" + nCandidates;
	Parameters.UNION_REL_NAME = header + "_UNION";
	System.out.println(Parameters.UNION_REL_NAME);
	Parameters.INTERSECTION_REL_NAME = header + "_INTERSECTION";
	Parameters.FD_REL_NAME = header + "_FD";
	Parameters.FD_PLUS_REL_NAME = header + "_FD_PLUS";
	
	try {
	    Class.forName("com.mysql.jdbc.Driver").newInstance();
	    Connection dbConn;
	    
	    /*
	     * use soe server or test locally
	     */
	    if (Parameters.USE_SERVER) {
		dbConn = DriverManager.getConnection(Parameters.MYSQL_CONNECTION_STRING);
		System.out.println("Using server.");
	    } else {
		dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL, Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
		System.out.println("Using local.");
	    }
	    
	    /*
	     * create the standard answer result table
	     */
	    String standardAnswer = QueryManager.getStandardAnswer(datasetName, queryIndex);
	    Statement stmt = dbConn.createStatement();
	    stmt.executeUpdate("DROP TABLE IF EXISTS " + Parameters.STANDARD_ANSWER_REL_NAME);
	    stmt.executeUpdate("CREATE TABLE " + Parameters.STANDARD_ANSWER_REL_NAME + " AS " + standardAnswer);
	    stmt.close();
	    
	    
	    /*
	     * initialize the orginal query batch, odered by the their ranks
	     */
	    int[] answerIndices = new int[topN];
	    for (int i = 0; i < topN; i++) {
		answerIndices[i] = candidates[i];
	    }
	    ArrayList<String> originalQueries = QueryManager.getRankedQueries(datasetName, queryIndex, answerIndices);
	    

	    System.out.println("standard answer: " + standardAnswer);
	    for (int i = 0; i < originalQueries.size(); i++) {
		System.out.println("candidate query: " + originalQueries.get(i));
	    }
	    
	    
	    /*
	     * clean old intermediate tables
	     */
	    System.out.println("Start clearing old intermediate and result tables ......");
	    Cleaner.dropAll(dbConn);
	    System.out.println("Finish clearing old intermediate and result tables.\n");
	    
	    /*
	     * rewrite the queries
	     */
	    ArrayList<String> rewriteQueries = new ArrayList<String>();
	    System.out.println("Start rewriting queries ...............");
	    rewriteQueries = TableauxRewriter.getRewriteQueries(originalQueries, dbConn);
	    System.out.println("Finish rewriting queries.\n");
	    
	    /*
	     * execute the queries
	     */
	    System.out.println("Start executing queries ...............");
	    QueryExecutor.executeQueries(rewriteQueries, dbConn);
	    System.out.println("Finish executing queries.\n");
	    
	    /*
	     * create PROV table
	     */
	    System.out.println("Start creating " + Parameters.PROV_REL_NAME + " ........");
	    Provenance.createWhyProvenanceRelation(rewriteQueries, dbConn);
	    System.out.println("Finish creating " + Parameters.PROV_REL_NAME + ".\n");
	    
	    /*
	     * create CONN table
	     */
	    System.out.println("Start creating " + Parameters.CONN_REL_NAME + " ..................");
	    Connected.createWhyConnectedRelation(dbConn);
	    System.out.println("Finish creating " + Parameters.CONN_REL_NAME + ".\n");
	    
	    /*
	     * create FD
	     */
	    System.out.println("Start creating " + Parameters.FD_REL_NAME + " ...............");
	    FD.createFDRelation(dbConn); // rename the table like: WORLD_0_TOP5_FD
	    System.out.println("Finish creating " + Parameters.FD_REL_NAME + ".\n");
	    
	    /*
	     * create FD+
	     */
	    System.out.println("Start creating " + Parameters.FD_PLUS_REL_NAME + " .............");
	    FDPlus.createFDPlusRelation(dbConn); // rename the table like: WORLD_0_TOP5_FD_PLUS
	    System.out.println("Finish creating " + Parameters.FD_PLUS_REL_NAME + ".\n");
	    
	    /*
	     * create intersection
	     */
	    System.out.println("Start creating " + Parameters.INTERSECTION_REL_NAME + " .............");
	    Intersection.createIntersectionRelation(dbConn);
	    System.out.println("Finish creating " + Parameters.INTERSECTION_REL_NAME + ".\n");
	    
	    /*
	     * create union
	     */
	    System.out.println("Start creating " + Parameters.UNION_REL_NAME + " ............");
	    Union.createUionRelation(dbConn);
	    System.out.println("Finish creating " + Parameters.UNION_REL_NAME + ".\n");
	    
	    /*
	     * metric
	     */
	    System.out.println("Start evaluating performance by metric ........");
	    double stdIntersec = Metric.sim(Parameters.STANDARD_ANSWER_REL_NAME, Parameters.INTERSECTION_REL_NAME, dbConn);
	    double stdUnion = Metric.sim(Parameters.STANDARD_ANSWER_REL_NAME, Parameters.INTERSECTION_REL_NAME, dbConn);
	    double stdFD = Metric.sim(Parameters.STANDARD_ANSWER_REL_NAME, Parameters.FD_REL_NAME, dbConn);
	    double stdFDPlus = Metric.sim(Parameters.STANDARD_ANSWER_REL_NAME, Parameters.FD_PLUS_REL_NAME, dbConn);
	    double intersecStd = Metric.sim(Parameters.INTERSECTION_REL_NAME, Parameters.STANDARD_ANSWER_REL_NAME, dbConn);
	    double unionStd = Metric.sim(Parameters.INTERSECTION_REL_NAME, Parameters.STANDARD_ANSWER_REL_NAME, dbConn);
	    double fdStd = Metric.sim(Parameters.FD_REL_NAME, Parameters.STANDARD_ANSWER_REL_NAME, dbConn);
	    double fdplusStd = Metric.sim(Parameters.FD_PLUS_REL_NAME, Parameters.STANDARD_ANSWER_REL_NAME, dbConn);
	    appendToLog(header, Parameters.PERFORMANCE_LOG_NAME);
	    appendToLog("Intersection\tPrecision\t" + intersecStd + "\tRecall\t" + stdIntersec, Parameters.PERFORMANCE_LOG_NAME);
	    appendToLog("Union\tPrecision\t" + unionStd + "\tRecall\t" + stdUnion, Parameters.PERFORMANCE_LOG_NAME);
	    appendToLog("FD\tPrecision\t" + fdStd + "\tRecall\t" + stdFD, Parameters.PERFORMANCE_LOG_NAME);
	    appendToLog("FDP_Plus\tPrecision\t" + fdplusStd + "\tRecall\t" + stdFDPlus, Parameters.PERFORMANCE_LOG_NAME);
	    System.out.println("Finish evaluating performance by metric ........");
	    
	    /*
	     * clear intermediate tables
	     */
	    System.out.println("Start clearing intermediate tables ............");
	    Cleaner.dropIntermediateTables(dbConn);
	    System.out.println("Finish clearing intermediate tables.\n");

	    System.out.println("Done.");
	} catch (InstantiationException e) {
	    e.printStackTrace();
	} catch (IllegalAccessException e) {
	    e.printStackTrace();
	} catch (ClassNotFoundException e) {
	    e.printStackTrace();
	}
	
	
	// rollback the name
	Parameters.UNION_REL_NAME = "UNION";
	Parameters.INTERSECTION_REL_NAME = "INTERSECTION";
	Parameters.FD_REL_NAME = "FD";
	Parameters.FD_PLUS_REL_NAME = "FD_PLUS";

    }

}
