package sqlturk.util.provenance;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import sqlturk.configuration.Parameters;
import sqlturk.util.procedure.ProcedureDeployer;
import sqlturk.util.procedure.ProcedureExecuter;

public class Provenance {
    private Provenance() {
	throw new AssertionError();
    }

    // // materializes the results of the queries
    // // and augments them with the __ROWID attribute
    // private void materializeResults(ArrayList<String> queries, Connection
    // dbConn) {
    // int nextRowID=Parameters.QUERY_RESULT_ROWID_START;
    // int qIndex = 1;
    // for (String query: queries) {
    // String resultRelationName = Parameters.QUERY_RESULT_PREFIX + qIndex +
    // Parameters.QUERY_RESULT_SUFFIX;
    // // materialize the query result
    // DBUtil.materializeQueryResult(dbConn, query, resultRelationName);
    // // augment with __ROWID attribute
    // nextRowID = DBUtil.augmentRelationRowID(dbConn, resultRelationName,
    // nextRowID);
    // qIndex++;
    // }
    // }

    // populates the why-provenance relation, for all the queries
    public static void createWhyProvenanceRelation(ArrayList<String> queries,
	    Connection dbConn) throws SQLException {

	int qIndex = Parameters.QUERY_RESULT_TABLE_INDEX_START;
	Statement stmt = dbConn.createStatement();
	stmt.executeUpdate("DROP TABLE IF EXISTS " + Parameters.PROV_REL_NAME);
	stmt.executeUpdate("CREATE TABLE " + Parameters.PROV_REL_NAME + " ("
		+ Parameters.RESULT_ID_ATT + " INT(11), "
		+ Parameters.DERIVATION_NUMBER_ATT + " INT(11), "
		+ Parameters.SOURCE_NUMBER_ATT + " INT(11))");
	stmt.close();

	for (String query : queries) {
	    StringBuffer provProcedure = generateProvenanceProcedure(query,
		    qIndex);

	    // System.out.println("Provenance tracking procedure: ");
	    // System.out.println(provProcedure);

	    // now we need to deploy the procedure to the database

	    System.out.println("Deploying procedure:");
	    ProcedureDeployer.deployProcedure(dbConn,
		    Parameters.TRACK_PROV_PROCNAME, provProcedure.toString());

	    // execute the newly deployed procedure
	    System.out.println("Executing procedure:");
	    ProcedureExecuter.executeProcedure(dbConn,
		    Parameters.TRACK_PROV_PROCNAME);
	    qIndex++;
	}
    }

    // generates the body of the stored procedure which, after deployment
    // will populate the provenance relation with the why-provenance for this
    // query
    private static StringBuffer generateProvenanceProcedure(String query,
	    int qIndex) {

	/*
	 * First, we need the list of the FROM clause atoms/aliases that we will
	 * use to augment the SELECT clause with ROWIDs. assume the query is an
	 * SPJ Select .... From... Where .
	 */
	String uQuery = query.toUpperCase();

	int fromIndex = uQuery.indexOf("FROM");
	int whereIndex = uQuery.indexOf("WHERE");

	if (whereIndex == -1) {
	    whereIndex = uQuery.length();
	}
	String fromClause = query.substring(fromIndex + 4, whereIndex - 1)
		.trim();
	// System.out.println("From: "+fromClause);
	StringTokenizer st = new StringTokenizer(fromClause, ",");

	// Keep the list of FROM clause atoms
	List<String> fromAtoms = new ArrayList<String>();
	while (st.hasMoreTokens()) {
	    String thisFromAtom = st.nextToken().trim();

	    // System.out.println("Atom:"+thisFromAtom);

	    // bug fixed
	    // bug: didn't consider "as"
	    if (thisFromAtom.contains("as")) {
		StringTokenizer atomTok = new StringTokenizer(thisFromAtom,
			"as");
		String atomAlias = atomTok.nextToken();
		if (atomTok.hasMoreTokens())
		    atomAlias = atomTok.nextToken();
		fromAtoms.add(atomAlias);
	    } else if (thisFromAtom.contains("AS")) {
		StringTokenizer atomTok = new StringTokenizer(thisFromAtom,
			"AS");
		String atomAlias = atomTok.nextToken();
		if (atomTok.hasMoreTokens())
		    atomAlias = atomTok.nextToken();
		fromAtoms.add(atomAlias);
	    } else {
		StringTokenizer atomTok = new StringTokenizer(thisFromAtom, " ");
		String atomAlias = atomTok.nextToken();
		if (atomTok.hasMoreTokens())
		    atomAlias = atomTok.nextToken();
		fromAtoms.add(atomAlias);
	    }

	    // original code from Bogdan
	    // StringTokenizer atomTok = new StringTokenizer(thisFromAtom," ");
	    // String atomAlias = atomTok.nextToken();
	    // if(atomTok.hasMoreTokens())
	    // atomAlias=atomTok.nextToken();
	    // fromAtoms.add(atomAlias);

	}

	// System.out.println("FROM atoms:"+fromAtoms);

	/*
	 * the extension to the SELECT clause which will be used to extract the
	 * ROWIDs of the source tuples (why-provenance)
	 */
	StringBuffer selectExtension = new StringBuffer();
	for (int i = 1; i <= fromAtoms.size(); i++) {
	    String fromAtom = fromAtoms.get(i - 1);
	    selectExtension.append(", " + fromAtom + "."
		    + Parameters.ROWID_ATT_NAME);
	    selectExtension.append(" as src" + i + "_ID");
	}
	selectExtension.append("\n\t\t ");

	// System.out.println("Select extension: "+selectExtension);
	StringBuffer buffer = new StringBuffer();
	// buffer.append("DELIMITER !\n");
	buffer.append("CREATE PROCEDURE " + Parameters.TRACK_PROV_PROCNAME
		+ "()\n");
	buffer.append("BEGIN\n\n");
	buffer.append("\t declare deriv_no int default 0;\n");
	buffer.append("\t declare done int default 0;\n\n");

	// declare res_id_var, src1_id_var, src2_id_var int;
	buffer.append("\t declare res_id_var");
	for (int i = 1; i <= fromAtoms.size(); i++) {
	    buffer.append(", src" + i + "_id_var");
	}
	buffer.append(" int;\n");
	// declare prov_cursor cursor for
	// select __ROWID, src1_ID, src2_ID
	// from Q1_RES q_result
	// natural join
	// (
	// select r1.A as A, r1.__ROWID as src1_ID, r2.__ROWID as src2_ID
	// from R r1, R r2
	// where r1.B=r2.A
	// ) prov_query;
	buffer.append("\t declare prov_cursor cursor for\n");
	buffer.append("\t\t select " + Parameters.ROWID_ATT_NAME);
	for (int i = 1; i <= fromAtoms.size(); i++) {
	    buffer.append(", src" + i + "_ID");
	}
	buffer.append("\n");
	buffer.append("\t\t from " + Parameters.QUERY_RESULT_PREFIX + qIndex
		+ Parameters.QUERY_RESULT_SUFFIX);
	buffer.append(" q_result\n");
	buffer.append("\t\t natural join\n");
	buffer.append("\t\t (\n");
	// the augmented query here:
	StringBuffer queryBuf = new StringBuffer(query);
	queryBuf.insert(fromIndex, selectExtension); // right before the FROM
						     // clause
	buffer.append("\t\t " + queryBuf + "\n");
	buffer.append("\t\t ) prov_query;\n\n");
	buffer.append("\t declare continue handler for not found set done = 1;\n\n");
	buffer.append("\t open prov_cursor;\n\n");
	buffer.append("\t read_loop: loop\n\n");
	buffer.append("\t\t fetch prov_cursor into res_id_var");
	// , src1_id_var, src2_id_var;
	for (int i = 1; i <= fromAtoms.size(); i++) {
	    buffer.append(", src" + i + "_id_var");
	}
	buffer.append(";\n\n");
	buffer.append("\t\t if done then leave read_loop;\n");
	buffer.append("\t\t end if;\n\n");
	for (int i = 1; i <= fromAtoms.size(); i++) {
	    buffer.append("\t\t insert ignore into PROV values (res_id_var,deriv_no,src"
		    + i + "_id_var);\n");
	}
	// insert ignore into PROV values (res_id_var,deriv_no,src1_id_var);
	// insert ignore into PROV values (res_id_var,deriv_no,src2_id_var);
	buffer.append("\n\t\t set deriv_no = deriv_no + 1;\n\n");
	buffer.append("\t end loop;\n\n");
	buffer.append("\t close prov_cursor;\n\n");
	buffer.append("END\n");
	return buffer;
    }
}
