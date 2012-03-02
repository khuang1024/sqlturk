package sqlturk.experiment.provenance;

import java.io.IOException;
import java.sql.SQLException;

public class Experiment3 {

    /**
     * @param args
     * @throws IOException
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException, IOException {
	String type = args[0];
	String datasetName = args[1];
	int queryIndex = Integer.parseInt(args[2]);
	int topN = Integer.parseInt(args[3]);
	int limit = Integer.parseInt(args[4]);
	
	// validate input
	if (!datasetName.equals("world") && !datasetName.equals("tpch")) {
	    throw new RuntimeException("args[0]: Wrong dataset name.");
	}
	if (queryIndex < 0 || queryIndex > 7) {
	    throw new RuntimeException("args[1]: Wrong queryIndex.");
	}
	if (topN < 2 || topN > 10) {
	    throw new RuntimeException("args[2]: Wrong topN.");
	}

	int[] worldQuery0 = { 3, 1, 5, 6, 2, 8, 9, 10, 4, 7 };
	int[] worldQuery1 = { 1, 5, 3, 7, 6, 2, 9, 8, 10, 4 };
	
	int[] worldQuery2 = { 2, 4, 5, 1, 6, 9, 7, 10, 3, 8 };
	int[] worldQuery3 = { 3, 8, 4, 7, 5, 1, 10, 6, 2, 9 };
	
	int[] worldQuery4 = { 5, 8, 2, 7, 3, 6, 4, 10, 9, 1 };
	int[] worldQuery5 = { 2, 5, 6, 3, 7, 10, 9, 4, 8, 1 };
	
	int[] worldQuery6 = { 1, 5, 9, 7, 10, 2, 6, 8, 3, 4 };
	int[] worldQuery7 = { 5, 1, 2, 9, 7, 6, 10, 3, 8, 4 };
	
	int[] tpchQuery0 = { 4, 2, 6, 8, 7, 9, 5, 1, 3, 10 };
	int[] tpchQuery1 = { 5, 2, 4, 3, 8, 10, 7, 9, 6, 1 };
	
	int[] tpchQuery2 = { 7, 6, 3, 9, 5, 2, 4, 8, 1, 10 };
	int[] tpchQuery3 = { 4, 2, 5, 3, 7, 6, 1, 8, 9, 10 };
	
	int[] tpchQuery4 = { 7, 1, 10, 5, 8, 4, 9, 3, 2, 6 };
	int[] tpchQuery5 = { 9, 10, 8, 6, 2, 7, 5, 4, 1, 3 };
	
	int[] tpchQuery6 = { 2, 6, 5, 8, 3, 1, 4, 9, 10, 7 };
	int[] tpchQuery7 = { 5, 3, 8, 4, 1, 2, 7, 10, 9, 6 };

	int[][] worldCandidates = { worldQuery0, worldQuery1, worldQuery2,
		worldQuery3, worldQuery4, worldQuery5, worldQuery6, worldQuery7 };
	int[][] tpchCandidates = { tpchQuery0, tpchQuery1, tpchQuery2,
		tpchQuery3, tpchQuery4, tpchQuery5, tpchQuery6, tpchQuery7 };
	
	if (datasetName.equals("world")) {
	    SQLTurk34.run(type, datasetName, queryIndex, topN, worldCandidates[queryIndex], limit);
	} else {
	    SQLTurk34.run(type, datasetName, queryIndex, topN, tpchCandidates[queryIndex], limit);
	}
    }
}
