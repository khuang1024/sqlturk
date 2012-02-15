package sqlturk.experiment.provenance;

import java.io.IOException;
import java.sql.SQLException;

public class Experiment {

    /**
     * @param args
     * @throws IOException
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException, IOException {

	String datasetName = args[0];
	int queryIndex = Integer.parseInt(args[1]);
	int topN = 10;
	if (args.length == 3) {
	    topN = Integer.parseInt(args[2]);
	}

	int[] worldQuery0 = { 3, 1, 5, 6, 2, 8, 9, 10, 4, 7 };
	int[] worldQuery1 = { 1, 5, 3, 7, 6, 2, 9, 8, 10, 4 };
	int[] worldQuery2 = { 1, 3, 2, 5, 7, 9, 8, 6, 4, 10 };
	int[] worldQuery3 = { 2, 3, 4, 5, 1, 6, 7, 10, 8, 9 };
	int[] worldQuery4 = { 3, 1, 4, 2, 5, 6, 8, 7, 10, 9 };
	int[] worldQuery5 = { 1, 5, 2, 3, 4, 6, 10, 7, 8, 9 };
	int[] worldQuery6 = { 6, 1, 5, 3, 8, 10, 2, 4, 7, 9 };
	int[] worldQuery7 = { 3, 2, 1, 4, 5, 6, 7, 10, 9, 8 };
	int[] tpchQuery0 = { 4, 2, 6, 8, 7, 9, 5, 1, 3, 10 };
	int[] tpchQuery1 = { 5, 2, 4, 3, 8, 10, 7, 9, 6, 1 };
	int[] tpchQuery2 = { 7, 6, 3, 9, 5, 2, 4, 8, 1, 10 };
	int[] tpchQuery3 = { 4, 2, 5, 3, 7, 6, 1, 8, 9, 10 };
	int[] tpchQuery4 = { 1, 5, 4, 2, 7, 3, 8, 6, 9, 10 };
	int[] tpchQuery5 = { 3, 4, 6, 2, 5, 1, 10, 7, 9, 8 };
	int[] tpchQuery6 = { 1, 3, 5, 2, 4, 6, 10, 8, 7, 9 };
	int[] tpchQuery7 = { 6, 4, 1, 3, 9, 10, 8, 5, 7, 2 };

	int[][] worldCandidates = { worldQuery0, worldQuery1, worldQuery2,
		worldQuery3, worldQuery4, worldQuery5, worldQuery6, worldQuery7 };
	int[][] tpchCandidates = { tpchQuery0, tpchQuery1, tpchQuery2,
		tpchQuery3, tpchQuery4, tpchQuery5, tpchQuery6, tpchQuery7 };

	if (topN != 10) {
	    if (datasetName.equals("world")) {
		SQLTurk.run("world", queryIndex, topN, 10,
			worldCandidates[queryIndex]);

	    } else if (datasetName.equals("tpch")) {

		SQLTurk.run("tpch", queryIndex, topN, 10,
			tpchCandidates[queryIndex]);
	    } else {
		throw new RuntimeException("Invalid dataset name.");
	    }
	} else {
	    if (datasetName.equals("world")) {
		for (int j = 2; j <= 10; j++) { // for each top j
		    SQLTurk.run("world", queryIndex, j, 10,
			    worldCandidates[queryIndex]);
		}
	    } else if (datasetName.equals("tpch")) {
		for (int j = 2; j <= 10; j++) { // for each top j
		    SQLTurk.run("tpch", queryIndex, j, 10,
			    tpchCandidates[queryIndex]);
		}
	    } else {
		throw new RuntimeException("Invalid dataset name.");
	    }
	}

    }
}
