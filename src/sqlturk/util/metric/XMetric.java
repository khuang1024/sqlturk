package sqlturk.util.metric;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;
import sqlturk.util.common.XCommon;
import sqlturk.util.common.XTuple;
import sqlturk.util.equivalence.XEqui;

public class XMetric {
    
    public static double sim(String rel1, String rel2, Connection dbConn) throws SQLException {
	ArrayList<XTuple> tuples1 = XCommon.getTuples(rel1, dbConn);
	double sum = 0;
	for (XTuple tuple1: tuples1) {
	    sum += sim(tuple1, rel2, dbConn);
	}
	return sum/tuples1.size();
    }
    
    private static double sim(XTuple tuple, String table, Connection dbConn) throws SQLException {
	ArrayList<XTuple> tuples = XCommon.getTuples(table, dbConn);
	double max = 0;
	for (XTuple t : tuples) {
	    double sim = sim(tuple, t);
	    if (max < sim) {
		max = sim;
	    }
	}
	return max;
    }
    
    /**
     * tuple1 is the deninominator.
     * @param tuple1
     * @param tuple2
     * @return
     */
    private static double sim(XTuple tuple1, XTuple tuple2) {
	ArrayList<String> schema1 = tuple1.getSchema();
	ArrayList<String> schema2 = tuple2.getSchema();
	double count = 0;
	for (String column : schema1) {
	    boolean found = false;
	    
	    String value1 = tuple1.getValues().get(schema1.indexOf(column));
	    
	    // search the original column
	    if (schema2.contains(column)) {
		
		String value2 = tuple2.getValues().get(schema2.indexOf(column));
		if (value1.equals(value2)) {
		    found = true;
		}
	    }
	    
	    // if not found, search the equivalent column
	    if (!found) {
		ArrayList<String> equiColumns = XEqui.getEquiColumns(column);
		if (equiColumns != null) { // when this column has equivalent columns
		    
		    // check these equivalent columns on by on
		    for (String equiColumn : equiColumns) {
			if (schema2.contains(equiColumn)) {
				String value2 = tuple2.getValues().get(schema2.indexOf(equiColumn));
				if (value1.equals(value2)) {
				    found = true;
				    break;
				}
			    }
		    }
		}
	    }
	    
	    // check if found
	    if (found) {
		count++;
	    }
	    
	}
	return count/schema1.size();
    }

    /**
     * @param args
     * @throws ClassNotFoundException 
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     * @throws SQLException 
     */
    public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
	Class.forName("com.mysql.jdbc.Driver").newInstance();
	Connection dbConn = null;
	if (Parameters.USE_SERVER) {
	    dbConn = DriverManager.getConnection(Parameters.MYSQL_CONNECTION_STRING);
	    System.out.println("Using server.\n");
	} else {
	    dbConn = DriverManager.getConnection(Parameters.LOCAL_DB_URL, Parameters.LOCAL_USER, Parameters.LOCAL_PASSWORD);
	    System.out.println("Using local.\n");
	}
	
	System.out.println(sim("WORLD_QUERY0_TOP8_FROM10_FD", "WORLD_QUERY0_TOP8_FROM10_FD_PLUS", dbConn));
	System.out.println(sim("WORLD_QUERY0_TOP8_FROM10_FD_PLUS", "WORLD_QUERY0_TOP8_FROM10_FD", dbConn));
    }

}
