package sqlturk.util.tableaux;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;

public class XTableaux {
    
    private ArrayList<String> froms; // ADDITIONAL tables
    private ArrayList<String> wheres; // ADDITIONAL predicates
    private int index;
    
    XTableaux(XParser xParser, Connection dbConn) throws SQLException {
	froms = new ArrayList<String>();
	wheres = new ArrayList<String>();
	index = 0;
	
	ArrayList<String> globalRefTables = new ArrayList<String>();
	
	ArrayList<String> oTables = xParser.getAllOriginalTables();
	for (int i = 0; i < oTables.size(); i++) {
	    
	    String rel = oTables.get(i);
	    XForeignKey xfk = new XForeignKey(rel, dbConn);
	    ArrayList<String> refTables = xfk.getAllRefTables(); // get all the FK referenced tables
	    ArrayList<String> fkEquationStrings = xfk.getFKStrings();
	    
	    //System.out.println(rel + "' Referenced Table:");
	    for (String refTable : refTables) {
		
		//System.out.println("-" + refTable);
		
		if (oTables.contains(refTable)) {
		    if (globalRefTables.contains(refTable)) {
			String newRefTable = getNewRefTable(refTable);
			
			// this.froms.add("original AS alias")
			this.froms.add(refTable + " AS " + newRefTable);
			
			// update fkEquationStrings: replace original refTable with newRefTable
			updateFKEquationStrings(fkEquationStrings, refTable, newRefTable);
		    }
		} else {
		    if (globalRefTables.contains(refTable)) {
			String newRefTable = getNewRefTable(refTable);
			
			// this.froms.add("original AS alias")
			this.froms.add(refTable + " AS " + newRefTable);
			
			// update fkEquationStrings: replace original refTable with newRefTable
			updateFKEquationStrings(fkEquationStrings, refTable, newRefTable);
		    } else {
			this.froms.add(refTable);
		    }
		}
		
		globalRefTables.add(refTable);
		
		// add new referenced table into oTable so that their referenced tables
		// will be taken into account
		if (!oTables.contains(refTable)) {
		    oTables.add(refTable);
		}
		
		//debug
		//System.out.println("debug:\tglobal: " + globalRefTables.toString());
		//System.out.println("debug:\tfkEquation: " + fkEquationStrings.toString());
		//System.out.println("debug:\toTables: " + oTables.toString());
	    }
	    
	    // debug
	    //System.out.println(rel + "'s Tableaux:");
//	    for (String eq : fkEquationStrings) {
//		System.out.println("-" + eq);
//	    }
	    
	    this.wheres.addAll(fkEquationStrings);
	    //System.out.println();
	    
	    
	    
	}
	//System.out.println();
	
    }
    
    private String getNewRefTable(String refTable) {
	String newRefTable = Parameters.TABLEAUX_ALIAS_PREFIX + index;
	index++;
	return newRefTable;
    }
    
    private void updateFKEquationStrings(ArrayList<String> fkEquationStrings, String oRefTable, String newRefTable) {
	for (int i = 0; i < fkEquationStrings.size(); i++) {
	    String equation = fkEquationStrings.get(i);
	    equation = equation.replaceAll("\\s+" + oRefTable + "\\.", " " + newRefTable + ".");
	    equation = equation.replaceAll("\\+" + oRefTable + "\\.", "+" + newRefTable + ".");
	    equation = equation.replaceAll("-" + oRefTable + "\\.", "-" + newRefTable + ".");
	    equation = equation.replaceAll("\\*" + oRefTable + "\\.", "*" + newRefTable + ".");
	    equation = equation.replaceAll("/" + oRefTable + "\\.", "/" + newRefTable + ".");
	    equation = equation.replaceAll("\\(" + oRefTable + "\\.", "(" + newRefTable + ".");
	    equation = equation.replaceAll("\\)" + oRefTable + "\\.", ")" + newRefTable + ".");
	    equation = equation.replaceAll("=" + oRefTable + "\\.", "=" + newRefTable + ".");
	    equation = equation.replaceAll("<" + oRefTable + "\\.", "<" + newRefTable + ".");
	    equation = equation.replaceAll(">" + oRefTable + "\\.", ">" + newRefTable + ".");
	    if (equation.startsWith(oRefTable + ".")) {
		equation = newRefTable + equation.substring(oRefTable.length());
	    }
	    fkEquationStrings.set(i, equation);
	}
    }
    
    
    String getAdditionalFroms(){
	if (this.froms.isEmpty()) {
	    return null;
	}
	
	String additionalFroms = "";
	for (String tb : this.froms) {
	    additionalFroms += " " + tb + ",";
	}
	additionalFroms = additionalFroms.substring(0, additionalFroms.length() - 1);
	return additionalFroms;
    }
    
    String getAdditionalWheres(){
	if (this.wheres.isEmpty()) {
	    return null;
	}
	
	String additionalWheres = "";
	for (String eq : this.wheres) {
	    additionalWheres += " " + eq + " AND";
	}
	additionalWheres = additionalWheres.substring(0, additionalWheres.length() - 3);
	return additionalWheres;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
	

    }

}
