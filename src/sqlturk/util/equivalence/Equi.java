package sqlturk.util.equivalence;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class Equi {
    
    private Connection dbConn;
    private ArrayList<ArrayList<String>> equiColumns = new ArrayList<ArrayList<String>>();
    private ArrayList<ArrayList<String>> equalTables = new ArrayList<ArrayList<String>>();
    
    public Equi(Connection dbConn) throws SQLException {
	this.dbConn = dbConn;
	
	ArrayList<String> code = new ArrayList<String>();
	code.add("Country_Code");
	code.add("CountryLanguage_CountryCode");
	code.add("City_CountryCode");
	
	ArrayList<String> partKey = new ArrayList<String>();
	partKey.add("PART_P_KEY");
	partKey.add("PARTSUPP_PS_PARTKEY");
	partKey.add("LINEITEM_L_PARTKEY");
	
	ArrayList<String> supplierKey = new ArrayList<String>();
	supplierKey.add("SUPPLIER_S_SUPPKEY");
	supplierKey.add("PARTSUPP_PS_SUPPKEY");
	supplierKey.add("LINEITEM_L_SUPPKEY");
	
	ArrayList<String> customerKey = new ArrayList<String>();
	customerKey.add("CUSTOMER_C_CUSTKEY");
	customerKey.add("ORDERS_O_CUSTKEY");
	
	ArrayList<String> nationKey = new ArrayList<String>();
	nationKey.add("NATION_N_NATIONKEY");
	nationKey.add("SUPPLIER_S_NATIONKEY");
	nationKey.add("CUSTOMER_C_NATIONKEY");
	
	ArrayList<String> regionKey = new ArrayList<String>();
	regionKey.add("REGION_R_REGIONKEY");
	regionKey.add("NATION_N_REGIONKEY");
	
	ArrayList<String> orderKey = new ArrayList<String>();
	orderKey.add("ORDERS_O_ORDERKEY");
	orderKey.add("LINEITEM_L_ORDERKEY");
	
	equiColumns.add(code);
	equiColumns.add(partKey);
	equiColumns.add(supplierKey);
	equiColumns.add(customerKey);
	equiColumns.add(nationKey);
	equiColumns.add(regionKey);
	equiColumns.add(orderKey);
	
	ArrayList<String> resultTables = new ArrayList<String>();
	for (String table : resultTables) {
	    boolean found = false;
	    for (ArrayList<String> equiTables : equalTables) {
		if (!equiTables.isEmpty()) {
		    if (equals(table, equiTables.get(0), dbConn)) {
			found = true;
			equiTables.add(table);
		    }
		}
	    }
	    if (!found) {
		ArrayList<String> newEqualTableSet = new ArrayList<String>();
		newEqualTableSet.add(table);
		equalTables.add(newEqualTableSet);
	    }
	}
    }
    
    public boolean equals(String rel1, String rel2) {
	for (ArrayList<String> equalSet : this.equalTables) {
	    if (equalSet.contains(rel1) && equalSet.contains(rel2)) {
		return true;
	    }
	}
	return false;
    }
    
    public static boolean equals(String rel1, String rel2, Connection dbConn) throws SQLException {
	Statement stmt1 = dbConn.createStatement();
	Statement stmt2 = dbConn.createStatement();
	ResultSet rs1 = null;
	ResultSet rs2 = null; 
	ResultSetMetaData rsmd1 = null;
	ResultSetMetaData rsmd2 = null;
	
	ArrayList<String> columnNames = new ArrayList<String>();
	
	// first, check dimensions
	
	// check # row
	rs1 = stmt1.executeQuery("SELECT COUNT(*) FROM " + rel1);
	rs2 = stmt2.executeQuery("SELECT COUNT(*) FROM " + rel2);
	rs1.next();
	rs2.next();
	int nRows1 = rs1.getInt(1);
	int nRows2 = rs2.getInt(1);
	if (nRows1 != nRows2) {
	    rs1.close();
	    rs2.close();
	    stmt1.close();
	    stmt2.close();
	    return false;
	}
	// check # column
	rs1 = stmt1.executeQuery("SELECT * FROM " + rel1);
	rsmd1 = rs1.getMetaData();
	rs2 = stmt2.executeQuery("SELECT * FROM " + rel2);
	rsmd2 = rs2.getMetaData();
	if (rsmd1.getColumnCount() != rsmd2.getColumnCount()) {
	    rs1.close();
	    rs2.close();
	    stmt1.close();
	    stmt2.close();
	    return false;
	}
	
	// second, check name and type of columns
	int nColumns = rsmd1.getColumnCount();
	for (int i = 1; i <= nColumns; i++) {
	    String label = rsmd1.getColumnLabel(i);
	    String type = rsmd1.getColumnTypeName(i);
	    for (int j = 1; j <= nColumns; j++) {
		if (rsmd2.getColumnLabel(j).equals(label)) {
		    if (!rsmd2.getColumnTypeName(j).equals(type)) {
			rs1.close();
			rs2.close();
			stmt1.close();
			stmt2.close();
			return false;
		    } else {
			columnNames.add(label);
			break; // when find the same column
		    }
		}
		if (j == nColumns) {
		    rs1.close();
		    rs2.close();
		    stmt1.close();
		    stmt2.close();
		    return false;
		}
	    }
	}
	
	// values of columns
	String attributes = "";
	for (int i = 0; i < nColumns; i++) {
	    attributes += columnNames.get(i) + ", ";
	}
	attributes = attributes.substring(0, attributes.length() - 2);
	rs1 = stmt1.executeQuery("SELECT COUNT(*) FROM " + rel1 + " INNER JOIN " + rel2 + " USING(" + attributes + ")");
	rs1.next();
	if (rs1.getInt(1) != nRows1) {
	    rs1.close();
	    rs2.close();
	    stmt1.close();
	    stmt2.close();
	    return false;
	} else {
	    rs1.close();
	    rs2.close();
	    stmt1.close();
	    stmt2.close();
	    return true;
	}
    }
    
    public boolean isEqui(String col1, String col2) {
	for (ArrayList<String> fk : equiColumns) {
	    if (fk.contains(col1) && fk.contains(col2)) {
		return true;
	    }
	}
	return false;
    }
    
}
