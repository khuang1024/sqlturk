package sqlturk.util.equivalence;

import java.util.ArrayList;

public class Equi {
    
    private Equi() {}
    
    private static ArrayList<ArrayList<String>> equi = new ArrayList<ArrayList<String>>();
    
    static {
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
	
	equi.add(code);
	equi.add(partKey);
	equi.add(supplierKey);
	equi.add(customerKey);
	equi.add(nationKey);
	equi.add(regionKey);
	equi.add(orderKey);
    }
    
    public static boolean isEqui(String col1, String col2) {
	for (ArrayList<String> fk : equi) {
	    if (fk.contains(col1) && fk.contains(col2)) {
		return true;
	    }
	}
	return false;
    }
    
}
