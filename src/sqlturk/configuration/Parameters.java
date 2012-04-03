package sqlturk.configuration;

public class Parameters {
    
    // database connection
    public final static String DB_HOST = "mysql.cse.ucsc.edu";
    public final static String DB_USER = "abogdan";
    public final static String DB_PASS = "FJyJqqJs";
    public final static String DB_NAME = "abogdan";
    public final static String MYSQL_CONNECTION_STRING="jdbc:mysql://mysql.cse.ucsc.edu/abogdan?user=abogdan&password=FJyJqqJs";
    
    // local testing database information
    public static final boolean USE_SERVER = true;
    public final static String LOCAL_DB_URL = "jdbc:mysql://127.0.0.1:3306/abogdan";
    public final static String LOCAL_USER = "root";
    public final static String LOCAL_PASSWORD = "1";
    
    // foreign key constraint table
    public final static String FK_CONSTRAINT_TABLE_PREFIX = "";
    public final static String FK_CONSTRAINT_TABLE_SUFFIX = "_FK";
    public final static String FK_CONSTRAINT_COLUMN_TABLE_NAME = "TABLE_NAME";
    public final static String FK_CONSTRAINT_COLUMN_COLUMN_NAME = "COLUMN_NAME";
    public final static String FK_CONSTRAINT_COLUMN_REFERENCED_TABLE_SCHEMA = "REFERENCED_TABLE_SCHEMA";
    public final static String FK_CONSTRAINT_COLUMN_REFERENCED_TABLE_NAME = "REFERENCED_TABLE_NAME";
    public final static String FK_CONSTRAINT_COLUMN_REFERENCED_COLUMN_NAME = "REFERENCED_COLUMN_NAME";
    
    // sample queries
    public static final String QUERY_1 = "select firstName, lastName from Employees, Offices where Employees.officeCode=Offices.officeCode and city='Sydney'";
    public static final String QUERY_2 = "select customerName from Employees, Customers where Customers.salesRepEmployeeNumber=Employees.employeeNumber and Employees.lastName='Fixter'";
    public static final String QUERY_3 = "select orderNumber, sum(quantityOrdered * priceEach) from OrderDetails group by orderNumber";
    public static final String QUERY_4 = "select customerName, productName from Customers, Products, Orders, OrderDetails where Products.productCode=OrderDetails.productCode and OrderDetails.orderNumber=Orders.orderNumber and Orders.customerNumber=Customers.customerNumber group by (customerName) having count(*)>50";
    public static final String QUERY_5 = "select distinct Offices.officeCode from Customers, Employees, Offices where Customers.country='USA' and Customers.salesRepEmployeeNumber=Employees.employeeNumber and Employees.officeCode=Offices.officeCode and Offices.country='USA'";
    public static final String QUERY_6 = "select distinct Products.productCode, productName from OrderDetails, Products where OrderDetails.priceEach<31 and OrderDetails.productCode=Products.productCode";
    public static final String QUERY_7 = "select Name, Population from Country where Continent='Asia' and Population>100000000";
    public static final String QUERY_8 = "select Name from Country where Region =’Western Europe’";
    public static final String QUERY_9 = "select Country.Name from CountryLanguage, Country where Language=’Spanish’ AND IsOfficial=true AND Country.Code=CountryLanguage.CountryCode;";
    public static final String QUERY_10 = "select City.Name, Country.Name  from Country, CountryLanguage, City Where Country.Code = CountryLanguage.CountryCode and City.ID = Country.Capital and CountryLanguage.Language = 'English' and CountryLanguage.Percentage >=.5";
    public static final String QUERY_11 = "select City.Name, LifeExpectancy from City, Country where LifeExpectancy>81 and City.CountryCode=Country.Code";
    public static final String QUERY_12 = "select ci.Name from Country c, City ci where c.GovernmentForm='Republic' and c.LifeExpectancy < 60 and c.Code = ci.CountryCode group by c.Name";
    public static final String QUERY_13 = "select customerName as cName, productName pName from Customers as Cus, Products, Orders, OrderDetails where Products.productCode=OrderDetails.productCode and OrderDetails.orderNumber=Orders.orderNumber and Orders.customerNumber=Cus.customerNumber group by (customerName) having count(*)>50";
    
    // data
    public final static String DATASET_PATH_PREFIX = "data"; // the path where the relation names and questions are stored
    public final static String RELATION_LIST_FILE = "relations.txt";
    public final static String QUERIES_LIST_FILE = "queries.txt";
    public final static String EQUIV_LIST_FILE = "equiv.txt";
    public final static String QUERY_DATA_DELIM = "#";
    
    // mturk authoring task configuration
    public static final String HIT_TITLE = "Write a simple SQL query for a research project";
    public static final String HIT_DESCRIPTION = "You have to write a simple SQL query from a short sentence in English.";
    public static final String HIT_KEYWORDS = "SQL, query, answer, database, question, translate, research";
    public static final String HIT_REQUESTERANNOTATION = "SQL#query";
    public static final double HIT_REWARD = 0.05; // 5 cents
    public static final long HIT_DURATION = 30 * 60; // 1 hr
    public static final long HIT_AUTOAPPROVE_DELAY = 60 * 60 * 24 * 15; // 15 days
    public final static long TASK_LIFETIME = 60 * 60 * 24 * 7; // 1 week
    public static final int HIT_ASSIGNMENT_NUM = 3; // the number of assignments per HIT
    
    // mturk authoring log configuration
    public static final String HIT_LOG_RIGHT_ANSWER = "RIGHT_ANSWER";
    public static final String HIT_LOG_HIT_ID = "HIT_ID";
    public static final String HIT_LOG_QUESTION_INFO = "QUESTION_INFO";
    public static final String HIT_LOG_TIME_STAMP = "TIME_STAMP";
    public static final String HIT_LOG_HIT_URL = "HIT_URL";
    public static final String HIT_LOG = "hit_log.csv";
    public static final String ANSWER_LOG_HIT_ID = "HIT_ID";
    public static final String ANSWER_LOG_TIME_STAMP = "TIME_STAMP";
    public static final String ANSWER_LOG_WORKER_ID = "WORKER_ID";
    public static final String ANSWER_LOG_ANSWER_SQL = "ANSWER_SQL";
    public static final String ANSWER_LOG = "answer_log.csv";
    public static final String REJECT_LOG = "reject_log.csv";
    
    // mturk authoring task configuration
    public static final String RANKING_HIT_TITLE = "Rank queries based on the need and schema";
    public static final String RANKING_HIT_DESCRIPTION = "Your task is to rank some queries based on the need which is given as an English statement and the related schema.";
    public static final String RANKING_HIT_KEYWORDS = "SQL, query, answer, database, question, research";
    public static final String RANKING_HIT_REQUESTERANNOTATION = "SQL#query";
    public static final double RANKING_HIT_REWARD = 0.05; // 5 cents
    public static final long RANKING_HIT_DURATION = 6 * 60 * 60; // 6 hr
    public static final long RANKING_HIT_AUTOAPPROVE_DELAY = 60 * 60 * 24 * 15; // 15 days
    public final static long RANKING_TASK_LIFETIME = 60 * 60 * 24 * 7; // 1 week
    public static final int RANKING_HIT_ASSIGNMENT_NUM = 10; // the number of assignments per HIT
    
    // mturk ranking log configuration
    public static final String RANKING_HIT_LOG = "ranking_hit_log.csv";
    public static final String RANKING_ANSWER_LOG = "ranking_answer_log.csv";
    public static final String RANKING_REJECT_LOG = "ranking_reject_log.csv";
    
    // mturk requester configuration
    public final static int MIN_APPROVAL_RATE = 75;
    public final static boolean IS_USE_MASTER = false; // not used yet
    public final static String MTURK_PROPS_FILE_PRODUCTION = "mturk_production.properties";
    public final static String MTURK_PROPS_FILE_SANDBOX = "mturk_sandbox.properties";
    public final static int SANDBOX_MODE = 0;
    public final static int PRODUCTION_MODE = 1;
    public final static int CURRENT_MODE = PRODUCTION_MODE;
    
    // ROWID (These parameters MUST be consistent with the REAL data in database?)    
    public final static String ROWID_ATT_NAME = "__ROWID";
    public final static int ROWID_SRC_RELATION_START = 0;
    public final static int ROWID_QUERY_RESULT_START = 2000000000;
    public final static int ROWID_TUPLE_INCREMENT = 2;
    
    //
    public static int SRC_RELATION_ROWID_CURRENT = ROWID_SRC_RELATION_START;
    public final static int RESULT_RELATION_ROWID_CURRENT = ROWID_QUERY_RESULT_START;
	            
    // result table
    public final static String QUERY_RESULT_PREFIX = "Q";
    public final static int QUERY_RESULT_TABLE_INDEX_START = 0;
    public final static String QUERY_RESULT_SUFFIX = "_RES";
    
    public final static String QUERY_RESULT_REWRITE_PREFIX = "";
    public final static String QUERY_RESULT_REWRITE_SUFFIX = "_REWRITE";
    
    
    // why-provenance
    public final static String PROV_REL_NAME = "PROV";
    public final static String RESULT_ID_ATT = ROWID_ATT_NAME;
    public final static String DERIVATION_NUMBER_ATT = "DERIV";
    public final static String SOURCE_NUMBER_ATT = "SRC_ID";
    
    public final static String TRACK_PROV_PROCNAME="track_provenance";
    
    // why-connected
    public final static String CONN_REL_NAME = "CONN";
    public final static String CONN_TUPLE1_ID_ATT = "TUPLE1__ROWID";
    public final static String CONN_TUPLE2_ID_ATT = "TUPLE2__ROWID";
    
    // tableaux
    public static final String TABLEAUX_ALIAS_PREFIX = "Tableaux_";
    public static final int TABLEAUX_ALIAS_START_ID = 0;
    public static final String TABLEAUX_ALIAS_SUFFIX = "";
    
    // FD and FD+
    public static String FD_REL_NAME = "FD";
    public static String FD_PLUS_REL_NAME = "FD_PLUS";
    public static final String FD_PLUS_SCORE_REL_NAME = "FD_PLUS_SCORE_TABLE";
    
    // FD temp table and FD+ temp table
    public static final String FD_TEMP_TABLE_PREFIX = "FD_TEMP_";
    public static final String FD_TEMP_TABLE_SUFFIX = "";
    public static final String FD_PLUS_TEMP_TABLE_PREFIX = "FD_PLUS_TEMP_";
    public static final String FD_PLUS_TEMP_TABLE_SUFFIX = "";
    
    // select alias info table
    public static final String SELECT_ALIAS_REL_NAME = "SELECT_ALIAS";
    public static final String SELECT_ALIAS_RESULT_TABLE = "RESULT_TABLE";
    public static final String SELECT_ALIAS_COLUMN_REAL = "COL_REAL";
    public static final String SELECT_ALIAS_COLUMN_ALIAS = "COL_ALIAS";
    
    // from alias info table
    public static final String FROM_ALIAS_REL_NAME = "FROM_ALIAS";
    public static final String FROM_ALIAS_REL_TALBE = SELECT_ALIAS_RESULT_TABLE;
    public static final String FROM_ALIAS_TABLE_REAL = "TABLE_REAL";
    public static final String FROM_ALIAS_TABLE_ALIAS = "TABLE_ALIAS";

    // source info table
    public static final String SOURCE_MAP_REL_NAME = "SOURCE_MAP";
    public static final String SOURCE_MAP_RESULT_TABLE = SELECT_ALIAS_RESULT_TABLE;
    public static final String SOURCE_MAP_COLUMN_ALIAS = SELECT_ALIAS_COLUMN_ALIAS;
    public static final String SOURCE_MAP_TABLE_ALIAS = FROM_ALIAS_TABLE_ALIAS;

    // column info table
    public static final String COLUMN_INFO_REL_NAME = "COLUMN_INFO";
    
    // intersection
    public static String INTERSECTION_REL_NAME = "INTERSECTION_TABLE";
    
    // union
    public static String UNION_REL_NAME = "UNION_TABLE";
    
    // standard answer table
    public static final String STANDARD_ANSWER_REL_NAME = "STANDARD_TABLE";
    
    // relations in dataset
    public static final String REAL_RELATION_0 = "City";
    public static final String REAL_RELATION_1 = "Country";
    public static final String REAL_RELATION_2 = "CountryLanguage";
    public static final String REAL_RELATION_3 = "Customers";
    public static final String REAL_RELATION_4 = "Employees";
    public static final String REAL_RELATION_5 = "Offices";
    public static final String REAL_RELATION_6 = "OrderDetails";
    public static final String REAL_RELATION_7 = "Orders";
    public static final String REAL_RELATION_8 = "Payments";
    public static final String REAL_RELATION_9 = "Products";
    
    public static final String PERFORMANCE_LOG_NAME = "performance.csv";
    
    public static final int LIMIT_NUM = 200;
    
}
