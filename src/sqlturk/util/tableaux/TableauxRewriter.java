package sqlturk.util.tableaux;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
import java.util.regex.Pattern;

import sqlturk.configuration.Parameters;
import sqlturk.util.map.Column;
import sqlturk.util.map.ColumnInfo;

/**
 * NOTE: suppose this is the most basic SPJ query, such as
 * "select t1.A, t2.B from table1 as t1, table2 as t2 where t1.A = t2.A"
 * 
 */
public class TableauxRewriter {

    private static int queryId = Parameters.QUERY_RESULT_TABLE_INDEX_START;

    // stores all the information about the alias in select clause
    // <resultTableName, <attAlias,attName>>
    private static HashMap<String, HashMap<String, String>> selectClauseAliasPairs = 
	    new HashMap<String, HashMap<String, String>>();

    // <resultTableName, <aliasName, originalName>>
    private static HashMap<String, HashMap<String, String>> fromClauseAliasPair = 
	    new HashMap<String, HashMap<String, String>>();

    // <resultTable, <table's alias name, column's original name>>
    // Note: table's alias name is from fromClauseAliasPair's aliasName
    // column's original name is from selectClauseAliasPairs' attName
    private static HashMap<String, HashMap<String, String>> sourceTableResultColumn = 
	    new HashMap<String, HashMap<String, String>>();

    private static int selectStart = -1;
    private static int fromStart = -1;
    private static int whereStart = -1;
    private static int groupStart = -1;
    private static int orderStart = -1;
    private static int havingStart = -1;

    // suppress the default constructor
    private TableauxRewriter() {
    }

    public static ArrayList<String> getRewriteQueries(
	    ArrayList<String> queries, Connection dbConn) throws SQLException {
	Statement stmt = dbConn.createStatement();
	stmt.executeUpdate("DROP TABLE IF EXISTS "
		+ Parameters.SELECT_ALIAS_REL_NAME);
	stmt.executeUpdate("DROP TABLE IF EXISTS "
		+ Parameters.FROM_ALIAS_REL_NAME);
	stmt.close();

	ArrayList<String> rewriteQueries = new ArrayList<String>();
	for (String query : queries) {
	    // debug
	    // System.out.println("debug:\t" + query);

	    rewriteQueries.add(getRewriteQuery(query, dbConn));
	}

	createSelectClauseAliasPairRelation(dbConn);
	createFromClauseAliasPairRelation(dbConn);
	createSourceTableResultColumnRelation(dbConn);
	createColumnMap(dbConn);

	// Important, since we will use the info stored in ColumnInfo
	initializeColumnInfo(dbConn);

	return rewriteQueries;
    }

    public static String getRewriteQuery(String query, Connection dbConn)
	    throws SQLException {
	String originalQuery = new String(query);
	boolean hasDistinct = Pattern
		.compile(Pattern.quote(" DISTINCT "), Pattern.CASE_INSENSITIVE)
		.matcher(query).find();

	if (hasDistinct) {
	    int distinctStart = query.toUpperCase().indexOf(" DISTINCT ") + 1;
	    query = query.substring(0, distinctStart)
		    + query.substring(distinctStart + 9, query.length());
	}

	selectStart = -1;
	fromStart = -1;
	whereStart = -1;
	groupStart = -1;
	orderStart = -1;
	havingStart = -1;

	ArrayList<String> fromClauseForeignKeyConstraintEquations = 
		new ArrayList<String>();
	AliasManager aliasManager = new AliasManager();

	// get the hierarchical clause
	LinkedList<Clause> clauses = getHierarchicalClause(query);

	if (clauses.size() < 2) {
	    throw new RuntimeException(
		    "SPJRewriteProcessor: Not a single SPJ query.");
	}

	// analyze each clause
	for (int i = 0; i < clauses.size(); i++) {

	    /*
	     * get all the alias in the select clause for future use, since we
	     * have to know the original column afterwards.
	     */
	    if (clauses.get(i).getClauseName().equalsIgnoreCase("SELECT")) {
		Clause selectClause = clauses.get(i);
		HashMap<String, String> hm = selectClause.getAlias();
		HashMap<String, String> nameLabelPairs = new HashMap<String, String>();
		ArrayList<String> allRelations = selectClause.getAllElements();

		if (hm == null) {// ---build a table hosting the alias name?
		    for (String relationName : allRelations) {
			String[] showName = relationName.split("\\.");
			if (showName.length <= 1) {
			    nameLabelPairs.put(relationName, relationName);
			} else {
			    nameLabelPairs.put(showName[1], relationName);
			}
		    }
		} else {
		    for (String relationName : allRelations) {
			if (hm.get(relationName) != null) {
			    String[] showName = hm.get(relationName).split(
				    "\\.");
			    if (showName.length <= 1) {
				nameLabelPairs.put(hm.get(relationName),
					relationName);
			    } else {
				nameLabelPairs.put(showName[1], relationName);
			    }
			} else {
			    String[] showName = relationName.split("\\.");
			    if (showName.length <= 1) {
				nameLabelPairs.put(relationName, relationName);
			    } else {
				nameLabelPairs.put(showName[1], relationName);
			    }
			}
		    }
		}

		// // check if we have "*"
		// Set<String> ks = nameLabelPairs.keySet();
		// for (String key : ks) {
		// System.out.println("debug:\tkey=" + key + ", value=" +
		// nameLabelPairs.get(key));
		// }

		// put all the alias into the array
		selectClauseAliasPairs.put(Parameters.QUERY_RESULT_PREFIX
			+ queryId + Parameters.QUERY_RESULT_SUFFIX,
			nameLabelPairs);

	    } else if (clauses.get(i).getClauseName().equalsIgnoreCase("FROM")) {
		Clause fromClause = clauses.get(i);
		ArrayList<String> allRelationsInFromClause = fromClause
			.getAllElements();
		ArrayList<String> allForeigKeyConstraints = new ArrayList<String>();

		// put all existing aliases into aliasManager
		for (String relationName : allRelationsInFromClause) {
		    if (fromClause.getRelationsWithoutAlias().contains(
			    relationName)) {
			aliasManager.addGlobalAliasPair(relationName,
				relationName);
			// debug
			// System.out.println("debug:\tadd new alias pair:" +
			// relationName + "->" + relationName);
		    } else {
			aliasManager.addGlobalAliasPair(relationName,
				fromClause.getAlias().get(relationName));
			// debug
			// System.out.println("debug:\tadd new alias pair:" +
			// relationName + "->" +
			// fromClause.getAlias().get(relationName));
		    }
		}

		// get all tableaux/FK equations into allFKEquations
		for (String relationName : allRelationsInFromClause) {
		    try {
			ArrayList<String> foreignKeyConstraints = ForeignKeyConstraintUtil
				.getFKConstraintString(relationName, dbConn);
			for (String foreignkeyConstraint : foreignKeyConstraints) {
			    if (!allForeigKeyConstraints
				    .contains(foreignkeyConstraint)) {
				allForeigKeyConstraints
					.add(foreignkeyConstraint);
				// System.out.println("debug:\tOriginal_Foreign_Key_Constraint_Equation: "
				// + foreignkeyConstraint);
			    }
			}
		    } catch (SQLException e) {
			System.err.println("Cannot create FK table for "
				+ relationName);
			e.printStackTrace();
		    }
		}

		// parse the equations
		HashMap<String, ArrayList<String>> innerReferenced = 
			new HashMap<String, ArrayList<String>>();
		for (String foreignKeyConstraint : allForeigKeyConstraints) {
		    String rewriteForeignKeyConstraint = getRewriteForeignKeyConstraint(
			    foreignKeyConstraint, aliasManager, innerReferenced);
		    fromClauseForeignKeyConstraintEquations
			    .add(rewriteForeignKeyConstraint);
		    // System.out.println("debug:\tRewrite_Foreign_Key_Constraint_Equation: "
		    // + rewriteForeignKeyConstraint);
		}
	    }
	}

	ArrayList<String> relations = null;
	HashMap<String, String> relationAlias = null;
	for (Clause clause : clauses) {
	    if (clause.getClauseName().equalsIgnoreCase("FROM")) {
		relations = clause.getAllElements();
		relationAlias = clause.getAlias();
	    }
	}

	// rewrite select clause
	String selectClause = getRewriteSelectGroupOrderClause("SELECT",
		clauses, relations, relationAlias, dbConn);

	// rewrite from clause
	String fromClause = "FROM ";
	ArrayList<String> allAliasPairs = aliasManager.getAllAliasPairs();
	HashMap<String, String> relationAliasPairs = new HashMap<String, String>();
	for (int i = 0; i < allAliasPairs.size(); i++) {
	    if (i < allAliasPairs.size() - 1) {
		fromClause += allAliasPairs.get(i) + ", ";
	    } else {
		fromClause += allAliasPairs.get(i) + " ";
	    }
	    if (Pattern.compile(Pattern.quote("AS"), Pattern.CASE_INSENSITIVE)
		    .matcher(allAliasPairs.get(i)).find()) {
		String[] aliasPairs = allAliasPairs.get(i).split("[aA][sS]");
		relationAliasPairs.put(aliasPairs[1].trim(),
			aliasPairs[0].trim()); // first parameter should be the
					       // alias of the table, second
					       // parameter should be the
					       // original/real name
	    } else {
		relationAliasPairs.put(allAliasPairs.get(i).trim(),
			allAliasPairs.get(i).trim());
	    }
	}
	fromClauseAliasPair.put(Parameters.QUERY_RESULT_PREFIX + queryId
		+ Parameters.QUERY_RESULT_SUFFIX, relationAliasPairs);

	// rewrite where clauses
	String whereClause = getRewriteWhereClause(clauses, relations,
		relationAlias, dbConn);
	// append the foreign key constraint chain to where clause
	if (whereClause.equals("")
		&& !fromClauseForeignKeyConstraintEquations.isEmpty()) {
	    whereClause = "WHERE "
		    + fromClauseForeignKeyConstraintEquations.get(0);
	    for (int i = 1; i < fromClauseForeignKeyConstraintEquations.size(); i++) {
		whereClause += " AND "
			+ fromClauseForeignKeyConstraintEquations.get(i);
	    }
	} else {
	    for (int i = 0; i < fromClauseForeignKeyConstraintEquations.size(); i++) {
		whereClause += " AND "
			+ fromClauseForeignKeyConstraintEquations.get(i);
	    }
	}

	// rewrite group by
	String groupByClause = "";
	if (groupStart > 0) { // when there is a group by clause
	    groupByClause = getRewriteSelectGroupOrderClause("GROUP BY",
		    clauses, relations, relationAlias, dbConn);
	}

	// rewrite order by
	String orderByClause = "";
	if (orderStart > 0) { // when there is a order by clause
	    orderByClause = getRewriteSelectGroupOrderClause("ORDER BY",
		    clauses, relations, relationAlias, dbConn);
	}

	// rewrite having
	String havingClause = "";
	if (havingStart > 0) { // when there is a group by clause
	    havingClause = getRewriteHavingClause(clauses, relations,
		    relationAlias, dbConn);
	}

	String restClause = " ";
	String[] restClauses = { groupByClause, orderByClause, havingClause };
	int[] rank = { groupStart, orderStart, havingStart };
	for (int i = 0; i < rank.length; i++) {
	    for (int j = 0; j < rank.length; j++) {
		if (rank[i] < rank[j] && rank[i] > 0) {
		    String tempStr = restClauses[i];
		    int tempInt = rank[i];
		    restClauses[i] = restClauses[j];
		    rank[i] = rank[j];
		    restClauses[j] = tempStr;
		    rank[j] = tempInt;
		}
	    }
	}
	for (int i = 0; i < rank.length; i++) {
	    if (rank[i] > 0) {
		restClause += restClauses[i];
	    }
	}

	String rewriteQuery = "";
	if (hasDistinct) {
	    rewriteQuery = selectClause.substring(0, 6) + " DISTINCT "
		    + selectClause.substring(6, selectClause.length())
		    + fromClause + whereClause + restClause;
	} else {
	    rewriteQuery = selectClause + fromClause + whereClause + restClause;
	}

	System.out.println("debug:\tOriginalQuery:\t" + originalQuery);
	System.out.println("debug:\tRewriteQuery:\t" + rewriteQuery);
	System.out.println();

	// clear
	fromClauseForeignKeyConstraintEquations.clear();

	queryId++;

	return (rewriteQuery);

    }

    private static void initializeColumnInfo(Connection dbConn)
	    throws SQLException {

	Statement stmt = dbConn.createStatement();
	ResultSet rs = stmt.executeQuery("SELECT * FROM "
		+ Parameters.COLUMN_INFO_REL_NAME);
	while (rs.next()) {
	    String resultTableName = rs
		    .getString(Parameters.SELECT_ALIAS_RESULT_TABLE);
	    String resultColumnName = rs
		    .getString(Parameters.SELECT_ALIAS_COLUMN_ALIAS);
	    String sourceTableAliasName = rs
		    .getString(Parameters.FROM_ALIAS_TABLE_ALIAS);
	    String sourceTableOriginalName = rs
		    .getString(Parameters.FROM_ALIAS_TABLE_REAL);
	    String sourceColumnName = null;

	    // get rid of "."
	    String tempSouceColumnName = rs
		    .getString(Parameters.SELECT_ALIAS_COLUMN_REAL);
	    if (tempSouceColumnName.contains(".")) {
		String[] temp = tempSouceColumnName.split("\\.");
		sourceColumnName = temp[1].trim();
	    } else {
		sourceColumnName = tempSouceColumnName;
	    }

	    // String sourceColumnName =
	    // rs.getString(Parameters.SELECT_ALIAS_COLUMN_REAL);
	    ColumnInfo.addColumn(new Column(resultTableName, resultColumnName,
		    sourceTableAliasName, sourceTableOriginalName,
		    sourceColumnName));
	}

	rs.close();
	stmt.close();

	// debug
	ColumnInfo.printColumnInfo();
    }

    private static void createSelectClauseAliasPairRelation(Connection dbConn)
	    throws SQLException {
	Statement stmt = dbConn.createStatement();
	stmt.executeUpdate("CREATE TABLE IF NOT EXISTS "
		+ Parameters.SELECT_ALIAS_REL_NAME + " ("
		+ Parameters.SELECT_ALIAS_RESULT_TABLE + " VARCHAR(30), "
		+ Parameters.SELECT_ALIAS_COLUMN_ALIAS + " VARCHAR(30), "
		+ Parameters.SELECT_ALIAS_COLUMN_REAL + " VARCHAR(30))");

	Set<String> resultTables = selectClauseAliasPairs.keySet();
	for (String resultTable : resultTables) {
	    HashMap<String, String> aliasPair = selectClauseAliasPairs
		    .get(resultTable);
	    Set<String> names = aliasPair.keySet();
	    for (String name : names) {
		// System.out.println("debug:\tRelation = " + alias +
		// ", RealName = " + name + ", Alias = " + aliasPair.get(name));
		stmt.executeUpdate("INSERT INTO "
			+ Parameters.SELECT_ALIAS_REL_NAME + " VALUES ('"
			+ resultTable + "', '" + name + "', '"
			+ aliasPair.get(name) + "')");
	    }
	}
	stmt.close();
    }

    private static void createSourceTableResultColumnRelation(Connection dbConn)
	    throws SQLException {
	Statement stmt = dbConn.createStatement();
	stmt.executeUpdate("DROP TABLE IF EXISTS "
		+ Parameters.SOURCE_MAP_REL_NAME);
	stmt.executeUpdate("CREATE TABLE " + Parameters.SOURCE_MAP_REL_NAME
		+ " (" + Parameters.SOURCE_MAP_RESULT_TABLE + " VARCHAR(30), "
		+ Parameters.SOURCE_MAP_COLUMN_ALIAS + " VARCHAR(30), "
		+ Parameters.SOURCE_MAP_TABLE_ALIAS + " VARCHAR(30))");

	Set<String> resultTables = sourceTableResultColumn.keySet();
	for (String resultTable : resultTables) {
	    HashMap<String, String> aliasPair = sourceTableResultColumn
		    .get(resultTable);
	    Set<String> selectAliasNames = aliasPair.keySet();
	    for (String selectAliasName : selectAliasNames) {
		// System.out.println("debug:\tRelation = " + alias +
		// ", RealName = " + name + ", Alias = " + aliasPair.get(name));
		stmt.executeUpdate("INSERT INTO "
			+ Parameters.SOURCE_MAP_REL_NAME + " VALUES ('"
			+ resultTable + "', '" + selectAliasName + "', '"
			+ aliasPair.get(selectAliasName) + "')");
	    }
	}
	stmt.close();
    }

    private static void createFromClauseAliasPairRelation(Connection dbConn)
	    throws SQLException {
	Statement stmt = dbConn.createStatement();
	stmt.executeUpdate("CREATE TABLE IF NOT EXISTS "
		+ Parameters.FROM_ALIAS_REL_NAME + " ("
		+ Parameters.FROM_ALIAS_REL_TALBE + " VARCHAR(30), "
		+ Parameters.FROM_ALIAS_TABLE_ALIAS + " VARCHAR(30), "
		+ Parameters.FROM_ALIAS_TABLE_REAL + " VARCHAR(30))");

	Set<String> keys = fromClauseAliasPair.keySet();
	for (String resultTableId : keys) {
	    HashMap<String, String> aliasPair = fromClauseAliasPair
		    .get(resultTableId);
	    Set<String> names = aliasPair.keySet();
	    for (String name : names) {
		// System.out.println("Relation = " + key + ", RealName = " +
		// name + ", Alias = " + aliasPair.get(name));
		stmt.executeUpdate("INSERT INTO "
			+ Parameters.FROM_ALIAS_REL_NAME + " VALUES ('"
			+ resultTableId + "', '" + name + "', '"
			+ aliasPair.get(name) + "')");
	    }
	}
	stmt.close();
    }

    private static void createColumnMap(Connection dbConn) throws SQLException {
	Statement stmt = dbConn.createStatement();
	stmt.executeUpdate("DROP TABLE IF EXISTS "
		+ Parameters.COLUMN_INFO_REL_NAME);
	String query = "CREATE TABLE " + Parameters.COLUMN_INFO_REL_NAME + "\n";
	query += "AS\n";
	query += "SELECT DISTINCT " + Parameters.SELECT_ALIAS_REL_NAME + "."
		+ Parameters.SELECT_ALIAS_RESULT_TABLE + ", "
		+ Parameters.SELECT_ALIAS_REL_NAME + "."
		+ Parameters.SELECT_ALIAS_COLUMN_ALIAS + ", "
		+ Parameters.FROM_ALIAS_REL_NAME + "."
		+ Parameters.FROM_ALIAS_TABLE_ALIAS + ", "
		+ Parameters.FROM_ALIAS_REL_NAME + "."
		+ Parameters.FROM_ALIAS_TABLE_REAL + ", "
		+ Parameters.SELECT_ALIAS_REL_NAME + "."
		+ Parameters.SELECT_ALIAS_COLUMN_REAL + " FROM "
		+ Parameters.SELECT_ALIAS_REL_NAME + ", "
		+ Parameters.FROM_ALIAS_REL_NAME + ", "
		+ Parameters.SOURCE_MAP_REL_NAME + " WHERE "
		+ Parameters.SELECT_ALIAS_REL_NAME + "."
		+ Parameters.SELECT_ALIAS_RESULT_TABLE + "="
		+ Parameters.SOURCE_MAP_REL_NAME + "."
		+ Parameters.SOURCE_MAP_RESULT_TABLE + " AND "

		+ Parameters.SELECT_ALIAS_REL_NAME + "."
		+ Parameters.SELECT_ALIAS_COLUMN_ALIAS + "="
		+ Parameters.SOURCE_MAP_REL_NAME + "."
		+ Parameters.SOURCE_MAP_COLUMN_ALIAS + " AND "

		+ Parameters.SOURCE_MAP_REL_NAME + "."
		+ Parameters.SOURCE_MAP_RESULT_TABLE + "="
		+ Parameters.FROM_ALIAS_REL_NAME + "."
		+ Parameters.FROM_ALIAS_REL_TALBE + " AND "

		+ Parameters.SOURCE_MAP_REL_NAME + "."
		+ Parameters.SOURCE_MAP_TABLE_ALIAS + "="
		+ Parameters.FROM_ALIAS_REL_NAME + "."
		+ Parameters.FROM_ALIAS_TABLE_ALIAS;
	// System.out.println("debug:\t"+query);
	stmt.executeUpdate(query);
	stmt.close();
    }

    private static String getRewriteHavingClause(LinkedList<Clause> clauses,
	    ArrayList<String> relations, HashMap<String, String> relationAlias,
	    Connection conn) throws SQLException {
	String newClause = "";
	for (Clause clause : clauses) {
	    if (Pattern
		    .compile(Pattern.quote("HAVING"), Pattern.CASE_INSENSITIVE)
		    .matcher(clause.getClauseName()).find()
		    && !clause.getAllElements().isEmpty()) {
		newClause += "HAVING "
			+ getRewriteHavingAllElements(clause.getAllElements()
				.get(0), relations, relationAlias, conn);
	    }
	}
	return newClause;
    }

    private static String getRewriteWhereClause(LinkedList<Clause> clauses,
	    ArrayList<String> relations, HashMap<String, String> relationAlias,
	    Connection conn) throws SQLException {
	String newClause = "";
	for (Clause clause : clauses) {
	    if (Pattern
		    .compile(Pattern.quote("WHERE"), Pattern.CASE_INSENSITIVE)
		    .matcher(clause.getClauseName()).find()
		    && !clause.getAllElements().isEmpty()) {
		newClause = "WHERE "
			+ getRewriteWhereAllElements(clause.getAllElements()
				.get(0), null, relations, relationAlias, conn);
	    }
	}
	return newClause;
    }

    /*
     * This function is only for select, group by, order by not for from, where
     * and having
     */
    private static String getRewriteSelectGroupOrderClause(
	    String originalClauseName, LinkedList<Clause> clauses,
	    ArrayList<String> relations, HashMap<String, String> relationAlias,
	    Connection dbConn) throws SQLException {
	String newClause = originalClauseName + " ";
	for (Clause clause : clauses) {

	    if (Pattern
		    .compile(Pattern.quote(originalClauseName),
			    Pattern.CASE_INSENSITIVE)
		    .matcher(clause.getClauseName()).find()) {

		// record which column is from which table
		HashMap<String, String> map = new HashMap<String, String>();

		// when we have normal columns
		for (int i = 0; i < clause.getAllElements().size(); i++) {
		    // rewrite the normal column name
		    String colName = clause.getAllElements().get(i);
		    String aliasName = "";

		    // if we have alias
		    if (clause.getAlias().containsKey(colName)) {
			aliasName = " AS " + clause.getAlias().get(colName);
		    }

		    String newElement = "";
		    if (colName.contains("(") && colName.contains(")")) {
			// when it is an anayltical function AND it has alias.
			// p.s. if it doesn't have
			// alias, it will be seen as a function afterwards
			// (function and functionParameters)
			// e.g. avg (Population) avgPop, avg (Population) as
			// avgPop
			// rewritten should be: avg(Country.Population) AS
			// avgPop
			int startIndex = colName.indexOf("(");
			int endIndex = colName.indexOf(")");
			String oldParameter = colName.substring(startIndex + 1,
				endIndex);
			ArrayList<String> operators = new ArrayList<String>();
			operators.add("+");
			operators.add("-");
			operators.add("*");
			operators.add("/");
			operators.add("%");
			String newParameter = getRewriteFunctionParameter(
				oldParameter, operators, relations,
				relationAlias, dbConn);
			newElement = colName
				.replace(oldParameter, newParameter)
				+ aliasName;
		    } else {
			// when it is a normal column, not a parameter in a
			// function
			// e.g. Name, Name as CityName, Name CityName, *,
			// Country.*

			// when it is a real column name, rather than "*"
			if (!colName.equals("*")) {
			    newElement = getRewriteColumnName(colName,
				    relations, relationAlias, dbConn)
				    + aliasName;
			} else {
			    // when it is "*"
			    // check: there should be only one relation
			    if (relations.size() > 1) {
				throw new RuntimeException("Using *, but " +
						"more than one relations" +
						" in FROM clause.");
			    }
			    Statement stmt = dbConn.createStatement();
			    ResultSet rs = stmt.executeQuery("DESC "
				    + relations.get(0));

			    newElement = "";
			    while (rs.next()) {
				if (!rs.getString(1).equals(
					Parameters.ROWID_ATT_NAME)) {
				    newElement += relations.get(0) + "."
					    + rs.getString(1) + ", ";
				    HashMap<String, String> hm = selectClauseAliasPairs
					    .get(Parameters.QUERY_RESULT_PREFIX
						    + queryId
						    + Parameters.QUERY_RESULT_SUFFIX);
				    hm.put(rs.getString(1), rs.getString(1));
				}
			    }
			    newElement = newElement.substring(0,
				    newElement.length() - 2);
			    rs.close();
			    stmt.close();
			}
		    }

		    if (i < clause.getAllElements().size() - 1) {
			newClause += newElement + ", ";
		    } else {
			newClause += newElement + " ";
		    }

		    // update source
		    if (newElement.contains(" AS ")) {
			String[] aliasPairs = newElement.split(" AS ");
			String[] sourceTable = aliasPairs[0].trim()
				.split("\\.");
			map.put(aliasPairs[1].trim(), sourceTable[0].trim());
		    } else if (!newElement.contains(",")) {
			String[] sourceTable = newElement.split("\\.");
			map.put(sourceTable[1].trim(), sourceTable[0].trim());
		    } else {
			String[] cols = newElement.split(",");
			for (int j = 0; j < cols.length; j++) {
			    String col = cols[j].trim();
			    String[] sourceTable = col.split("\\.");
			    map.put(sourceTable[1].trim(),
				    sourceTable[0].trim());
			}
		    }
		}

		// when it is select clause, also update sourceTable
		if (Pattern
			.compile(Pattern.quote("SELECT"),
				Pattern.CASE_INSENSITIVE)
			.matcher(originalClauseName).find()) {
		    // update source
		    sourceTableResultColumn.put(Parameters.QUERY_RESULT_PREFIX
			    + queryId + Parameters.QUERY_RESULT_SUFFIX, map);
		}

		// when we have functions
		if (clause.getFunctionParameters().size() > 0) {
		    ArrayList<String> operators = new ArrayList<String>();
		    operators.add("+");
		    operators.add("-");
		    operators.add("*");
		    operators.add("/");
		    operators.add("%");
		    // when we have normal columns, it means there is a space
		    // already, we have to use a comma to separate
		    if (clause.getAllElements().size() > 0) {
			newClause += ", ";
		    }
		    for (int i = 0; i < clause.getFunctionParameters().size(); i++) {
			String newParameter = getRewriteFunctionParameter(
				clause.getFunctionParameters().get(i),
				operators, relations, relationAlias, dbConn);
			if (i < clause.getFunctionParameters().size() - 1) {
			    if (clause.getFunctions().get(i) != null) {
				newClause += clause.getFunctions().get(i) + "("
					+ newParameter + "), ";
			    } else {
				newClause += "(" + newParameter + "), ";
			    }
			} else {
			    if (clause.getFunctions().get(i) != null) {
				newClause += clause.getFunctions().get(i) + "("
					+ newParameter + ") ";
			    } else {
				newClause += "(" + newParameter + ") ";
			    }
			}
		    }
		}
	    }
	}
	return newClause;
    }

    // retrieve the parameters which are in the arithmetic expression.
    private static ArrayList<String> recursivelyGetAllFunctionParameters(
	    String expression, ArrayList<String> operators, int iterationNo) {
	ArrayList<String> functionParameters = new ArrayList<String>();
	String[] subParameters = expression.split("\\"
		+ operators.get(iterationNo));
	if (iterationNo < operators.size() - 1) {
	    for (int i = 0; i < subParameters.length; i++) {
		functionParameters.addAll(recursivelyGetAllFunctionParameters(
			subParameters[i].trim(), operators, iterationNo + 1));
	    }
	} else {
	    for (int i = 0; i < subParameters.length; i++) {
		functionParameters.add(subParameters[i].trim());
	    }
	}
	return functionParameters;
    }

    private static String getRewriteHavingAllElements(String expression,
	    ArrayList<String> relations, HashMap<String, String> relationAlias,
	    Connection dbConn) throws SQLException {
	ArrayList<String> originalEquations = new ArrayList<String>();
	ArrayList<String> rewriteEquations = new ArrayList<String>();

	// split by "and"
	if (expression.toUpperCase().contains(" AND ")) {
	    String[] andExp = expression.split("[aA][nN][dD]");
	    for (int i = 0; i < andExp.length; i++) {
		originalEquations.add(andExp[i].trim());
		rewriteEquations.add(andExp[i].trim());
	    }
	}

	// split by "or"
	if (expression.toUpperCase().contains(" OR ")) {
	    String[] orExp = expression.split("[oO][rR]");
	    for (int i = 0; i < orExp.length; i++) {
		originalEquations.add(orExp[i].trim());
		rewriteEquations.add(orExp[i].trim());
	    }
	}

	// if there is only one element in where clause
	if (!expression.toUpperCase().contains(" AND ")
		&& !expression.toUpperCase().contains(" OR ")) {
	    originalEquations.add(expression);
	    rewriteEquations.add(expression);
	}

	ArrayList<String> operators = new ArrayList<String>();
	operators.add("+");
	operators.add("-");
	operators.add("*");
	operators.add("/");
	operators.add("%");

	ArrayList<String> functionParameters = new ArrayList<String>();
	ArrayList<String> newFunctionParameters = new ArrayList<String>();
	for (int i = 0; i < originalEquations.size(); i++) {
	    int startIndex = originalEquations.get(i).indexOf("(");
	    int endIndex = originalEquations.get(i).indexOf(")");
	    String parameter = originalEquations.get(i).substring(
		    startIndex + 1, endIndex);
	    functionParameters.add(parameter);
	    newFunctionParameters.add(getRewriteFunctionParameter(parameter,
		    operators, relations, relationAlias, dbConn));
	}

	String newExpression = expression;
	for (int i = 0; i < functionParameters.size(); i++) {
	    newExpression = newExpression.replace(functionParameters.get(i),
		    newFunctionParameters.get(i));
	}
	return newExpression;
    }

    private static String getRewriteWhereAllElements(String expression,
	    ArrayList<String> operators, ArrayList<String> relations,
	    HashMap<String, String> relationAlias, Connection dbConn)
	    throws SQLException {
	ArrayList<String> originalEquations = new ArrayList<String>();
	ArrayList<String> rewriteEquations = new ArrayList<String>();

	// split by "and"
	if (expression.toUpperCase().contains(" AND ")) {
	    String[] andExp = expression.split("[aA][nN][dD]");
	    for (int i = 0; i < andExp.length; i++) {
		originalEquations.add(andExp[i].trim());
		rewriteEquations.add(andExp[i].trim());
	    }
	}

	// split by "or"
	if (expression.toUpperCase().contains(" OR ")) {
	    String[] orExp = expression.split("[oO][rR]");
	    for (int i = 0; i < orExp.length; i++) {
		originalEquations.add(orExp[i].trim());
		rewriteEquations.add(orExp[i].trim());
	    }
	}

	// if there is only one element in where clause
	if (!expression.toUpperCase().contains(" AND ")
		&& !expression.toUpperCase().contains(" OR ")) {
	    originalEquations.add(expression);
	    rewriteEquations.add(expression);
	}

	//
	for (int i = 0; i < originalEquations.size(); i++) {
	    // boolean isRightOperand = false;// the operand on the right
	    String[] operands = originalEquations.get(i).split("[=><]");
	    for (int j = 0; j < operands.length; j++) {
		String[] operand = operands[j].split("\\.");

		// when there is only one element here
		if (operand.length == 1) {
		    String newOperand = getRewriteColumnName(
			    operands[j].trim(), relations, relationAlias,
			    dbConn);
		    rewriteEquations.set(
			    i,
			    rewriteEquations.get(i).replace(operand[0].trim(),
				    newOperand));
		}
	    }
	}

	String newExpression = expression;
	for (int i = 0; i < originalEquations.size(); i++) {
	    newExpression = newExpression.replace(originalEquations.get(i),
		    rewriteEquations.get(i));
	}
	return newExpression;
    }

    // rewrite the parameters
    private static String getRewriteFunctionParameter(String expression,
	    ArrayList<String> operators, ArrayList<String> relations,
	    HashMap<String, String> relationAlias, Connection dbConn)
	    throws SQLException {
	String newExpression = expression;

	ArrayList<String> originalFunctionParameters = recursivelyGetAllFunctionParameters(
		expression, operators, 0);
	// for(String x : originalFunctionParameters) {
	// System.out.println("debug:\toriginal function parameters: "+x);
	// }
	// System.out.println(originalFunctionParameters.size());

	ArrayList<String> rewriteFunctionParameters = new ArrayList<String>();
	for (int i = 0; i < originalFunctionParameters.size(); i++) {
	    rewriteFunctionParameters.add(getRewriteColumnName(
		    originalFunctionParameters.get(i), relations,
		    relationAlias, dbConn));
	}
	for (int i = 0; i < originalFunctionParameters.size(); i++) {
	    int start = newExpression
		    .indexOf(originalFunctionParameters.get(i));
	    int len = originalFunctionParameters.get(i).length();
	    newExpression = newExpression.substring(0, start)
		    + rewriteFunctionParameters.get(i)
		    + newExpression.substring(start + len,
			    newExpression.length());
	}
	return newExpression;
    }

    private static String getRewriteColumnName(String originalColumn,
	    ArrayList<String> relations, HashMap<String, String> relationAlias,
	    Connection dbConn) throws SQLException {

	// if the column is "*" which also implies that there is only one table
	if (originalColumn.equals("*")) {
	    if (relations.size() > 1) {
		throw new RuntimeException(
			"Using *, but there are more than one tables in FROM clause.");
	    } else {
		return relations.get(0) + "." + originalColumn;
	    }
	}

	// if the table name has been already explicitly indicated in the column
	// name, just return.
	if (originalColumn.contains(".")) {
	    return originalColumn;
	}

	// if it is "distinct"
	if (originalColumn.equalsIgnoreCase("DISTINCT")) {
	    return originalColumn;
	}

	// find which relation the column is from
	String tableName = null;
	Statement stmt = dbConn.createStatement();
	for (String relation : relations) {
	    ResultSet rs = stmt.executeQuery("DESC " + relation);
	    while (rs.next()) {
		if (rs.getString(1).equals(originalColumn)) {
		    tableName = relation;
		}
	    }
	}

	// double-check if this relation has alias
	if (relationAlias.containsKey(tableName) && relationAlias != null) {
	    tableName = relationAlias.get(tableName);
	}

	if (tableName == null) {// if no source table is found, this "column" is
				// an arithematic operand, just return it
	    return originalColumn;
	} else {
	    return tableName + "." + originalColumn;
	}
    }

    private static String getRewriteForeignKeyConstraint(
	    String foreignKeyEquation, AliasManager aliasManager,
	    HashMap<String, ArrayList<String>> innerReferenced) {
	String[] columns = foreignKeyEquation.split("=");
	String[] alias = new String[2];
	String[] cols = new String[2];

	// left part
	String[] left = columns[0].trim().split("\\.");
	cols[0] = left[1];
	if (aliasManager.isInGlobalAliasPair(left[0])) {
	    alias[0] = aliasManager.getGlobalAliasName(left[0]);
	} else {
	    alias[0] = aliasManager.createAliasPair(left[0]);
	}
	String leftEquation = alias[0] + "." + cols[0];
	if (innerReferenced.get(left[0]) == null) {
	    innerReferenced.put(left[0], new ArrayList<String>());
	}

	// right part
	String[] right = columns[1].trim().split("\\.");
	cols[1] = right[1];
	// if it was referenced before
	if (aliasManager.isReferencedRelation(right[0])) {
	    // if it is inner referenced
	    if (innerReferenced.get(left[0]).contains(right[0])) {
		alias[1] = aliasManager.getLastGlobalAliasName(right[0]);
	    } else {
		alias[1] = aliasManager.createAliasPair(right[0]);
		aliasManager.addReferencedRelation(right[0]);
		innerReferenced.get(left[0]).add(right[0]);
	    }
	} else {
	    // if it has had global alias already, such as the relations in the
	    // original from clause
	    if (aliasManager.isInGlobalAliasPair(right[0])) {
		alias[1] = aliasManager.getGlobalAliasName(right[0]);
	    } else {
		alias[1] = aliasManager.createAliasPair(right[0]);
	    }
	    aliasManager.addReferencedRelation(right[0]);// correct
	    innerReferenced.get(left[0]).add(right[0]);
	}
	String rightEquation = alias[1] + "." + cols[1];
	String newConstraint = leftEquation + "=" + rightEquation;
	return newConstraint;
    }

    /*
     * parse the original query string into a hierarchical clauses
     */
    private static LinkedList<Clause> getHierarchicalClause(String originalQuery) {
	LinkedList<Clause> queryStringQueue = new LinkedList<Clause>();

	// select
	if (Pattern.compile(Pattern.quote("SELECT"), Pattern.CASE_INSENSITIVE)
		.matcher(originalQuery).find()) {
	    selectStart = originalQuery.toUpperCase().indexOf("SELECT ");
	} else {
	    throw new RuntimeException("NO SELECT clause!");
	}

	// from
	if (Pattern.compile(Pattern.quote(" FROM "), Pattern.CASE_INSENSITIVE)
		.matcher(originalQuery).find()) {
	    fromStart = originalQuery.toUpperCase().indexOf(" FROM ") + 1;
	} else {
	    throw new RuntimeException("NO FROM clause!");
	}

	if (Pattern.compile(Pattern.quote(" WHERE "), Pattern.CASE_INSENSITIVE)
		.matcher(originalQuery).find()) {
	    whereStart = originalQuery.toUpperCase().indexOf(" WHERE ") + 1;
	}
	if (Pattern
		.compile(Pattern.quote(" GROUP BY "), Pattern.CASE_INSENSITIVE)
		.matcher(originalQuery).find()) {
	    groupStart = originalQuery.toUpperCase().indexOf(" GROUP BY ") + 1;
	}
	if (Pattern
		.compile(Pattern.quote(" HAVING "), Pattern.CASE_INSENSITIVE)
		.matcher(originalQuery).find()) {
	    havingStart = originalQuery.toUpperCase().indexOf(" HAVING ") + 1;
	}
	if (Pattern
		.compile(Pattern.quote(" ORDER BY "), Pattern.CASE_INSENSITIVE)
		.matcher(originalQuery).find()) {
	    orderStart = originalQuery.toUpperCase().indexOf(" ORDER BY ") + 1;
	}

	int[] positions = new int[4];
	String[] clauses = new String[4];
	positions[0] = whereStart;
	clauses[0] = "where ";
	positions[1] = groupStart;
	clauses[1] = "group by ";
	positions[2] = havingStart;
	clauses[2] = "having";
	positions[3] = orderStart;
	clauses[3] = "order by ";
	int nClause = 0;
	for (int i = 0; i < 4; i++) {
	    for (int j = i; j < 4; j++) {
		if ((positions[i] > positions[j] && positions[j] > 0)
			|| (positions[i] < 0)) {
		    String tempStr = clauses[i];
		    int tempInt = positions[i];
		    clauses[i] = clauses[j];
		    positions[i] = positions[j];
		    clauses[j] = tempStr;
		    positions[j] = tempInt;
		}
	    }
	}

	for (int i = 0; i < 4; i++) {
	    if (positions[i] > 0) {
		nClause++;
	    }
	    // System.out.print("debug:\t(" + clauses[i] + "," + positions[i] +
	    // "),");
	}
	// System.out.println("debug:\tnClause=" + nClause);

	if (nClause > 0) { // when there are other clause
	    queryStringQueue.add(newClause("SELECT", originalQuery,
		    selectStart, fromStart));
	    queryStringQueue.add(newClause("FROM", originalQuery, fromStart,
		    positions[0]));
	    for (int i = 0; i < nClause; i++) {
		if (i < nClause - 1) {
		    queryStringQueue.add(newClause(clauses[i].toUpperCase(),
			    originalQuery, positions[i], positions[i + 1]));
		} else {
		    queryStringQueue
			    .add(newClause(clauses[i].toUpperCase(),
				    originalQuery, positions[i],
				    originalQuery.length()));
		}
	    }
	} else {
	    queryStringQueue.add(newClause("SELECT", originalQuery,
		    selectStart, fromStart));
	    queryStringQueue.add(newClause("FROM", originalQuery, fromStart,
		    originalQuery.length()));
	}

	return queryStringQueue;
    }

    private static Clause newClause(String clauseName, String originalQuery,
	    int start, int end) {
	String subString = originalQuery.substring(start, end)
		.substring(clauseName.length()).trim();
	HashMap<String, String> newAlias = new HashMap<String, String>();
	ArrayList<String> allElements = new ArrayList<String>();
	ArrayList<String> elementsWithoutAlias = new ArrayList<String>();
	ArrayList<String> functions = new ArrayList<String>();
	ArrayList<String> functionParameters = new ArrayList<String>();

	if (clauseName.toUpperCase().contains("SELECT")) {
	    String[] columns = subString.split(",");
	    for (int i = 0; i < columns.length; i++) {
		String column = columns[i].trim();
		if (column.contains("(") && column.contains(")")
			&& column.contains(" ")
			&& column.lastIndexOf(" ") < column.indexOf(")")) {
		    // when this is an analytical function and no alias for it
		    // e.g. sum ( Populaion )
		    int indexOfOpen = column.indexOf("(");
		    int indexOfClose = column.indexOf(")");
		    functions.add(column.substring(0, indexOfOpen));
		    functionParameters.add(column.substring(indexOfOpen + 1,
			    indexOfClose));
		} else if (column.contains(" ")
			&& column.lastIndexOf(" ") > column.indexOf(")")) {
		    // when there is an alias for normal column (this column can
		    // be an analytical function)
		    // e.g. Population as Pop, Population Pop, avg(Population)
		    // AS average, avg(Population) average
		    String[] alias;
		    if (column.contains("as")) {
			alias = column.split("as");
		    } else if (column.contains("AS")) {
			alias = column.split("AS");
		    } else if (column.contains("aS")) {
			alias = column.split("aS");
		    } else if (column.contains("As")) {
			alias = column.split("As");
		    } else {
			alias = column.split("\\s+");
		    }
		    newAlias.put(alias[0].trim(), alias[1].trim());
		    allElements.add(alias[0].trim());
		} else {
		    // when this is just a normal column (without alias, not an
		    // analytical function)
		    // e.g. Population
		    allElements.add(column);
		    elementsWithoutAlias.add(column);
		}
	    }

	} else if (clauseName.toUpperCase().contains("FROM")) {
	    String[] tables = subString.split(",");
	    for (int i = 0; i < tables.length; i++) {
		String table = tables[i].trim();
		if (table.contains(" ")) {
		    // when there is an alias for the table
		    // e.g. City as ct, City ct
		    String[] alias;
		    if (table.contains("as")) {
			alias = table.split("as");
		    } else if (table.contains("AS")) {
			alias = table.split("AS");
		    } else if (table.contains("aS")) {
			alias = table.split("aS");
		    } else if (table.contains("As")) {
			alias = table.split("As");
		    } else {
			alias = table.split("\\s+");
		    }
		    newAlias.put(alias[0].trim(), alias[1].trim());
		    allElements.add(alias[0].trim());
		} else {
		    // when this is just a table (without alias)
		    // e.g. City
		    allElements.add(table);
		    elementsWithoutAlias.add(table);
		}
	    }
	} else if (clauseName.toUpperCase().contains("WHERE")) {
	    allElements.add(subString);
	    elementsWithoutAlias.add(subString);
	} else if (clauseName.toUpperCase().contains("GROUP BY")) {
	    String[] columns = subString.split(",");
	    for (int i = 0; i < columns.length; i++) {
		allElements.add(columns[i].trim());
		elementsWithoutAlias.add(columns[i].trim());
	    }
	} else if (clauseName.toUpperCase().contains("HAVING")) {
	    allElements.add(subString);
	    elementsWithoutAlias.add(subString);
	} else if (clauseName.toUpperCase().contains("ORDER BY")) {
	    allElements.add(subString);
	    elementsWithoutAlias.add(subString);
	}

	// // debug
	// Set<String> keys = newAlias.keySet();
	// System.out.println("debug:");
	// System.out.println("debug:\tclauseName: " + clauseName);
	// for (String key : keys) {
	// System.out.println("debug:\talias: " + key + "->" +
	// newAlias.get(key));
	// }
	// for (String element: allElements) {
	// System.out.println("debug:\tall elements: " + element);
	// }
	// for (String element: elementsWithoutAlias) {
	// System.out.println("debug:\telements without alias: " + element);
	// }
	// for (int i = 0; i < functions.size(); i++) {
	// System.out.println("debug:\tfunction: " + functions.get(i) +
	// "-- parameters: " + functionParameters.get(i));
	// }
	// System.out.println("debug:");

	return new Clause(clauseName.toUpperCase(), newAlias, allElements,
		elementsWithoutAlias, functions, functionParameters);
    }
}
