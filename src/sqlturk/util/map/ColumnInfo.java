package sqlturk.util.map;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import sqlturk.configuration.Parameters;

public class ColumnInfo {
    private static ArrayList<Column> columns = new ArrayList<Column>();

    private ColumnInfo() {
	throw new AssertionError();
    }

    public static void addColumn(String resultTableName,
	    String resultColumnName, String sourceTableName,
	    String sourceTableOriginalName, String sourceColumnName) {
	columns.add(new Column(resultTableName, resultColumnName,
		sourceTableName, sourceTableOriginalName, sourceColumnName));
    }

    public static void addColumn(Column column) {
	columns.add(column);
    }

    public static void printColumnInfo() {
	System.out.println("Column information stored in ColumnInfo:");
	for (Column column : columns) {
	    column.printColumn();
	    System.out.println();
	}
    }

    public static String getOriginalTableColumnName(String resultTableName,
	    String resultColumnName, Connection dbConn) throws SQLException {
	return getOriginalTableName(resultTableName, resultColumnName, dbConn)
		+ "_"
		+ getOriginalColumnName(resultTableName, resultColumnName,
			dbConn);
    }

    /**
     * 
     * @param resultTableName
     * @param resultColumnName
     * @throws SQLException
     */
    public static String getOriginalColumnName(String resultTableName,
	    String resultColumnName, Connection dbConn) throws SQLException {
	Statement stmt = dbConn.createStatement();

	// first check if this attribute is in this table
	boolean hasColumn = false;
	ResultSet rs = stmt.executeQuery("DESC " + resultTableName);
	while (rs.next()) {
	    if (rs.getString(1).equals(resultColumnName)) {
		hasColumn = true;
		break;
	    }
	}
	if (!hasColumn) {
	    rs.close();
	    return resultColumnName;
	}

	// when we really have this attribute
	rs = stmt.executeQuery("SELECT " + Parameters.SELECT_ALIAS_COLUMN_REAL
		+ " FROM " + Parameters.COLUMN_INFO_REL_NAME + " WHERE "
		+ Parameters.SELECT_ALIAS_RESULT_TABLE + "='" + resultTableName
		+ "' AND " + Parameters.SELECT_ALIAS_COLUMN_ALIAS + "='"
		+ resultColumnName + "'");

	if (rs.next()) {
	    // // debug
	    // System.out.println("debug:\tresultTableName\t=\t" +
	    // resultTableName);
	    // System.out.println("debug:\tresultColumnName\t=\t" +
	    // resultColumnName);
	    String originalColumnName = rs
		    .getString(Parameters.SELECT_ALIAS_COLUMN_REAL);
	    rs.close();
	    if (originalColumnName.contains(".")) {
		String[] col = originalColumnName.split("\\.");
		return col[1];
	    } else {
		return originalColumnName;
	    }
	} else {
	    // when there is no information about this table in SELECT_ALIAS
	    // table
	    // just return the name of the column as the original column name
	    rs.close();
	    return resultColumnName;
	}
    }

    /**
     * 
     * @param resultTableName
     * @param resultColumnName
     * @throws SQLException
     */
    public static String getOriginalTableName(String resultTableName,
	    String resultColumnName, Connection dbConn) throws SQLException {
	Statement stmt = dbConn.createStatement();

	// first check if this attribute is in this table
	boolean hasColumn = false;
	ResultSet rs = stmt.executeQuery("DESC " + resultTableName);
	while (rs.next()) {
	    if (rs.getString(1).equals(resultColumnName)) {
		hasColumn = true;
		break;
	    }
	}
	if (!hasColumn) {
	    rs.close();
	    return resultTableName;
	}

	// when we have this column in this table
	rs = stmt.executeQuery("SELECT " + Parameters.FROM_ALIAS_TABLE_REAL
		+ " FROM " + Parameters.COLUMN_INFO_REL_NAME + " WHERE "
		+ Parameters.SELECT_ALIAS_RESULT_TABLE + "='" + resultTableName
		+ "' AND " + Parameters.SELECT_ALIAS_COLUMN_ALIAS + "='"
		+ resultColumnName + "'");

	rs.next();
	String originalTableName = rs
		.getString(Parameters.FROM_ALIAS_TABLE_REAL);
	rs.close();
	return originalTableName;

	// if (rs.next()) {
	// // // debug
	// // System.out.println("debug:\tresultTableName\t=\t" +
	// resultTableName);
	// // System.out.println("debug:\tresultColumnName\t=\t" +
	// resultColumnName);
	// String originalTableName =
	// rs.getString(Parameters.FROM_ALIAS_TABLE_REAL);
	// rs.close();
	// return originalTableName;
	// } else {
	// // when there is no information about this table in FROM_ALIAS table
	// // just return the name of the table as the original table name
	// return resultTableName;
	// }
    }

}
