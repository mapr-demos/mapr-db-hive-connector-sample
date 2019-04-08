package com.mapr.hiveconnector.utils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The class TableManager contains the utility methods
 * which are responsible for managing tables.
 */
public class TableManager {

    /**
     * The mandatory properties for creating an intermediate table.
     */
    public static String intermediateProperties =
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE";

    /**
     * The method adds SerDe Jar for JSON to Hive.
     * @param state SQL statement.
     * @throws SQLException if Sql exception occurs.
     */
    public static void addSerDe(Statement state) throws SQLException {
        System.out.println("INFO: -= Adding SerDe JAR for JSON ...");
        state.execute(
                "add jar /opt/mapr/hive/hive-2.3/hcatalog/share/hcatalog/hive-hcatalog-core-2.3.3-mapr-1901.jar");
        System.out.println("INFO: -= SerDe JAR added");
    }

    /**
     * The mandatory properties for the table.
     *
     * @param tableName the name of table which be created in Mapr DB.
     * @param key the field in the table will be mapped to "maprdb.column.id".
     * @return the string of mandatory properties.
     */
    public static String getTableProperties(String tableName, String key) {
        return "STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler' "
                + "TBLPROPERTIES(\"maprdb.table.name\" = \"/" + tableName + "\" ,"
                + "\"maprdb.column.id\" = \"" + key + "\")";
    }

    /**
     * The method creates table.
     * @param state statement.
     * @param query SQL query.
     * @param tableName the name of creating table.
     * @param field the tables field.
     * @param properties the mandatory properties.
     * @throws SQLException if Sql exception occurs.
     */
    public static void creatingTable(
            Statement state, String query, String tableName, String field, String properties)
            throws SQLException {
        System.out.println("INFO: -= Creating table " + tableName + "...");
        state.execute("DROP TABLE IF EXISTS " + tableName);
        state.execute(query + " " + tableName + field + properties);
        System.out.println("INFO: -= The table " + tableName + " is created");

    }

    /**
     * The method prints all columns from the given table until the limit.
     * Note: Use this method only for testing purposes.
     * @param state SQL statement.
     * @param tableName the name of table.
     * @throws SQLException if Sql exception occurs.
     */
    public static void selectAllFromTable(Statement state, String tableName, int limit) throws SQLException {
        ResultSet resultSet = state.executeQuery("SELECT * FROM " + tableName + " LIMIT " + limit);
        int columnsNumber = resultSet.getMetaData().getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                System.out.print(resultSet.getObject(i).toString() + " | ");
            }
            System.out.println();
        }
    }

    /**
     * The method checks if the table exists into a database.
     * @param statement SQL statement.
     * @param tableName name of the table to check.
     * @return true if the table exists or false - otherwise.
     * @throws SQLException if SQL exception occurs.
     */
    public static boolean existTable(Statement statement, String tableName) throws SQLException {
        boolean exist = false;
        ResultSet resultSet = statement.executeQuery("show tables");
        while (resultSet.next()) {
            if (tableName.equals(resultSet.getString(1))) {
                exist = true;
                break;
            }
        }
        return exist;
    }

}

