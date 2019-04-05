package com.mapr.hiveconnector.examples;

import com.mapr.hiveconnector.utils.DaoManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.mapr.hiveconnector.utils.TableManager.*;

/**
 * The class QueryingComplexNestedStructures is responsible
 * for querying complex nested structures of data from the table "json_nested_java"
 */
public class QueryingComplexNestedStructures {

    /**
     * The intermediate table name.
     */
    private static String intermediateTableName = "json_nested_load_java";

    /**
     * The table name.
     */
    private static String tableName = "json_nested_java";

    /**
     * The table's fields.
     */
    private static String tableFields  = " (country string, "
            + "languages array<string>, religions map<string,array<int>>) ";

    /**
     * The insertSQL is responsible for inserting data
     * from intermediate table to a hive and Mapr DB tables.
     */
    private static String insertSQL = "INSERT INTO TABLE " + tableName + " SELECT "
            + "country, languages, religions FROM " + intermediateTableName;

    /**
     * The main.
     * @param args arrays of arguments.
     */
    public static void main(String[] args) {

        try (Connection connection = DaoManager.getInstance().getConnection();
             Statement state = connection.createStatement()) {

           // prepareTable(state);

            System.out.println("INFO: -= Select all data from the table");
            selectAllFromTable(state, tableName, 1);

            System.out.println("INFO: -= Select the first language from the table");
            ResultSet resultSet = state.executeQuery("SELECT languages[0] FROM json_nested_java");
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1));
            }

            System.out.println("INFO: -= Select the amount of catholics from the table");
            resultSet = state.executeQuery("SELECT religions['catholic'][0] FROM json_nested_java");
            while (resultSet.next()) {
                System.out.println(resultSet.getInt(1));
            }

        } catch(SQLException sqlException) {
            System.err.println("ERROR: -= Got a sql exception");
            sqlException.printStackTrace();
        } catch(Exception e) {
            System.err.println("ERROR: -= Got an exception");
            e.printStackTrace();
        }
    }

    /**
     * The method makes initial operations under the table.
     * @param state SQL state.
     * @throws SQLException if Sql exception occurs.
     */
    private static void prepareTable(Statement state) throws SQLException {
        addSerDe(state);
        creatingTable(state, "CREATE EXTERNAL TABLE",
                intermediateTableName, tableFields, intermediateProperties);

        state.execute("LOAD DATA LOCAL INPATH '/tmp/dataset/nested.json' OVERWRITE "
                + "INTO TABLE " + intermediateTableName);
        System.out.println("INFO: -= The data is loaded into intermediate table");

        creatingTable(state, "CREATE TABLE",
                tableName, tableFields, getTableProperties(tableName, "country"));

        state.execute(insertSQL);
        System.out.println("INFO: -= The data is inserted into both tables");
    }

}
