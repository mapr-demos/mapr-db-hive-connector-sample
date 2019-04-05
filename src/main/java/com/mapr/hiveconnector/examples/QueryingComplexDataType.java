package com.mapr.hiveconnector.examples;

import com.mapr.hiveconnector.utils.DaoManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The class QueryingComplexDataType is responsible for
 * querying complex data from the table "artists_java"
 */
public class QueryingComplexDataType {

    /**
     * The table name.
     */
    private static String tableName = "artists_java";

    /**
     * The SQL selectSQL for inserting values into table.
     */
    private static String selectSQL = "SELECT name, albums.name[2] FROM "
            + tableName + " WHERE albums.name[2] IS NOT NULL LIMIT 4";

    /**
     * The main.
     * @param args arrays of arguments.
     */
    public static void main(String[] args) {

        try (Connection connection = DaoManager.getInstance().getConnection();
             Statement state = connection.createStatement()) {

            System.out.println("INFO: -= Reading table " + tableName + " ...");
            ResultSet resultSet = state.executeQuery(selectSQL);
            String row;

            while (resultSet.next()) {
                row = String.format("%s || %s",
                        resultSet.getString(1), resultSet.getString(2));
                System.out.println(row);
            }
            System.out.println("INFO: -= All data is read");

        } catch(SQLException sqlException) {
            System.err.println("ERROR: -= Got a sql exception");
            sqlException.printStackTrace();
        } catch(Exception e) {
            System.err.println("ERROR: -= Got an exception");
            e.printStackTrace();
        }
    }

}

