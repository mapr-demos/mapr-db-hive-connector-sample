package com.mapr.hiveconnector.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The ShowTables class is responsible
 * for showing list of tables in the database,
 * configured in the properties.
 */
public class ShowTables {

    /**
     * The selectSQL for displaying list of tables.
     */
    private static String selectSQL = "show tables";

    /**
     * The main.
     * @param args arrays of arguments.
     */
    public static void main(String[] args) {
        DaoManager.getInstance().setConfigParam(args);
        try (Connection connection = DaoManager.getInstance().getConnection();
             Statement state = connection.createStatement()) {
            System.out.println("INFO: -= Tables list:");
            ResultSet show_tables = state.executeQuery(selectSQL);

            while (show_tables.next()) {
                System.out.println(show_tables.getString(1));
            }
        } catch(SQLException sqlException) {
            System.err.println("ERROR: -= Got a sql exception");
            sqlException.printStackTrace();
        } catch(Exception e) {
            System.err.println("ERROR: -= Got an exception");
            e.printStackTrace();
        }
    }

}
