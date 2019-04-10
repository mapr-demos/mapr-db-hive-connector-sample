package com.mapr.hiveconnector.examples.querying;

import com.mapr.hiveconnector.utils.DaoManager;

import java.sql.*;

/**
 * The class is responsible for reading the data
 * from the table "primitive_types_from_java".
 */
public class SelectFromTablePrimitiveTypes {

    /**
     * The table name.
     */
    private static String tableName = "primitive_types_from_java";

    /**
     * The selectSQL for selecting values into table.
     */
    private static String selectSQL = "SELECT "
            + "doc_id, bo, d, da, f, i, s, ts, ti, bi, si, bin "
            + "FROM " + tableName;

    /**
     * The main.
     * @param args arrays of arguments.
     */
    public static void main(String[] args) {
        DaoManager.getInstance().setConfigParam(args);
        try (Connection connection = DaoManager.getInstance().getConnection();
             Statement state = connection.createStatement()) {

            System.out.println("INFO: -= Reading table " + tableName + " ...");
            ResultSet resultSet = state.executeQuery(selectSQL);
            while (resultSet.next()) {
                System.out.print("| " + resultSet.getString("doc_id"));
                System.out.print(" | " + resultSet.getBoolean("bo"));
                System.out.print(" | " + resultSet.getDouble("d"));
                System.out.print(" | " + resultSet.getDate("da"));
                System.out.print(" | " + resultSet.getFloat("f"));
                System.out.print(" | " + resultSet.getInt("i"));
                System.out.print(" | " + resultSet.getString("s"));
                System.out.print(" | " + resultSet.getTimestamp("ts"));
                System.out.print(" | " + resultSet.getByte("ti"));
                System.out.print(" | " + resultSet.getLong("bi"));
                System.out.print(" | " + resultSet.getShort("si") + " |");
                System.out.println();
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
