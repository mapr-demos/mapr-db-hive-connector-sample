package com.mapr.hiveconnector.examples.inserting.overwriting;

import com.mapr.hiveconnector.examples.inserting.MultipleRowInsert;
import com.mapr.hiveconnector.examples.inserting.SingleRowInsert;
import com.mapr.hiveconnector.utils.DaoManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.mapr.hiveconnector.utils.TableManager.selectAllFromTable;

/**
 * The class Overwriting describes how to use the INSERT statement
 * on a dummy table to overwrite one or more complete rows.
 */
public class Overwriting {

    /**
     * The table name.
     */
    private String tableName = "nested_data_java";

    /**
     * The overwriteOneSQL statement for overwriting values
     * in one row into a table using a dummy table.
     */
    private String overwriteOneSQL = "WITH dummy_table AS ("
            + " SELECT '001' AS KEY,"
            + " '1' AS num,"
            + " MAP ('newAdam',"
            + " Named_struct ('user_id', '1', 'address', 'newAdress', 'zip', 'newZip', 'country', 'newCountry')) "
            + " AS postal_addresses)"
            + " INSERT INTO " + tableName + " SELECT * FROM dummy_table";

    /**
     * The overwriteFewSQL statement for overwriting values
     * in a few rows into a table using a dummy table.
     */
    private String overwriteFewSQL = "WITH dummy_table AS ("
            + " SELECT '003' AS KEY,"
            + " '3' AS num,"
            + " MAP ('newName1',"
            + " Named_struct ('user_id', '1', 'address', 'newAdress1', 'zip', 'newZip1', 'country', 'newCountry1')) AS postal_addresses"
            + " UNION ALL"
            + " SELECT '004' AS KEY,"
            + " '4' AS num,"
            + " MAP ('newName2',"
            + " Named_struct ('user_id', '1', 'address', 'newAdress2', 'zip', 'newZip2', 'country', 'newCountry2')) AS postal_addresses)"
            + " INSERT INTO " + tableName + " SELECT * FROM dummy_table";

    /**
     * The method run demonstrates example.
     * @param state SQL statement.
     * @throws SQLException if Sql exception occurs.
     */
    public void run(Statement state) throws SQLException {
        System.out.println("INFO: -= Overwriting one row ...");
        state.execute(overwriteOneSQL);
        System.out.println("INFO: -= The data are overwritten:");
        selectAllFromTable(state, tableName, 10);

        System.out.println("INFO: -= Overwriting a few row ...");
        state.execute(overwriteFewSQL);
        System.out.println("INFO: -= The data are overwritten:");
        selectAllFromTable(state, tableName, 20);
    }

    /**
     * The main.
     * @param args arrays of arguments.
     */
    public static void main(String[] args) {
        DaoManager.getInstance().setConfigParam(args);
        try (Connection connection = DaoManager.getInstance().getConnection();
             Statement state = connection.createStatement()) {
            new SingleRowInsert().run(state);
            new MultipleRowInsert().run(state);

            Overwriting overwriting = new Overwriting();
            overwriting.run(state);

        } catch(SQLException sqlException) {
            System.err.println("ERROR: -= Got a sql exception");
            sqlException.printStackTrace();
        } catch(Exception e) {
            System.err.println("ERROR: -= Got an exception");
            e.printStackTrace();
        }
    }

}