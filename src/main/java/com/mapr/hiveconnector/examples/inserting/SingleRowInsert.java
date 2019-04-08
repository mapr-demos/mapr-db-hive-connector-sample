package com.mapr.hiveconnector.examples.inserting;

import com.mapr.hiveconnector.utils.DaoManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.mapr.hiveconnector.utils.TableManager.*;

/**
 * The class SingleRowInsert describes how to
 * insert a single table row into a nested MapR DB table.
 */
public class SingleRowInsert {

    /**
     * The table name.
     */
    private String tableName = "nested_data_java";

    /**
     * The table's fields.
     */
    private String tableFields  = " (entry STRING, num INT, "
            + "postal_addresses MAP <STRING, "
            + "struct <USER_ID:STRING,ADDRESS:STRING,ZIP:STRING,COUNTRY:STRING>>) ";


    /**
     * The singleRowInsertDummySQL statement for inserting values into table using dummy table.
     */
    private String singleRowInsertDummySQL = "WITH dummy_table AS (SELECT '001' AS KEY, '1' AS num,"
            + "MAP ('Adam',"
            + "Named_struct ('user_id', '1', 'address', '3205 Woodlake ct', 'zip', '45040', 'country', 'Usa'),"
            + "'Wilfred',"
            + "Named_struct ('user_id', '2', 'address', '777 Brockton Avenue', 'zip', '34000', 'country', 'Ita')) "
            + "AS postal_addresses) INSERT INTO " + tableName
            + " SELECT * FROM dummy_table";

    /**
     * The singleRowInsertIntoSQL statement for inserting values into table using INSERT INTO statement.
     */
    private String singleRowInsertIntoSQL = "INSERT INTO TABLE " + tableName + " SELECT '002', '2',"
            + "MAP ('Bill',"
            + "Named_struct ('user_id', '1', 'address', '328 Virginia Ave', 'zip', '54956', 'country', 'Bol'),"
            + "'Steve',"
            + "Named_struct ('user_id', '2', 'address', 'Schererville', 'zip', '46375', 'country', 'Efi'))";


    /**
     * The method creates a table for the demonstration inserting operations with nested data.
     * @param statement SQL statement.
     * @throws SQLException if Sql exception occurs.
     */
    public void createNestedDataTable(Statement statement) throws SQLException {
        creatingTable(statement, "CREATE TABLE",
                tableName, tableFields, getTableProperties(tableName, "entry"));
    }

    /**
     * The method run demonstrates example.
     * @param state SQL statement.
     * @throws SQLException if Sql exception occurs.
     */
    public void run(Statement state) throws SQLException {

        System.out.println("INFO: -= Inserting data via dummy table ...");
        state.execute(singleRowInsertDummySQL);
        System.out.println("INFO: -= The data are inserted via dummy table:");
        selectAllFromTable(state, tableName, 2);

        System.out.println("INFO: -= Inserting data using INSERT INTO statement ...");
        state.execute(singleRowInsertIntoSQL);
        System.out.println("INFO: -= The data are inserted via INSERT INTO statement:");
        selectAllFromTable(state, tableName, 4);
    }

    /**
     * The main.
     * @param args arrays of arguments.
     */
    public static void main(String[] args) {
        try (Connection connection = DaoManager.getInstance().getConnection();
             Statement state = connection.createStatement()) {

            SingleRowInsert insert = new SingleRowInsert();
            insert.createNestedDataTable(state);
            insert.run(state);

        } catch(SQLException sqlException) {
            System.err.println("ERROR: -= Got a sql exception");
            sqlException.printStackTrace();
        } catch(Exception e) {
            System.err.println("ERROR: -= Got an exception");
            e.printStackTrace();
        }
    }

}