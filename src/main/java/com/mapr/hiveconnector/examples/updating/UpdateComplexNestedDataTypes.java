package com.mapr.hiveconnector.examples.updating;

import com.mapr.hiveconnector.utils.DaoManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.mapr.hiveconnector.utils.TableManager.*;
import static com.mapr.hiveconnector.utils.TableManager.selectAllFromTable;

/**
 * The class UpdateComplexNestedDataTypes describes how to
 * update a complex nested data types in Mapr DB using hive MapR DB JSON connector.
 */
public class UpdateComplexNestedDataTypes {

    /**
     * The table name.
     */
    private String tableName = "complex_nested_data_java";

    /**
     * The key field to bound Hive table with Mapr DB JSON table.
     */
    private String key = "entry";

    /**
     * The table's fields.
     */
    private String tableFields  = " (entry STRING, num INT, postal_addresses MAP <STRING, "
            + " struct <USER_ID:STRING,ADDRESS:STRING,ZIP:STRING,COUNTRY:STRING>> ) ";

    /**
     * The insertSQL statement for inserting values into table.
     */
    private String insertSQL = "INSERT INTO TABLE " + tableName + " SELECT '001', '1',"
            + " MAP ( 'Bill',"
            + " Named_struct ('user_id', '1', 'address', '3205 Woodlake ct', 'zip', '45040', 'country', 'USA'))";

    /**
     * The updateSQL statement for updating values into table.
     */
    private String updateSQL = "UPDATE " + tableName
            + " SET postal_addresses = MAP ('Bill',"
            + " Named_struct ('user_id', '1', 'address', '3205 Woodlake ct', 'zip', '45040', 'country', 'Hun'))"
            + " WHERE entry = '001'";

    /**
     * The method run demonstrates example.
     * @param state SQL statement.
     * @throws SQLException if Sql exception occurs.
     */
    public void run(Statement state) throws SQLException {
        System.out.println("INFO: -= Updating table " + tableName + "...");
        state.execute(updateSQL);
        System.out.println("INFO: -= The data into table " + tableName + " are updated");
    }

    /**
     * The main.
     * @param args arrays of arguments.
     */
    public static void main(String[] args) {
        try (Connection connection = DaoManager.getInstance().getConnection();
             Statement state = connection.createStatement()) {

            UpdateComplexNestedDataTypes update = new UpdateComplexNestedDataTypes();

            creatingTable(state,"CREATE TABLE",
                    update.tableName, update.tableFields,
                    getTableProperties(update.tableName, update.key));

            System.out.println("INFO: -= Inserting data to " + update.tableName + "...");
            state.execute(update.insertSQL);
            selectAllFromTable(state, update.tableName, 10);

            update.run(state);
            selectAllFromTable(state, update.tableName, 10);

        } catch(SQLException sqlException) {
            System.err.println("ERROR: -= Got a sql exception");
            sqlException.printStackTrace();
        } catch(Exception e) {
            System.err.println("ERROR: -= Got an exception");
            e.printStackTrace();
        }
    }

}
