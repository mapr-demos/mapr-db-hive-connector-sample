package com.mapr.hiveconnector.examples.updating;

import com.mapr.hiveconnector.utils.DaoManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.mapr.hiveconnector.utils.TableManager.*;

/**
 * The class UpdateComplexDataTypes describes how to
 * update a complex data types in Mapr DB using Hive MapR DB JSON connector.
 */
public class UpdateComplexDataTypes {

    /**
     * The table name.
     */
    private String tableName = "complex_types_java";

    /**
     * The key field to bound Hive table with Mapr DB JSON table.
     */
    private String key = "doc_id";

    /**
     * The table's fields.
     */
    private String tableFields  = " (doc_id string, info MAP<STRING, INT>, pets ARRAY<STRING>,"
            + " user_info STRUCT<name:STRING, surname:STRING, age:INT, gender:STRING>) ";

    /**
     * The insertSQL statement for inserting values into table.
     */
    private String insertSQL = "INSERT INTO TABLE " + tableName
            + " SELECT '1', map('age', 28), array('Cat', 'Cat', 'Cat'),"
            + " named_struct('name', 'Santa', 'surname', 'Claus','age', 1000,'gender', 'MALE')";

    /**
     * The updateSQL statement for updating values into table.
     */
    private String updateSQL = "UPDATE " + tableName + " SET"
            + " info = map('year', 32), pets = array('Dog', 'Cat', 'Pig'),"
            + " user_info = named_struct('name', 'Vasco', 'surname', 'da Gama','age', 558,'gender', 'MALE')"
            + " WHERE doc_id = '1'";

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

            UpdateComplexDataTypes update = new UpdateComplexDataTypes();

            creatingTable(state,"CREATE TABLE",
                    update.tableName, update.tableFields,
                    getTableProperties(update.tableName, update.key));
            System.out.println("INFO: -= Inserting to " + update.tableName + "...");
            state.execute(update.insertSQL);
            selectAllFromTable(state, update.tableName, 8);

            update.run(state);
            selectAllFromTable(state, update.tableName, 8);

        } catch(SQLException sqlException) {
            System.err.println("ERROR: -= Got a sql exception");
            sqlException.printStackTrace();
        } catch(Exception e) {
            System.err.println("ERROR: -= Got an exception");
            e.printStackTrace();
        }
    }

}
