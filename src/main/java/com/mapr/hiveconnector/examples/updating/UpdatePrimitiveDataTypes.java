package com.mapr.hiveconnector.examples.updating;

import com.mapr.hiveconnector.examples.CreateTablePrimitiveTypes;
import com.mapr.hiveconnector.examples.inserting.InsertIntoTablePrimitiveTypes;
import com.mapr.hiveconnector.utils.DaoManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.mapr.hiveconnector.utils.TableManager.selectAllFromTable;

/**
 * The class UpdatePrimitiveDataTypes describes how to
 * update a primitive data types in Mapr DB using hive MapR DB JSON connector.
 */
public class UpdatePrimitiveDataTypes {

    /**
     * The table name.
     */
    public String tableName = "primitive_types_from_java";

    /**
     * The updateSQL for updating values into table.
     */
    private String updateSQL = "UPDATE " + tableName + " SET "
            + " da = '2018-12-11',"
            + " bo = FALSE,"
            + " f = 91.777 WHERE doc_id = '2'";

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

            UpdatePrimitiveDataTypes update = new UpdatePrimitiveDataTypes();
            CreateTablePrimitiveTypes primitiveTypes = new CreateTablePrimitiveTypes();
            primitiveTypes.run(state);
            new InsertIntoTablePrimitiveTypes().run(state);
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

