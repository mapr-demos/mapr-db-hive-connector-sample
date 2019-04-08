package com.mapr.hiveconnector.examples.inserting;

import com.mapr.hiveconnector.examples.CreateTablePrimitiveTypes;
import com.mapr.hiveconnector.utils.DaoManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.mapr.hiveconnector.utils.TableManager.existTable;

/**
 * The class InsertIntoTablePrimitiveTypes is responsible
 * for populating the table "primitive_types_from_java" with values.
 */
public class InsertIntoTablePrimitiveTypes {

    /**
     * The table name.
     */
    private String tableName = "primitive_types_from_java";

    /**
     * The insertSQL for inserting values into table.
     */
    private String insertSQL = "INSERT INTO TABLE " + tableName + " VALUES"
            + " ('2', true, 124.14, '2017-11-29', 9192.12,"
            + " 214566190, 'text', '2017-03-17 00:14:13', 125,"
            + " 9223372036854775806, 23434, \"binary string\")";

    /**
     * The method run demonstrates example.
     * @param state SQL statement.
     * @throws SQLException if Sql exception occurs.
     */
    public void run(Statement state) throws SQLException {
        System.out.println("INFO: -= Populating table " + tableName + " with data ...");
        state.execute(insertSQL);
        System.out.println("INFO: -= All data inserted");
    }

    /**
     * The main.
     * @param args arrays of arguments.
     */
    public static void main(String[] args) {
        try (Connection connection = DaoManager.getInstance().getConnection();
             Statement state = connection.createStatement()) {

            InsertIntoTablePrimitiveTypes insertPrimitiveTypes = new InsertIntoTablePrimitiveTypes();
            if (!existTable(state, insertPrimitiveTypes.tableName)) {
                new CreateTablePrimitiveTypes().run(state);
            }
            insertPrimitiveTypes.run(state);

        } catch(SQLException sqlException) {
            System.err.println("ERROR: -= Got a sql exception");
            sqlException.printStackTrace();
        } catch(Exception e) {
            System.err.println("ERROR: -= Got an exception");
            e.printStackTrace();
        }
    }

}
