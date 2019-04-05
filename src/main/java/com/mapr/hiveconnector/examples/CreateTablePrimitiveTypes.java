package com.mapr.hiveconnector.examples;

import com.mapr.hiveconnector.utils.DaoManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The class CreateTablePrimitiveTypes is responsible
 * for creating table in the database.
 */
public class CreateTablePrimitiveTypes {

    /**
     * The table name.
     */
    private static String tableName = "primitive_types_from_java";

    /**
     * The SQL createSQL for creating the primitive types table.
     */
    private static String createSQL = "CREATE TABLE " + tableName + "("
            + "doc_id string, "
            + "bo boolean, "
            + "d double, "
            + "da date, "
            + "f float, "
            + "i int, "
            + "s string, "
            + "ts timestamp, "
            + "ti tinyint, "
            + "bi bigint, "
            + "si smallint, "
            + "bin binary) "
            + "STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler'"
            + "TBLPROPERTIES(\"maprdb.table.name\" = \"/primitive_types_from_java\","
            + "\"maprdb.column.id\" = \"doc_id\")";

    /**
     * The main.
     * @param args arrays of arguments.
     */
    public static void main(String[] args) {

        try (Connection connection = DaoManager.getInstance().getConnection();
             Statement state = connection.createStatement()) {

            System.out.println("INFO: -= Creating table " + tableName + "...");
            state.execute("DROP TABLE IF EXISTS " + tableName);
            state.execute(createSQL);
            System.out.println("INFO: -= Table " + tableName + " is created");

        } catch(SQLException sqlException) {
            System.err.println("ERROR: -= Got a sql exception");
            sqlException.printStackTrace();
        } catch(Exception e) {
            System.err.println("ERROR: -= Got an exception");
            e.printStackTrace();
        }
    }

}