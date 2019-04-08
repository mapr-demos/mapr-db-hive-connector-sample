package com.mapr.hiveconnector.examples.merging;

import com.mapr.hiveconnector.utils.DaoManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.mapr.hiveconnector.utils.TableManager.*;
import static com.mapr.hiveconnector.utils.TableManager.selectAllFromTable;

/**
 * The class MergeMixedDataTypes describes merging operation
 * on mixed data types, such as arrays, maps, and structures,
 * using Hive MapR DB JSON connector.
 */
public class MergeMixedDataTypes {

    /**
     * The source table name.
     */
    private String tableSrc = "mixed_types_source_java";

    /**
     * The target table name.
     */
    private String tableTar = "mixed_types_target_java";

    /**
     * The key field to bound Hive table with Mapr DB JSON table.
     */
    private String key = "doc_id";

    /**
     * The tables' fields.
     */
    private String tablesField = " (doc_id string, "
            + " user_info struct <name:STRING,surname:STRING,age:INT,gender:STRING>) ";

    /**
     * The insertSrcSQL statements for inserting values into source table.
     */
    private String insertSrcSQL = "INSERT INTO TABLE " + tableSrc
            + " SELECT '1', named_struct('name', 'Brandon', 'surname', 'Lee', 'age', 31, 'gender', 'MALE')"
            + " UNION ALL"
            + " SELECT '2', named_struct ('name', 'Johnson', 'surname', 'Fall', 'age', 23, 'gender', 'MALE')"
            + " UNION ALL"
            + " SELECT '3', named_struct ('name', 'Mary', 'surname', 'Dowson', 'age', 11, 'gender', 'FEMALE')"
            + " UNION ALL"
            + " SELECT '4', named_struct ('name', 'Paul', 'surname', 'Rodgers', 'age', 41, 'gender', 'MALE')";

    /**
     * The insertTarSQL statements for inserting values into target table.
     */
    private String insertTarSQL = "INSERT INTO TABLE " + tableTar
            + " SELECT '1', named_struct ('name', 'Lexx', 'surname', 'Comfuzer', 'age', 31, 'gender', 'MALE')";

    /**
     * The property 'hive.auto.convert.join' disables
     * optimize Auto Join Conversion and avoid exceptions due to  merging.
     */
    private String propHiveAutoConvert = "set hive.auto.convert.join=false";

    /**
     * The mergeSQL statement for merging values in the tables.
     */
    private String mergeSQL = "MERGE INTO " + tableTar + " trg USING " + tableSrc + " src ON src.doc_id = trg.doc_id"
            + " WHEN MATCHED THEN UPDATE SET user_info = src.user_info "
            + " WHEN NOT MATCHED THEN INSERT VALUES (src.doc_id, src.user_info)";

    /**
     * The method run demonstrates example.
     * @param state SQL statement.
     * @throws SQLException if Sql exception occurs.
     */
    public void run(Statement state) throws SQLException {
        System.out.println("INFO: -= Merging tables " + tableTar + " and " + tableSrc + " ...");
        state.execute(mergeSQL);
        System.out.println("INFO: -= Merging is done");
    }

    /**
     * The main.
     * @param args arrays of arguments.
     */
    public static void main(String[] args) {
        try (Connection connection = DaoManager.getInstance().getConnection();
             Statement state = connection.createStatement()) {

            MergeMixedDataTypes merge = new MergeMixedDataTypes();

            creatingTable(state,"CREATE TABLE",
                    merge.tableTar, merge.tablesField,
                    getTableProperties(merge.tableTar, merge.key));

            creatingTable(state,"CREATE TABLE",
                    merge.tableSrc, merge.tablesField,
                    getTableProperties(merge.tableSrc, merge.key));

            System.out.println("INFO: -= Inserting data to " + merge.tableTar + "...");
            state.execute(merge.insertTarSQL);

            System.out.println("INFO: -= Inserting data to " + merge.tableSrc + "...");
            state.execute(merge.insertSrcSQL);

            System.out.println("INFO: -= The target table:");
            selectAllFromTable(state, merge.tableTar, 5);

            System.out.println("INFO: -= The source table:");
            selectAllFromTable(state, merge.tableSrc, 5);

            state.execute(merge.propHiveAutoConvert);
            merge.run(state);
            selectAllFromTable(state, merge.tableTar, 10);

        } catch(SQLException sqlException) {
            System.err.println("ERROR: -= Got a sql exception");
            sqlException.printStackTrace();
        } catch(Exception e) {
            System.err.println("ERROR: -= Got an exception");
            e.printStackTrace();
        }
    }

}
