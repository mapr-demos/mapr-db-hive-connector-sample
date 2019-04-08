package com.mapr.hiveconnector.examples.merging;

import com.mapr.hiveconnector.utils.DaoManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.mapr.hiveconnector.utils.TableManager.*;

/**
 * The class MergeWhenIdIsJoinKey describes how to
 * merge two tables in a case when id is the join key using Hive MapR DB JSON connector.
 */
public class MergeWhenIdIsJoinKey {

    /**
     * The source table name.
     */
    private String tableSrc = "customer_source_java";

    /**
     * The target table name.
     */
    private String tableTar = "customer_db_json_target_java";

    /**
     * The key field to bound Hive table with Mapr DB JSON table.
     */
    private String key = "id";

    /**
     * The tables' fields.
     */
    private String tablesField = " (id string, first_name string, last_name string, age int) ";

    /**
     * The insertSrcSQL1..2 statements for inserting values into source table.
     */
    private String insertSrcSQL1 = "INSERT INTO TABLE " + tableSrc + " VALUES ('001', 'Dorothi', 'Hogward', 7777)";
    private String insertSrcSQL2 = "INSERT INTO TABLE " + tableSrc + " VALUES ('002', 'Alex', 'Bowee', 7777)";
    private String insertSrcSQL3 = "INSERT INTO TABLE " + tableSrc + " VALUES ('088', 'Robert', 'Dowson', 25)";

    /**
     * The insertTarSQL1..2 statements for inserting values into target table.
     */
    private String insertTarSQL1 = "INSERT INTO TABLE " + tableTar + " VALUES ('001', 'John', 'Smith', 45)";
    private String insertTarSQL2 = "INSERT INTO TABLE " + tableTar + " VALUES ('002', 'Michael', 'Watson', 27)";
    private String insertTarSQL3 = "INSERT INTO TABLE " + tableTar + " VALUES ('003', 'Den', 'Brown', 33)";

    /**
     * The property 'hive.auto.convert.join' disables
     * optimize Auto Join Conversion and avoid exceptions due to  merging.
     */
    private String propHiveAutoConvert = "set hive.auto.convert.join=false";

    /**
     * The mergeSQL statement for merging values in the tables.
     */
    private String mergeSQL = "MERGE into " + tableTar + " trg USING " + tableSrc + " src ON src.id = trg.id "
            + " WHEN MATCHED THEN UPDATE SET age = src.age"
            + " WHEN NOT MATCHED THEN INSERT VALUES (src.id, src.first_name, src.last_name, src.age)";

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

            MergeWhenIdIsJoinKey merge = new MergeWhenIdIsJoinKey();

            creatingTable(state,"CREATE TABLE",
                    merge.tableTar, merge.tablesField,
                    getTableProperties(merge.tableTar, merge.key));

            creatingTable(state,"CREATE TABLE",
                    merge.tableSrc, merge.tablesField,
                    getTableProperties(merge.tableSrc, merge.key));

            System.out.println("INFO: -= Inserting data to " + merge.tableTar + "...");
            state.execute(merge.insertTarSQL1);
            state.execute(merge.insertTarSQL2);
            state.execute(merge.insertTarSQL3);

            System.out.println("INFO: -= Inserting data to " + merge.tableSrc + "...");
            state.execute(merge.insertSrcSQL1);
            state.execute(merge.insertSrcSQL2);
            state.execute(merge.insertSrcSQL3);

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
