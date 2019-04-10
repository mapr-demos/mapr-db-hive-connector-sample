package com.mapr.hiveconnector.examples.inserting;

import com.mapr.hiveconnector.utils.DaoManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.mapr.hiveconnector.utils.TableManager.existTable;
import static com.mapr.hiveconnector.utils.TableManager.selectAllFromTable;

/**
 * The class MultipleRowInsert describes how to
 * insert a multiple table row into a nested MapR DB table.
 */
public class MultipleRowInsert {

    /**
     * The table name.
     */
    private String tableName = "nested_data_java";

    /**
     * The multipleRowInsertDummySQL statement for inserting values into table using dummy table.
     */
    private String multipleRowInsertDummySQL = "WITH dummy_table AS (SELECT '003' AS KEY,"
            + " '3' AS num,"
            + " MAP ('Rony',"
            + " Named_struct ('user_id', '1', 'address', '4333 Backer str', 'zip', '12311', 'country', 'Hun'))"
            + " AS postal_addresses"
            + " UNION ALL SELECT '004' AS KEY,"
            + " '4' AS num, MAP ('Ivan',"
            + " Named_struct ('user_id', '1', 'address', '833 Bridle Avenue', 'zip', '95111', 'country', 'CA'))"
            + " AS postal_addresses"
            + " UNION ALL SELECT '005' AS KEY,"
            + " '5' AS num, MAP ('Ivan',"
            + " Named_struct ('user_id', '1', 'address', '664 Devon Ave', 'zip', '92021', 'country', 'Tog'))"
            + " AS postal_addresses)"
            + " INSERT INTO " + tableName + " SELECT * FROM dummy_table";

    /**
     * The multipleRowInsertIntoSQL statement for inserting values into table using INSERT INTO statement.
     */
    private String multipleRowInsertIntoSQL = "INSERT INTO TABLE " + tableName + " SELECT '006',"
            + " '6', MAP ('Rony',"
            + " Named_struct ('user_id', '1', 'address', '150 National City', 'zip', '91950', 'country', 'Hun'))"
            + " UNION ALL SELECT '007', '7',"
            + " MAP ('Tomason',"
            + " Named_struct ('user_id', '1', 'address', '272 Ocean Circle' , 'zip', '92801', 'country', 'CA'))"
            + " UNION ALL SELECT '008', '8',"
            + " MAP ('Davin',"
            + " Named_struct ('user_id', '1', 'address', '81 Augusta Ave', 'zip', '93905', 'country', 'CA'))";

    /**
     * The method run demonstrates example.
     * @param state SQL statement.
     * @throws SQLException if Sql exception occurs.
     */
    public void run(Statement state) throws SQLException {

        System.out.println("INFO: -= Inserting data via dummy table ...");
        state.execute(multipleRowInsertDummySQL);
        System.out.println("INFO: -= The data are inserted via dummy table:");
        selectAllFromTable(state, tableName, 10);

        System.out.println("INFO: -= Inserting data using INSERT INTO statement ...");
        state.execute(multipleRowInsertIntoSQL);
        System.out.println("INFO: -= The data are inserted via INSERT INTO statement:");
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

            MultipleRowInsert multipleRowInsert = new MultipleRowInsert();

            if (!existTable(state, multipleRowInsert.tableName)) {
                new SingleRowInsert().createNestedDataTable(state);
            }

            multipleRowInsert.run(state);
        } catch(SQLException sqlException) {
            System.err.println("ERROR: -= Got a sql exception");
            sqlException.printStackTrace();
        } catch(Exception e) {
            System.err.println("ERROR: -= Got an exception");
            e.printStackTrace();
        }
    }

}