package com.mapr.hiveconnector.examples.loading;

import com.mapr.hiveconnector.utils.DaoManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.mapr.hiveconnector.utils.TableManager.*;

/**
 * The class LoadDataToTables is responsible for
 * loading data to Mapr DB JSON table using
 * Hive MapR Database JSON Connector.
 */
public class LoadDataToTables {

    /**
     * The intermediate table name.
     */
    private static String intermediateTableName = "artists_load_java";

    /**
     * The table name.
     */
    private static String tableName = "artists_java";

    /**
     * The table's fields.
     */
    private static String tableFields  = " (`_id` string, "
            + "albums array<struct<cover_image_url:string, id:string, name:string, slug:string>>, "
            + "area string, deleted boolean, disambiguation_comment string, gender string, "
            + "images_urls array<string>, mbid string, name string, profile_image_url string, "
            + "rating double, slug_name string) ";

    /**
     * The insertSQL is inserting data
     * from intermediate table to a hive and Mapr DB tables.
     */
    private static String insertSQL = "INSERT INTO TABLE " + tableName + " SELECT "
            + "`_id` string, albums, area, deleted, disambiguation_comment, "
            + "gender, images_urls, mbid, name, profile_image_url, rating, "
            + "slug_name FROM " + intermediateTableName;

    /**
     * The main.
     * @param args arrays of arguments.
     */
    public static void main(String[] args) {
        DaoManager.getInstance().setConfigParam(args);
        try (Connection connection = DaoManager.getInstance().getConnection();
             Statement state = connection.createStatement()) {

            addSerDe(state);
            creatingTable(state, "CREATE EXTERNAL TABLE",
                    intermediateTableName, tableFields, intermediateProperties);

            state.execute("LOAD DATA LOCAL INPATH '/tmp/dataset/artists.json' OVERWRITE "
                    + "INTO TABLE " + intermediateTableName);
            System.out.println("INFO: -= The data is loaded into intermediate table");

            creatingTable(state, "CREATE TABLE",
                    tableName, tableFields, getTableProperties(tableName, "_id"));

            state.execute(insertSQL);
            System.out.println("INFO: -= The data is inserted into both tables");

        } catch(SQLException sqlException) {
            System.err.println("ERROR: -= Got a sql exception");
            sqlException.printStackTrace();
        } catch(Exception e) {
            System.err.println("ERROR: -= Got an exception");
            e.printStackTrace();
        }
    }

}
