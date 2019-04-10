package com.mapr.hiveconnector.utils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * The class DaoManager is responsible for connecting to the Hive2 server.
 */
public class DaoManager {

    /**
     * The properties.
     */
    private Properties properties;

    /**
     * The driver to the database.
     */
    private String driver;

    /**
     * The url to the database.
     */
    private String url;

    /**
     * The database's name.
     */
    private String dataBaseName;

    /**
     * The user of the database.
     */
    private String user;

    /**
     * The password to database.
     */
    private String password;

    /**
     * The connection host for driver.
     */
    private String host;

    /**
     * The connection port for driver.
     */
    private String port;

    /**
     * The connection string for driver.
     */
    private String connectionString;

    /**
     * The DaoManager field.
     */
    private static DaoManager manager;

    /**
     * The static method returns single instance of DaoManager.
     * Works correctly in one thread mode.
     * @return instance of DaoManager.
     */
    public static DaoManager getInstance() {
        if (manager == null) {
            manager = new DaoManager();
            manager.initDataBase();
        }
        return manager;
    }

    /**
     * The private constructor to avoid creating additional instances of a class.
     */
    private DaoManager() {
    }

    /**
     * The method initializes the database from system variable.
     */
    private void initDataBase() {
        this.driver = System.getenv("HIVE_DRIVER_CLASS_NAME");
        this.url = System.getenv("HIVE_DRIVER_CONNECTION_URL");
        this.host = System.getenv("HIVE_HOST");
        this.port = System.getenv("HIVE_PORT");
        this.dataBaseName = System.getenv("HIVE_DATABASE_NAME");
        this.user = System.getenv("HIVE_DATABASE_USER");
        this.password = System.getenv("HIVE_DATABASE_USER_PASSWORD");
        if (this.checkConfig()) {
            System.out.println("INFO: -= Database is able to initialize with configuration system properties");
        } else {
            this.initDataBaseFromProp();
        }
    }

    /**
     * The method initializes the database from properties.
     */
    private void initDataBaseFromProp() {
        this.properties = new Properties();
        try (InputStream stream = ClassLoader.getSystemResourceAsStream("hive_mapr_db.properties")) {
            this.properties.load(stream);
            this.driver = properties.getProperty("driverClassName");
            this.url = properties.getProperty("url");
            this.host = properties.getProperty("host");
            this.port = properties.getProperty("port");
            this.dataBaseName = properties.getProperty("databaseName");
            this.user = properties.getProperty("user");
            this.password = properties.getProperty("password");
            if (this.checkConfig()) {
                System.out.println("INFO: -= Database initialized from properties with default configuration");
            } else {
                System.err.println("ERROR: -= Failed initialisation Hive database");
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

    /**
     * The method checks driver manager configuration.
     * @return true in case all
     */
    private boolean checkConfig() {
        if (this.driver != null & this.url != null & this.dataBaseName != null
                & this.host != null & this.port != null
                & this.user != null & this.password != null) {
            this.connectionString = String.format("%s%s:%s/%s", this.url, this.host, this.port, this.dataBaseName);
            return true;
        }
        return false;
    }

    /**
     * The connection method creates a singleton connection to the DB.
     * @return connection in case it is not null; otherwise return null.
     */
    public Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName(this.driver);
            connection = DriverManager.getConnection(this.connectionString, this.user, this.password);
            if (connection != null) {
                System.out.println("INFO: -= Connection established");
                return connection;
            }
        } catch (SQLException | ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
        return connection;
    }

    /**
     * The method sets configuration parameters given via command line arguments.
     * @param args configuration parameters given via command line arguments.
     */
    public void setConfigParam(String[] args) {
        if (args.length == 7) {
            this.driver = args[0];
            this.url = args[1];
            this.host = args[2];
            this.port = args[3];
            this.dataBaseName = args[4];
            this.user = args[5];
            this.password = args[6];
            if (!this.checkConfig()) {
                System.out.println("INFO: -= Bud command line configuration parameter(s)");
                System.out.println("INFO: -= Try to use system / default configuration");
                this.initDataBase();
            } else {
                System.out.println("INFO: -= Use configuration parameters given via command line arguments");
            }
        } else if (args.length > 1 & args.length < 7){
            System.out.println("INFO: -= You miss command line configuration parameter(s)");
            System.out.println("INFO: -= Will use system / default configuration");
        } else if (args.length > 7){
            System.out.println("INFO: -= You pass extra command line configuration parameter(s)");
            System.out.println("INFO: -= Will use system / default configuration");
        }
    }

}
