package utils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
     * The method initializes the database.
     */
    private void initDataBase() {
        this.properties = new Properties();
        try (InputStream stream = ClassLoader.getSystemResourceAsStream("hive_mapr_db.properties")) {
            this.properties.load(stream);
            this.driver = properties.getProperty("driverClassName");
            this.url = properties.getProperty("uri");
            this.dataBaseName = properties.getProperty("databaseName");
            this.user = properties.getProperty("user");
            this.password = properties.getProperty("password");
            System.out.println("Database initialized");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * The method creates a singleton connection to the DB.
     * @return connection.
     */
    public Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName(this.driver);
            connection = DriverManager.getConnection(this.url + this.dataBaseName, this.user, this.password);
        } catch (SQLException | ClassNotFoundException e) {
            System.out.println(e.getMessage());
        }
        System.out.println("Connection established");
        return connection;
    }
}
