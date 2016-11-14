/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 * The Following tables are used by Morgan's program (4/11/2016)
    
Laravel tables (don't need to copy)
    sessions
    password_reminders
    failed_jobs
    cache
    
App tables
    users
    dashboard
    sites
    sensortype

Views
    SensorAnalysis
    UserLines
    UserLocations
    UserSites
    
Tables used by views

    
 */
package tegnonanalysis;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * THis ws designed to transfer SenorDataNormal records
 * from Microsoft SQL server to Mysql
 * 
 * The process worked mid Aug 2016.
 * Several enhancements made amazing performance difference
 * 
 * 1) Used sql.batchAdd() to batch 1000 records at a time
 * 2) Used 'begin' and 'commit' after every batch
 * 3) Order by id - the inserts apparently go much much faster
 * 
 * There is still some MySQL housekeeping that improves performance
 *        probably overnight. I do not know how to invoke it manually
 * 
 * As a result there is a gradual degradation in performance over time
 *     Tested on 10M inserts in about 30 minutes (3K5 records a second)
 *     The original, without the above optimisations performed +- 100/second.
 * 
 * In order to satisfy foreign key constraints, many of the other tables are required
 *   Suitable transfer mechanisms were built.
 * 
 *
 * @author chris
 */
public class TegnonTransfer {
    
    static boolean TEST_RUN = true;
    

    static final int LOG_SIZE = 1000000;
    static final int LOG_ROTATION_COUNT = 10;

    public static Connection conn;
    public static Connection mysqlConn;
    static public final Logger tegnonLogger = Logger.getLogger("tegnonanTransfer");
    static Handler logHandler = null;

    static final Logger logger = TegnonTransfer.tegnonLogger.getLogger("tegnonTransfer");
    static final DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    static {
        try {
            
            if (TEST_RUN) {
                new File("/home/chris/Tegnon/test/logs/").mkdirs();
                logHandler = new FileHandler("/home/chris/Tegnon/test/logs/transfer.log", LOG_SIZE, LOG_ROTATION_COUNT);
            } else {
                new File("logs").mkdir();
                logHandler = new FileHandler("c:/inetpub/wwwroot/app/storage/TegnonLogs/TegnonAnalysis.log", LOG_SIZE, LOG_ROTATION_COUNT);
            }
            logHandler.setFormatter(new SimpleFormatter());
            logger.setLevel(Level.INFO);
            logger.setUseParentHandlers(false);

            Handler h[] = logger.getHandlers();
            logger.info("There were " + h.length + " log handlers");
            logger.addHandler(logHandler);
            h = logger.getHandlers();
            logger.info("There are " + h.length + " log handlers");
        } catch (Exception exc) {
            System.out.println("Failed to create a log ... Aaargh");
            System.exit(2);
        }

    }

    static public void connectSQL() {
        // Create a variable for the connection string.
        
        String username = "javaUser1";
        String password = "sHxXWij02AE4ciJre7yX";

        String connectionUrl = "jdbc:jtds:sqlserver://localhost/TegnonEfficiency";
        //String connectionUrl = "jdbc:sqlserver://localhost:1433;databasename=TegnonEfficiency";
       
        if (TEST_RUN) {
            connectionUrl = "jdbc:mysql://localhost/TegnonEfficiency";
            username = "root";
            password = "tttoyfJ1_3";
        }
        try {

       //     Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            conn = DriverManager.getConnection(connectionUrl, username, password);
            System.out.println("Connection to MS SQL Succeeded");

        } catch (Exception exc) {
            exc.printStackTrace();
            logger.log(Level.SEVERE, exc.getMessage(), exc);
            System.exit(3);
        }

    }

    static public void connectMySQL() {
        // Create a variable for the connection string.
        String username = "javaUser1";
        String password = "sHxXWij02AE4ciJre7yX";

        String connectionUrl = "jdbc:mysql://localhost/TegnonEfficiency";

        if (TEST_RUN) {
            connectionUrl = "jdbc:mysql://localhost/TegnonNormal";
            username = "root";
            password = "tttoyfJ1_3";
        }
        
        try {

            //Class.forName("com.mysql.MySQLDriver");
            mysqlConn = DriverManager.getConnection(connectionUrl, username, password);
            System.out.println("Connection to MySQL Succeeded");

        } catch (Exception exc) {
            exc.printStackTrace();
            logger.log(Level.SEVERE, exc.getMessage(), exc);
            System.exit(3);
        }

    }

    /**
     * @param args the command line arguments
     *
     * The main purpose is to - transfer data from sensordataNormal in MSSQL to
     * MySQL - Clean out MSSQL for sensordataNormal to keep overall database
     * size < 10Gb - Calculate SensorDataHalfHour and SensorDataHour based on
     * data in SensorDataNormal
     *
     * - Provide test data in MySQL so we can test new features offline
     */
    public static void main(String[] args) {

        connectSQL();
        connectMySQL();

        try {
            // measurementType
            MeasurementType measurementType = new MeasurementType();
            measurementType.transfer(conn, mysqlConn);
            // deviceType  (unknown... all arduino / Raspberry)

            // UnitOfMEasure (no such table)
            // sensorType
            SensorType sensorType = new SensorType();
            sensorType.transfer(conn, mysqlConn);
            // sensorUnit
            SensorUnit sensorUnit = new SensorUnit();
            sensorUnit.transfer(conn, mysqlConn);

            // SensorStatus
            SensorStatus sensorStatus = new SensorStatus();
            sensorStatus.transfer(conn, mysqlConn);

            // sensorCalculatedType
            SensorCalculatedType sensorCalculatedType = new SensorCalculatedType();
            sensorCalculatedType.transfer(conn, mysqlConn);

            // sensorCalculatedUnits
            SensorCalculatedUnit sensorCalculatedUnit = new SensorCalculatedUnit();
            sensorCalculatedUnit.transfer(conn, mysqlConn);

            // 
            // countries
            Country country = new Country();
            country.transfer(conn, mysqlConn);
            // client
            Client client = new Client();
            client.transfer(conn, mysqlConn);

            // site
            Site site = new Site();
            site.transfer(conn, mysqlConn);
            // network
            Network network = new Network();
            network.transfer(conn, mysqlConn);
            // location
            Location location = new Location();
            location.transfer(conn, mysqlConn);

            // TegnonLine
             TegnonLine tegnonLine = new TegnonLine();
            tegnonLine.transfer(conn, mysqlConn);
            
            // device
            Device device = new Device();
            device.transfer(conn, mysqlConn);

            // sensors
            Sensor sensor = new Sensor();
            sensor.transfer(conn, mysqlConn);

                // Roles
            Role role = new Role();
            role.transfer(conn, mysqlConn);
         
             // Users
            User user = new User();
            user.transfer(conn, mysqlConn);
            
             // User_has_sites
            UserHasSites userSites = new UserHasSites();
            userSites.transfer(conn, mysqlConn);
            
            //Needed
            AssignedRole assignedRole = new AssignedRole();
            assignedRole.transfer(conn, mysqlConn);
            
            
           UserHasClient userClient = new UserHasClient();
           userClient.transfer(conn, mysqlConn);
            
            
            // Superfluous tables
            //   persons, profession??. roles_has_permissions, TegnonPermission, userDevice,  UserSensor
            // FileMap, loadStats
            // sensorData??
            // sensorDataDewpoint??
            // SensorDataHour
            // SensorDataHalfHour
            // SensorDataImportLogs?? SensorDataImportStaging??
            // SensorDataNormal (the big huge)
            //SensorDataNormal sdn = new SensorDataNormal();
            /*
             
               SELECT TABLE_NAME AS 'Table Name', TABLE_ROWS AS 'Rows' FROM information_schema.TABLES 
WHERE TABLES.TABLE_SCHEMA = 'TegnonEfficiency' AND TABLES.TABLE_TYPE = 'BASE TABLE'
order by Rows desc; 
                
            */
           /*
            SensorDataNormal.transfer(conn, mysqlConn
                    , LocalDateTime.from(df.parse("2015-08-01 00:00:00")), LocalDateTime.from(df.parse("2016-09-01 00:00:00")),0,999999999);
                  //  , LocalDateTime.from(df.parse("2016-05-01 00:00:00")), LocalDateTime.from(df.parse("2016-06-01 00:00:00")),0,690);
                  //, LocalDateTime.from(df.parse("2016-06-01 00:00:00")), LocalDateTime.from(df.parse("2016-07-01 00:00:00")),0,9999999);
                  //, LocalDateTime.from(df.parse("2016-07-01 00:00:00")), LocalDateTime.from(df.parse("2016-08-01 00:00:00")),0,999999999);
                  //, LocalDateTime.from(df.parse("2016-08-01 00:00:00")), LocalDateTime.from(df.parse("2016-09-01 00:00:00")),0,999999999);
                 // , LocalDateTime.from(df.parse("2016-09-01 00:00:00")), LocalDateTime.from(df.parse("2016-10-01 00:00:00")),0,690);
                 // , LocalDateTime.from(df.parse("2016-10-01 00:00:00")), LocalDateTime.from(df.parse("2016-11-01 00:00:00")),0,690);
                
            */
        } catch (Exception exc) {
            System.out.println("Main Fialed " + exc.getLocalizedMessage());
            exc.printStackTrace();
            logger.severe(exc.getLocalizedMessage());
        }

    }

}
