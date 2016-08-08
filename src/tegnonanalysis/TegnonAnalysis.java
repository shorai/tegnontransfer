/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tegnonanalysis;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.format.DateTimeFormatter;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 *
 * @author chris
 */
public class TegnonAnalysis {
    
    static final int LOG_SIZE = 1000000;
    static final int LOG_ROTATION_COUNT = 10;
    
    public static Connection conn;
    public static Connection mysqlConn;
    static public final Logger tegnonLogger = Logger.getLogger("tegnonanalysis");
    static Handler logHandler = null;
    
    static final Logger logger = TegnonAnalysis.tegnonLogger.getLogger("tegnonanalysis");
    static final DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    static {
        try {
            new File("logs").mkdir();
            logHandler = new FileHandler("c:/inetpub/wwwroot/app/storage/TegnonLogs/TegnonAnalysis.log", LOG_SIZE, LOG_ROTATION_COUNT);
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
        
        try {
            
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
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
        
        try {
            
            Class.forName("com.mysql.MySQLDriver");
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
     * The main purpose is to
     *      - transfer data from sensordataNormal in MSSQL to MySQL
     *      - Clean out MSSQL for sensordataNormal to keep overall database size < 10Gb
     *      - Calculate SensorDataHalfHour and SensorDataHour based on data in SensorDataNormal
     * 
     *      - Provide test data in MySQL so we can test new features offline
     */
    public static void main(String[] args) {
        
        connectSQL();
        connectMySQL();
            
        try {            
            SensorDataNormal sdh = new SensorDataNormal();
            sdh.transfer(conn, mysqlConn);      
        } catch (Exception exc) {
            logger.severe(exc.getLocalizedMessage());
        }
        // TODO code application logic here
    }
    
}
