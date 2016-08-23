/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tegnonanalysis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
//import static tegnonload.PiLine.df;

/**
 * Singleton pattern since we always deal with a single instance at a time
 *
 * Later versions may optimise by reading an array for an attachment or device
 *Processed 23000000
Starting sensorId:1075 at:2016-08-16 21:42:42 Counter:23002504
Starting sensorId:1076 at:2016-08-16 21:44:17 Counter:23020342
Starting sensorId:1077 at:2016-08-16 21:45:50 Counter:23038180
Starting sensorId:1078 at:2016-08-16 21:47:28 Counter:23056018
Starting sensorId:1085 at:2016-08-16 21:49:05 Counter:23073856
Starting sensorId:1086 at:2016-08-16 21:49:58 Counter:23083934
Starting sensorId:1087 at:2016-08-16 21:50:50 Counter:23094012
Processed 23100000
Starting sensorId:1088 at:2016-08-16 21:51:41 Counter:23104090
Transfer SensorDataNormal complete after processing  23114168 records, inserts = 23114168 Updates=0
Started At2016-08-15 12:46:15
Ended At2016-08-16 21:52:33
Hours:33  Minutes:6
* 
* About 200 transactions per second
* 
* Using batched updates and committing transacttions every 1000 records we get
* 
* 
Starting sensorId:15339 at:2016-08-01 00:30:05  Time:2016-08-22 08:46:28 Count:59596     Seconds:19
Starting sensorId:15340 at:2016-08-01 00:30:05  Time:2016-08-22 08:46:47
Processed 7100000
 Count:59596     Seconds:19
Starting sensorId:15341 at:2016-08-01 00:00:05  Time:2016-08-22 08:47:06 Count:61246     Seconds:20
Starting sensorId:15342 at:2016-08-01 00:00:05  Time:2016-08-22 08:47:26
Processed 7200000
 Count:61246     Seconds:19
Starting sensorId:15343 at:2016-08-01 00:00:05  Time:2016-08-22 08:47:46
Processed 7300000
 Count:61246     Seconds:19
Starting sensorId:15344 at:2016-08-01 00:00:05  Time:2016-08-22 08:48:06Transfer SensorDataNormal complete after process
ing  7382845 records, inserts = 7382845 Updates=0
Started At2016-08-22 08:18:17
Ended At2016-08-22 08:48:25
Hours:0  Minutes:30
Porcessed for sensors 0 to 999999999  Times 2016-08-01 00:00:00 to 2016-09-01 00:00:00
* 
* This is about  4101 per second .... ten to twenty times faster than  uncommited
* The process slows down from +- 5500 per second to 3000 per second
* MySQL must reorganise itself as time goes by because repeating the operation with new data say 24 hours later  gives same result
* 
 * @author chris.rowse
 */
public class SensorDataNormal {

    static final Logger logger = TegnonTransfer.tegnonLogger.getLogger("tegnonanalysis.SensorDataNormal");
    static final DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    static final String loadSQL = "select id, AttachmentID, SensorID, DeviceID,DateTimeStamp,SensorType,"
            + "SensorValue, SensorCalculatedType,SensorCalculatedValue "
            + "from SensorDataNormal "
            + " where dateTimeStamp between ? and ?"
            //+ "where dateTimeStamp between '2015-11-01 00:00:00' and '2016-01-01 00:00:00' "
            //+ "where dateTimeStamp between '2016-01-01 00:00:00' and '2016-03-01 00:00:00' "
            //+ "where dateTimeStamp between '2016-03-01 00:00:00' and '2016-04-01 00:00:00' "
            //+ "where dateTimeStamp between '2016-04-01 00:00:00' and '2016-05-01 00:00:00' "
            //+ "where dateTimeStamp between '2016-05-01 00:00:00' and '2016-06-01 00:00:00' "
            //+ "where dateTimeStamp between '2016-06-01 00:00:00' and '2016-07-01 00:00:00' "
            //+ "where dateTimeStamp between '2016-07-01 00:00:00' and '2016-08-01 00:00:00' "
            //+ "where dateTimeStamp between '2016-08-01 00:00:00' and '2016-09-01 00:00:00' "
            + " and SensorID between ? and ?"
            
            + " and SensorID >= 690"
             + "order by id";
           // + "order by SensorID, DateTimeStamp";

    static PreparedStatement loadStatement = null;
// NB DateTimeStamp is a reserved word in SQL 92  MS SQL should NEVER allow it tio be used as a column name
    static final String insertSql = "insert into SensorDataNormal(AttachmentID, "
            + " SensorId, DeviceID,StampTime,SensorType,"
            + " SensorValue, SensorCalculatedType,SensorCalculatedValue)"
            + " values(?,?,?,?,?,?,?,?)";
    static PreparedStatement insertStatement = null;

    static final String updateSql = "update SensorDataNormal set"
            + " AttachmentId = ?, SensorId = ?, DeviceID = ?, StampTime = ?, SensorType = ?, "
            + " SensorValue=?, SensorCalculatedType=?, SensorCalculatedValue=?"
            + " where id = ?";
    static PreparedStatement updateStatement = null;

    static int numInserts = 0;
    static int numUpdates = 0;
    static int numErrors = 0;
    static  final int cacheSize = 1000;
    static Vector<SensorDataNormal> recordCache = new Vector<>(cacheSize);
  
    //static LocalDateTime firstTime = null;
    //static LocalDateTime lastTime = null;
    // set of all sensors touched by the file
    //  static Set<Sensor> sensorsTouched = new HashSet<>();
    Integer id;
    int attachmentId = 1;  // cant use -1 because it is in a foreign key relationship
    int sensorId;
    int deviceId;
    java.sql.Timestamp timestamp;
    //  Sensor sensor;
    Integer sensorType;
    Double value;
    Integer calcType = new Integer(0);
    Double calcValue = new Double(0.00);

   // static public SensorDataNormal instance = new SensorDataNormal();

    static {
        logger.setUseParentHandlers(false);
        logger.addHandler(TegnonTransfer.logHandler);
    }

    static void init() {
       // instance = new SensorDataNormal();
       for (int i=0; i < cacheSize; i++) 
           recordCache.add(new SensorDataNormal());
       
        logger.setLevel(Level.WARNING);
    }
        
    void bind(ResultSet rs) throws SQLException {
        //id, AttachmentID, SensorID, DeviceID,DateTimeStamp,SensorType,"
        //    + "SensorValue, SensorCalculatedType,SensorCalculatedValue
        int i = 1;
        id = rs.getInt(i++);
        attachmentId = rs.getInt(i++);
        sensorId = rs.getInt(i++);
        deviceId = rs.getInt(i++);
        this.timestamp = rs.getTimestamp(i++);
        this.sensorType = rs.getInt(i++);
        this.value = rs.getDouble(i++);
        this.calcType = rs.getInt(i++);
        this.calcValue = rs.getDouble(i++);
    }

    void update() throws SQLException {
        //ttachmentId = ?, SensorId = ?, DeviceID = ?, DateTimeStamp = ?, SensorType = ?, "
        //    + " SensorValue=?, SensorCalculatedType=?, SensorCalculatedValue=?"
        int i = 1;
        updateStatement.setInt(i++, attachmentId);
        updateStatement.setInt(i++, sensorId);
        updateStatement.setInt(i++, deviceId);
        updateStatement.setTimestamp(i++, timestamp);
        updateStatement.setInt(i++, sensorType);
        updateStatement.setDouble(i++, value);
        updateStatement.setInt(i++, calcType);
        updateStatement.setDouble(i++, calcValue);

        updateStatement.setInt(i++, id);
        updateStatement.addBatch();
        //i = updateStatement.executeUpdate();
        //return i;
    }

    void insert() throws SQLException {
        int i = 1;
        insertStatement.setInt(i++, attachmentId);
        insertStatement.setInt(i++, sensorId);
        insertStatement.setInt(i++, deviceId);
        insertStatement.setTimestamp(i++, timestamp);
        insertStatement.setInt(i++, sensorType);
        insertStatement.setDouble(i++, value);
        insertStatement.setInt(i++, calcType);
        insertStatement.setDouble(i++, calcValue);
        insertStatement.addBatch(); //executeUpdate();
    }

    /** SAve the records in the cache to the database
     *    AS we normally are appending, we simply insert, 
     * Inset will fail for records already on database, we update them
     * This shoud be quick.
     * @param count
     * @throws SQLException 
     */
    static void save(int cachePointer) throws SQLException {
        insertStatement.addBatch("BEGIN");
        for (int i=0; i < cachePointer; i++) {        
                recordCache.elementAt(i).insert();
        }
        insertStatement.addBatch("COMMIT");
        int rs[]  = insertStatement.executeBatch();
        int updates = 0;
        updateStatement.addBatch("BEGIN");
        for (int i=1; i < rs.length-1; i++) {  // BEgin and Commit are results
            if (rs[i] < 1) {
                recordCache.elementAt(i-1).update();
                updates++;
                numUpdates++;
           } else {
            numInserts++;
            }
        }
         updateStatement.addBatch("COMMIT");
        if (updates > 0)
            updateStatement.executeBatch();
        else 
            updateStatement.clearBatch();
        cachePointer=0;
    }

    static void transfer(Connection in, Connection out, LocalDateTime startTime,
            LocalDateTime endTime, int firstSensor, int lastSensor) {

        int count = 0;
        int sensorCount = 0;
        init();
      
        LocalDateTime sensorStart;
        LocalDateTime startAt = LocalDateTime.now();
        System.out.println("Started At: " + df.format(startAt));
        try {
            loadStatement = in.prepareStatement(loadSQL);
            insertStatement = out.prepareStatement(insertSql);
            updateStatement = out.prepareStatement(updateSql);

            loadStatement.setTimestamp(1, java.sql.Timestamp.valueOf(startTime));
            loadStatement.setTimestamp(2, java.sql.Timestamp.valueOf(endTime));
            loadStatement.setInt(3,firstSensor);
            loadStatement.setInt(4, lastSensor);
            ResultSet rs = loadStatement.executeQuery();
           
            int currentSensorId = 0;
            sensorStart = LocalDateTime.now();
            int cachePointer = 0;
            
            while (rs.next()) {
                SensorDataNormal thisx = recordCache.elementAt(cachePointer++);
                thisx.bind(rs);
                if (thisx.sensorId != currentSensorId) {
                    currentSensorId = thisx.sensorId;
                    LocalDateTime ldt = LocalDateTime.now();
                    long  seconds =  sensorStart.until(ldt, ChronoUnit.SECONDS);
                    System.out.print(" Count:"+ sensorCount + " \t Seconds:" + seconds
                            + "\r\nStarting sensorId:" + thisx.sensorId + " at:" 
                            + df.format(thisx.timestamp.toLocalDateTime())                         
                            + "  Time:"+ df.format(ldt));
                    sensorCount = 0;
                    sensorStart = ldt;
                }
                

                if (cachePointer >= cacheSize) {
                    save(cachePointer);
                    cachePointer = 0;
                }
                
                count++;
                sensorCount++;
                if (count % 100000 == 0) {
                    System.out.println("\r\nProcessed " + count);
                }
            }
            if (cachePointer > 0) 
                save(cachePointer);
            
            //int deleted =  deleteStatement.executeUpdate();
            logger.info("Transfer SensorDataNormal complete after processing  " + count
                    + " records, inserts = " + numInserts + " Updates=" + numUpdates);
            System.out.println("Transfer SensorDataNormal complete after processing  "
                    + count + " records, inserts = " + numInserts
                    + " Updates=" + numUpdates);
        } catch (SQLException sexc) {
            logger.severe("Transfer SensorDataNormal Failed  after processing  " + count
                    + " records, inserts = " + numInserts
                    + " Updates=" + numUpdates
                    + "   Exception:" + sexc.getLocalizedMessage());
            System.out.println("Transfer SensorDataNormal Failed  after processing  " + count
                    + " records, inserts = " + numInserts
                    + " Updates=" + numUpdates
                    + "   Exception:" + sexc.getLocalizedMessage());
            sexc.printStackTrace();
        }
        System.out.println("Started At" + df.format(startAt));
        LocalDateTime EndAt = LocalDateTime.now();
        System.out.println("Ended At" + df.format(EndAt));
        long m = startAt.until(EndAt, ChronoUnit.MINUTES);
        System.out.println("Hours:" + m / 60 + "  Minutes:" + m % 60);
        
        System.out.println("Porcessed for sensors " + firstSensor + " to " + lastSensor
        + "  Times " + df.format(startTime) + " to " + df.format(endTime));
    }

}
