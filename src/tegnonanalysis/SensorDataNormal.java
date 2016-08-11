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
import java.util.logging.Level;
import java.util.logging.Logger;
//import static tegnonload.PiLine.df;

/**
 * Singleton pattern since we always deal with a single instance at a time
 *
 * Later versions may optimise by reading an array for an attachment or device
 *
 * @author chris.rowse
 */
public class SensorDataNormal {

    static final Logger logger = TegnonTransfer.tegnonLogger.getLogger("tegnonanalysis.SensorDataNormal");

    static final String loadSQL = "select id, AttachmentID, SensorID, DeviceID,DateTimeStamp,SensorType,"
            + "SensorValue, SensorCalculatedType,SensorCalculatedValue "
            + "from SensorDataNormal "
            + "where dateTimeStamp between '2015-11-01 00:00:00' and '2016-01-01 00:00:00' "
            + "order by SensorID, DateTimeStamp";

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

    static public SensorDataNormal instance = new SensorDataNormal();

    static {
        logger.setUseParentHandlers(false);
        logger.addHandler(TegnonTransfer.logHandler);
    }

    static void init(Connection conn) throws SQLException {
        instance = new SensorDataNormal();

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

    int update() throws SQLException {
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

        i = updateStatement.executeUpdate();
        return i;
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
        insertStatement.executeUpdate();
    }
    
    void save() throws SQLException {
                if (update() == 0) {
                    insert();
                    numInserts++;
                } else {
                    numUpdates++;
                }
        
    }

    void transfer(Connection in, Connection out) {

        int count = 0;
       
        try {
            loadStatement = in.prepareStatement(loadSQL);
            insertStatement = out.prepareStatement(insertSql);
            updateStatement = out.prepareStatement(updateSql);

            ResultSet rs = loadStatement.executeQuery();

            while (rs.next()) {
                bind(rs);
                save();
                 
                count++;
                if (count % 100000 == 0) {
                    System.out.println("Processed " + count);
                }
            }
            //int deleted =  deleteStatement.executeUpdate();
            logger.info("Transfer SensorDataNormal complete after processing  " + count 
                    + " records, inserts = " + numInserts + " Updates="+numUpdates);
            System.out.println("Transfer SensorDataNormal complete after processing  " 
                    + count + " records, inserts = " + numInserts + numInserts 
                    + " Updates="+numUpdates);
        } catch (SQLException sexc) {
            logger.severe("Transfer SensorDataNormal Failed  after processing  " + count 
                    + " records, inserts = " + numInserts 
                    + " Updates="+numUpdates 
                    + "   Exception:" + sexc.getLocalizedMessage());
            System.out.println("Transfer SensorDataNormal Failed  after processing  " + count 
                    + " records, inserts = " + numInserts 
                    + " Updates="+numUpdates 
                    + "   Exception:" + sexc.getLocalizedMessage());
            sexc.printStackTrace();
        }

    }

}
