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
import java.util.HashSet;
import java.util.Set;
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

    static final Logger logger = TegnonAnalysis.tegnonLogger.getLogger("tegnonanalysis.SensorDataNormal");

    static final String loadSQL = "select id, AttachmentID, SensorID, DeviceID,DateTimeStamp,SensorType,"
            + "SensorValue, SensorCalculatedType,SensorCalculatedValue) "
            + "from SensorDataNormal "
            + "where dateTimeStamp between '2016-01-01' and '2016-02-01' "
            + "order by SensorID, DateTimeStamp";

    static PreparedStatement loadStatement = null;

    static final String insertSql = "insert into SensorDataNormal(AttachmentID, "
            + "SensorId, DeviceID,DateTimeStamp,SensorType,"
            + "SensorValue, SensorCalculatedType,SensorCalculatedValue)"
            + "values(?,?,?,?,?,?,?,?)";
    static PreparedStatement insertStatement = null;

    static final String updateSql = "update SensorDataNormal set"
            + " AttachmentId = ?, SensorId = ?, DeviceID = ?, DateTimeStamp = ?, SensorType = ?, "
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
        logger.addHandler(TegnonAnalysis.logHandler);
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

    }

    void transfer(Connection in, Connection out) {

        int count = 0;
        int inserts = 0;
        try {
            loadStatement = in.prepareStatement(loadSQL);
            insertStatement = out.prepareStatement(insertSql);
            updateStatement = out.prepareStatement(updateSql);

            ResultSet rs = loadStatement.executeQuery();

            while (rs.next()) {
                bind(rs);

                if (update() == 0) {
                    insert();
                    inserts++;
                }
                count++;

            }
            //int deleted =  deleteStatement.executeUpdate();
            logger.info("Transfer complete after processing  " + count + " records, inserts = " + inserts);
        } catch (SQLException sexc) {
            logger.severe("Transfer Failed  after processing  " + count + " records, inserts = " + inserts + "   Exception:" + sexc.getLocalizedMessage());
        }

    }

}
