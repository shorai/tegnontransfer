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
public class Sensor {

    static final Logger logger = TegnonTransfer.tegnonLogger.getLogger("tegnonanalysis.Sensor");
   
    static final String dataFields = " DeviceID, SensorTypeTID, SensorUnitTID, MeasurementTypeTID, LineId, NetworkId, SensorNumber";
            
    static final String fields = "sensorID," + dataFields;
   
    
    static final String loadSQL = "select " + fields
            + "from Sensors "
            + "order by SensorID";

    static PreparedStatement loadStatement = null;
// NB DateTimeStamp is a reserved word in SQL 92  MS SQL should NEVER allow it tio be used as a column name
    static final String insertSql = "insert into Sensors("
            + fields
            + ") values(?,?,?,?,?,?,?,?)";
    static PreparedStatement insertStatement = null;

    static final String updateSql = "update Sensors set"
            + " DeviceId = ?, SensorTypeTID = ?, SensorUnitTID = ?, MeasurementTypeTID = ?, "
            + " LineId=?, NetworkId=?, SensorNumber=?"
            + " where SensorId = ?";
    static PreparedStatement updateStatement = null;

    static int numInserts = 0;
    static int numUpdates = 0;
    static int numErrors = 0;

    //static LocalDateTime firstTime = null;
    //static LocalDateTime lastTime = null;
    // set of all sensors touched by the file
    //  static Set<Sensor> sensorsTouched = new HashSet<>();
    Integer id;
    int deviceId;
    int sensorTypeTID;
    int sensorUnitTID;
    int measurementTypeTID;
    int lineId;
    int networkId;
    int sensorNumber;
 
    static {
        logger.setUseParentHandlers(false);
        logger.addHandler(TegnonTransfer.logHandler);
    }

    static void init(Connection conn) throws SQLException {
         logger.setLevel(Level.WARNING);
    }

    void bind(ResultSet rs) throws SQLException {
        //id, AttachmentID, SensorID, DeviceID,DateTimeStamp,SensorType,"
        //    + "SensorValue, SensorCalculatedType,SensorCalculatedValue
        int i = 1;
        id = rs.getInt(i++);
        deviceId= rs.getInt(i++);
        sensorTypeTID= rs.getInt(i++);
        sensorUnitTID= rs.getInt(i++);
        measurementTypeTID= rs.getInt(i++);
        lineId= rs.getInt(i++);
        networkId= rs.getInt(i++);
        sensorNumber= rs.getInt(i++);
    }

    int update() throws SQLException {
        int i = 1;
       
        updateStatement.setInt(i++, deviceId);
       updateStatement.setInt(i++,sensorTypeTID);
       updateStatement.setInt(i++,sensorUnitTID);
       updateStatement.setInt(i++,measurementTypeTID);
       updateStatement.setInt(i++,lineId);
       updateStatement.setInt(i++,networkId);
       updateStatement.setInt(i++,sensorNumber);
        
        updateStatement.setInt(i++, id);

        i = updateStatement.executeUpdate();
        return i;
    }

    void insert() throws SQLException {
        int i = 1;
        insertStatement.setInt(i++, id);
        insertStatement.setInt(i++, deviceId);
        insertStatement.setInt(i++,sensorTypeTID);
        insertStatement.setInt(i++,sensorUnitTID);
        insertStatement.setInt(i++,measurementTypeTID);
        insertStatement.setInt(i++,lineId);
        insertStatement.setInt(i++,networkId);
        insertStatement.setInt(i++,sensorNumber);
           
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
            logger.info("Transfer Sensors complete after processing  " + count 
                    + " records, inserts = " + numInserts + " Updates="+numUpdates);
            System.out.println("Transfer Sensors  complete after processing  " 
                    + count + " records, inserts = " + numInserts + numInserts 
                    + " Updates="+numUpdates);
        } catch (SQLException sexc) {
            logger.severe("Transfer Sensors  Failed  after processing  " + count 
                    + " records, inserts = " + numInserts 
                    + " Updates="+numUpdates 
                    + "   Exception:" + sexc.getLocalizedMessage());
            System.out.println("Transfer Sensors  Failed  after processing  " + count 
                    + " records, inserts = " + numInserts 
                    + " Updates="+numUpdates 
                    + "   Exception:" + sexc.getLocalizedMessage());
            sexc.printStackTrace();
        }

    }

}
