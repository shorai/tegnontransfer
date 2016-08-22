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
public class Device {

    static final Logger logger = TegnonTransfer.tegnonLogger.getLogger("tegnonanalysis.Device");
   
    static final String dataFields = "FacilityInfo,DeviceCommonName,NetworkId, SensorFacilityInfo"
            + ",ModbusAddr, DeviceSerialNumber, DeviceType, FirmwareVersion, Reporting, LocationId, NumberOfAttachedSensors";
            
    static final String fields = "DeviceID," + dataFields;
       
    static final String loadSQL = "select *"// + fields
            + "from Device "
            + "order by DeviceID";

    static PreparedStatement loadStatement = null;
// NB DateTimeStamp is a reserved word in SQL 92  MS SQL should NEVER allow it tio be used as a column name
    static final String insertSql = "insert into Device("
            + fields
            + ") values(?,?,?,?,?,?,?,?,?,?,?,?)";
    static PreparedStatement insertStatement = null;

    static final String updateSql = "update Device set"
            + " FacilityInfo=?,DeviceCommonName=?,NetworkId=?, SensorFacilityInfo=?"
            + ",ModbusAddr=?, DeviceSerialNumber=?, DeviceType=?, FirmwareVersion=?"
            +", Reporting=?, LocationId=?, NumberOfAttachedSensors=?"
            + " where DeviceID = ?";
    static PreparedStatement updateStatement = null;

    static int numInserts = 0;
    static int numUpdates = 0;
    static int numErrors = 0;

    //static LocalDateTime firstTime = null;
    //static LocalDateTime lastTime = null;
    // set of all sensors touched by the file
    //  static Set<Sensor> sensorsTouched = new HashSet<>();
    Integer id;
    
    String facilityInfo,deviceCommonName;
    int networkId;
    String  sensorFacilityInfo;
    int modbusAddr, deviceSerialNumber;
    String deviceType,firmwareVersion;
    boolean reporting;
    int locationId, numberOfAttachedSensors;
    
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
        facilityInfo = rs.getString(i++);
        deviceCommonName = rs.getString(i++);
        
        networkId= rs.getInt(i++);
        sensorFacilityInfo = rs.getString(i++);
        modbusAddr = rs.getInt(i++);
        deviceSerialNumber = rs.getInt(i++);
        deviceType = rs.getString(i++);
        firmwareVersion = rs.getString(i++);
        reporting = rs.getBoolean(i++);
        locationId = rs.getInt(i++);
        numberOfAttachedSensors = rs.getInt(i++);
    }

    int update() throws SQLException {
        int i = 1;
       updateStatement.setString(i++, facilityInfo );
       updateStatement.setString(i++, deviceCommonName );
       updateStatement.setInt(i++, networkId);
       updateStatement.setString(i++, sensorFacilityInfo );
       updateStatement.setInt(i++,modbusAddr );
       updateStatement.setInt(i++,deviceSerialNumber );
       updateStatement.setString(i++,deviceType );
       updateStatement.setString(i++, firmwareVersion);
       updateStatement.setBoolean(i++, reporting);
       updateStatement.setInt(i++, locationId);
       updateStatement.setInt(i++, numberOfAttachedSensors);
        
        updateStatement.setInt(i++, id);

        i = updateStatement.executeUpdate();
        return i;
    }

    void insert() throws SQLException {
        int i = 1;
        insertStatement.setInt(i++, id);
       insertStatement.setString(i++, facilityInfo );
       insertStatement.setString(i++, deviceCommonName );
       insertStatement.setInt(i++, networkId);
       insertStatement.setString(i++, sensorFacilityInfo );
       insertStatement.setInt(i++,modbusAddr );
       insertStatement.setInt(i++,deviceSerialNumber );
       insertStatement.setString(i++,deviceType );
       insertStatement.setString(i++, firmwareVersion);
       insertStatement.setBoolean(i++, reporting);
       insertStatement.setInt(i++, locationId);
       insertStatement.setInt(i++, numberOfAttachedSensors);
           
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
            logger.info("Transfer Device complete after processing  " + count 
                    + " records, inserts = " + numInserts + " Updates="+numUpdates);
            System.out.println("Transfer Device  complete after processing  " 
                    + count + " records, inserts = " + numInserts  
                    + " Updates="+numUpdates);
        } catch (SQLException sexc) {
            logger.severe("Transfer Device  Failed  after processing  " + count 
                    + " records, inserts = " + numInserts 
                    + " Updates="+numUpdates 
                    + "   Exception:" + sexc.getLocalizedMessage());
            System.out.println("Transfer Device  Failed  after processing  " + count 
                    + " records, inserts = " + numInserts 
                    + " Updates="+numUpdates 
                    + "   Exception:" + sexc.getLocalizedMessage());
            sexc.printStackTrace();
        }

    }

}
