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
public class MeasurementType {

    static final Logger logger = TegnonTransfer.tegnonLogger.getLogger("tegnonanalysis.MeasurementType");
   
    static final String dataFields = " MeasurementTypeTID, MeasurementTypeName, Description";
            
    static final String fields = "MeasurementTypeID," + dataFields;
   
    
    static final String loadSQL = "select " + fields
            + "from MeasurementTypes "
            + "order by MeasurementTypeID";

    static PreparedStatement loadStatement = null;
// NB DateTimeStamp is a reserved word in SQL 92  MS SQL should NEVER allow it tio be used as a column name
    static final String insertSql = "insert into MeasurementType("
            + fields
            + ") values(?,?,?,?)";
    static PreparedStatement insertStatement = null;

    static final String updateSql = "update MeasurementType set"
            + " MeasurementTypeTID = ?,MeasurementTypeName = ?, Description = ?"
            + " where MeasurementTypeID = ?";
    static PreparedStatement updateStatement = null;

    static int numInserts = 0;
    static int numUpdates = 0;
    static int numErrors = 0;

    //static LocalDateTime firstTime = null;
    //static LocalDateTime lastTime = null;
    // set of all sensors touched by the file
    //  static Set<Sensor> sensorsTouched = new HashSet<>();
    Integer id;
    Integer measurementTypeTID;
    String measurementTypeName;
    String description;
    
    static {
        logger.setUseParentHandlers(false);
        logger.addHandler(TegnonTransfer.logHandler);
    }

    static void init(Connection conn) throws SQLException {
         logger.setLevel(Level.WARNING);
    }

   public String toString() {
        return "MeasurementType["+ id + "] " + measurementTypeTID + " " + measurementTypeName + " Descr:" + description;
    }
    void bind(ResultSet rs) throws SQLException {
        //id, AttachmentID, SensorID, DeviceID,DateTimeStamp,SensorType,"
        //    + "SensorValue, SensorCalculatedType,SensorCalculatedValue
        int i = 1;
    
        id = rs.getInt(i++);
        measurementTypeTID = rs.getInt(i++);
        measurementTypeName = rs.getString(i++);
        description = rs.getString(i++);
    }

    int update() throws SQLException {
        int i = 1;
       updateStatement.setInt(i++,measurementTypeTID);
       
        updateStatement.setString(i++, measurementTypeName);
        updateStatement.setString(i++,description);
        
        updateStatement.setInt(i++, id);

        i = updateStatement.executeUpdate();
        return i;
    }

    void insert() throws SQLException {
        int i = 1;
        insertStatement.setInt(i++, id);
        insertStatement.setInt(i++,measurementTypeTID);
        insertStatement.setString(i++, measurementTypeName);
        insertStatement.setString(i++,description);
           
        insertStatement.executeUpdate();
    }
    
    void save() throws SQLException {
        //System.out.println(this.toString());
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
            //loadStatement = in.prepareStatement(loadSQL);
            loadStatement = in.prepareStatement("select * from measurementType");
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
            logger.info("Transfer MeasurementType complete after processing  " + count 
                    + " records, inserts = " + numInserts + " Updates="+numUpdates);
            System.out.println("Transfer MeasurementTypes  complete after processing  " 
                    + count + " records, inserts = " + numInserts 
                    + " Updates="+numUpdates);
        } catch (SQLException sexc) {
            logger.severe("Transfer MeasurementType  Failed  after processing  " + count 
                    + " records, inserts = " + numInserts 
                    + " Updates="+numUpdates 
                    + "   Exception:" + sexc.getLocalizedMessage());
            System.out.println("Transfer MeasurementType  Failed  after processing  " + count 
                    + " records, inserts = " + numInserts 
                    + " Updates="+numUpdates 
                    + "   Exception:" + sexc.getLocalizedMessage());
            sexc.printStackTrace();
        }

    }

}
