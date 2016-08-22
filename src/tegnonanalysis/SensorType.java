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
public class SensorType {

    static final Logger logger = TegnonTransfer.tegnonLogger.getLogger("tegnonanalysis.SensorType");
   
    static final String dataFields = " sensorTypeTID, Description, DefaultUnitOfMeassure"
            + " ,DisplayOrder, Analysis, SourceTypeTID, ConversionFactor";
            
    static final String fields = "sensorTypeID," + dataFields;
   
    
    static final String loadSQL = "select *"// + fields
            + "from SensorType ";
            //+ "order by SensorTypeID";
static final String loadSQL1 = "select * from SensorType";
    static PreparedStatement loadStatement = null;
// NB DateTimeStamp is a reserved word in SQL 92  MS SQL should NEVER allow it tio be used as a column name
    static final String insertSql = "insert into SensorType("
            + fields
            + ") values(?,?,?,?,?,?,?,?)";
    static PreparedStatement insertStatement = null;

    static final String updateSql = "update SensorType set"
            + " SensorTypeTID = ?, Description = ?"
            + " ,DefaultUnitOfMeassure = ?, DisplayOrder = ?"
            + " ,Analysis = ?, SourceTypeTID = ?, ConversionFactor=?"
            + " where SensorTypeID = ?";
    static PreparedStatement updateStatement = null;

    static int numInserts = 0;
    static int numUpdates = 0;
    static int numErrors = 0;

    //static LocalDateTime firstTime = null;
    //static LocalDateTime lastTime = null;
    // set of all sensors touched by the file
    //  static Set<Sensor> sensorsTouched = new HashSet<>();
    Integer id;
    Integer sensorTypeTID;
    String description;
    String defaultUnitOfMeasure;
    int displayOrder;
    boolean analysis;
    int sourceTypeTID;
    double conversionFactor;
    
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
        sensorTypeTID = rs.getInt(i++);
        description = rs.getString(i++);
        defaultUnitOfMeasure = rs.getString(i++);
        displayOrder = rs.getInt(i++);
        analysis = rs.getBoolean(i++);
        sourceTypeTID = rs.getInt(i++);
        conversionFactor = rs.getDouble(i++);
    }

    int update() throws SQLException {
        int i = 1;
       updateStatement.setInt(i++,sensorTypeTID);
       
        updateStatement.setString(i++,description);
        updateStatement.setString(i++,defaultUnitOfMeasure);
        updateStatement.setInt(i++,displayOrder);
        updateStatement.setBoolean(i++,analysis);
        updateStatement.setInt(i++,sourceTypeTID);
        updateStatement.setDouble(i++,conversionFactor);
        
        updateStatement.setInt(i++, id);

        i = updateStatement.executeUpdate();
        return i;
    }

    void insert() throws SQLException {
        int i = 1;
        insertStatement.setInt(i++, id);
        insertStatement.setInt(i++,sensorTypeTID);
        insertStatement.setString(i++,description);
        insertStatement.setString(i++,defaultUnitOfMeasure);
        insertStatement.setInt(i++,displayOrder);
        insertStatement.setBoolean(i++,analysis);
        insertStatement.setInt(i++,sourceTypeTID);
        insertStatement.setDouble(i++,conversionFactor);
           
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
            logger.info("Transfer SensorType complete after processing  " + count 
                    + " records, inserts = " + numInserts + " Updates="+numUpdates);
            System.out.println("Transfer SensorTypes  complete after processing  " 
                    + count + " records, inserts = " + numInserts  
                    + " Updates="+numUpdates);
        } catch (SQLException sexc) {
            logger.severe("Transfer SensorType  Failed  after processing  " + count 
                    + " records, inserts = " + numInserts 
                    + " Updates="+numUpdates 
                    + "   Exception:" + sexc.getLocalizedMessage());
            System.out.println("Transfer SensorType  Failed  after processing  " + count 
                    + " records, inserts = " + numInserts 
                    + " Updates="+numUpdates 
                    + "   Exception:" + sexc.getLocalizedMessage());
            sexc.printStackTrace();
        }

    }

}
