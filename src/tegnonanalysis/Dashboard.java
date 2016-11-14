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
import java.sql.Timestamp;
import java.time.LocalDateTime;
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
public class Dashboard {

    static final Logger logger = TegnonTransfer.tegnonLogger; //.getLogger("tegnonanalysis.Country");
   
    static final String dataFields = " Name,URL,created_at,updated_at,status";
            
    static final String fields = "DashboardID," + dataFields;
   
    
    static final String loadSQL = "select *"// + fields
            + "from Dashboard "
            + "order by DashboardID";

    static PreparedStatement loadStatement = null;
// NB DateTimeStamp is a reserved word in SQL 92  MS SQL should NEVER allow it tio be used as a column name
    static final String insertSql = "insert into Dashboard("
            + fields
            + ") values(?,?,?,?,?,?)";
    static PreparedStatement insertStatement = null;

    static final String updateSql = "update Dashboard set"
            + " Name= ?, URL=?, created_at=?, updated_at=?,status=?"
            + " where DashboardID = ?";
    static PreparedStatement updateStatement = null;

    static int numInserts = 0;
    static int numUpdates = 0;
    static int numErrors = 0;

    //static LocalDateTime firstTime = null;
    //static LocalDateTime lastTime = null;
    // set of all sensors touched by the file
    //  static Set<Sensor> sensorsTouched = new HashSet<>();
    Integer id;
    String name;
    String url;
    LocalDateTime createdAt;
    LocalDateTime updatedAt;
    int status;
            
            
    
    
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
        name = rs.getString(i++);
        url = rs.getString(i++);
        createdAt = rs.getTimestamp(i++).toLocalDateTime();
        updatedAt = rs.getTimestamp(i++).toLocalDateTime();
        status = rs.getInt(i++);
           
    }

    int update() throws SQLException {
        int i = 1;
       
        updateStatement.setString(i++, name);
        updateStatement.setString(i++, url);
        updateStatement.setTimestamp(i++, Timestamp.valueOf(createdAt));
        updateStatement.setTimestamp(i++, Timestamp.valueOf(updatedAt));
        updateStatement.setInt(i++, status);
        
        updateStatement.setInt(i++, id);

        i = updateStatement.executeUpdate();
        return i;
    }

    void insert() throws SQLException {
        int i = 1;
        insertStatement.setInt(i++, id);
        insertStatement.setString(i++, name);
        insertStatement.setString(i++, url);
        insertStatement.setTimestamp(i++, Timestamp.valueOf(createdAt));
        insertStatement.setTimestamp(i++, Timestamp.valueOf(updatedAt));
        insertStatement.setInt(i++, status);
      
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
            logger.info("Transfer Dashboard complete after processing  " + count 
                    + " records, inserts = " + numInserts + " Updates="+numUpdates);
            System.out.println("Transfer Dashboard  complete after processing  " 
                    + count + " records, inserts = " + numInserts  
                    + " Updates="+numUpdates);
        } catch (SQLException sexc) {
            logger.severe("Transfer Dashboard  Failed  after processing  " + count 
                    + " records, inserts = " + numInserts 
                    + " Updates="+numUpdates 
                    + "   Exception:" + sexc.getLocalizedMessage());
            System.out.println("Transfer Dashboard Failed  after processing  " + count 
                    + " records, inserts = " + numInserts 
                    + " Updates="+numUpdates 
                    + "   Exception:" + sexc.getLocalizedMessage());
            sexc.printStackTrace();
        }

    }

}
