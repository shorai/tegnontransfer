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
public class Role {

    static final Logger logger = TegnonTransfer.tegnonLogger; //.getLogger("tegnonanalysis.Users");
   
    static final String dataFields = " name,"
            + "created_at,updated_at";
            
    static final String fields = "id," + dataFields;
   
    
    static final String loadSQL = "select *"// + fields
            + "from roles "
            + "order by id";

    static PreparedStatement loadStatement = null;
// NB DateTimeStamp is a reserved word in SQL 92  MS SQL should NEVER allow it tio be used as a column name
    static final String insertSql = "insert into roles("
            + fields
            + ") values(?,?,?,?)";
    static PreparedStatement insertStatement = null;

    static final String updateSql = "update roles set"
            + " name= ?, "
            + " created_at=?, updated_at=?"
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
    String name;
    LocalDateTime created_at,updated_at;
    
    
    static {
        logger.setUseParentHandlers(false);
        logger.addHandler(TegnonTransfer.logHandler);
    }

    static void init(Connection conn) throws SQLException {
         logger.setLevel(Level.WARNING);
    }

    void bind(ResultSet rs) throws SQLException {
        //userName,password,email,remember_token,"
        //    + "created_at,updated_at,status,"
        //    + "clientID,location_id,SiteID,active_client,active_site"
        //    + ""
        int i = 1;
    
        id = rs.getInt(i++);
        name = rs.getString(i++);
          try {
            Timestamp ct = rs.getTimestamp(i++);
            if (ct != null) {
                created_at = ct.toLocalDateTime();
            } else {
                created_at =  LocalDateTime.now();
            }
        } catch (SQLException sexc) {
            created_at = LocalDateTime.now();
        }
        try {
            Timestamp ut = rs.getTimestamp(i++);
            if (ut != null) {
                updated_at = ut.toLocalDateTime();
            } else {
                updated_at = LocalDateTime.now();
            }
        } catch (SQLException sexc) {
            updated_at = LocalDateTime.now();
        }
        
    }

    int update() throws SQLException {
        int i = 1;
       
        updateStatement.setString(i++, name);
        updateStatement.setTimestamp(i++, Timestamp.valueOf(created_at));
        updateStatement.setTimestamp(i++, Timestamp.valueOf(updated_at));
        
        
        updateStatement.setInt(i++, id);

        i = updateStatement.executeUpdate();
        return i;
    }

    /**
     * 
     " UserID, UserName,password,email,remember_token,"
            + "created_at,updated_at,status,"
            + "clientID,location_id,SiteID,active_client,active_site";
     * @throws SQLException 
     */
    void insert() throws SQLException {
        int i = 1;
        insertStatement.setInt(i++, id);
        insertStatement.setString(i++, name);
        insertStatement.setTimestamp(i++, Timestamp.valueOf(created_at));
        insertStatement.setTimestamp(i++, Timestamp.valueOf(updated_at));
        
           
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
            logger.info("Transfer Roles complete after processing  " + count 
                    + " records, inserts = " + numInserts + " Updates="+numUpdates);
            System.out.println("Transfer Roles  complete after processing  " 
                    + count + " records, inserts = " + numInserts  
                    + " Updates="+numUpdates);
        } catch (SQLException sexc) {
            logger.severe("Transfer Roles  Failed  after processing  " + count 
                    + " records, inserts = " + numInserts 
                    + " Updates="+numUpdates 
                    + "   Exception:" + sexc.getLocalizedMessage());
            System.out.println("Transfer Roles  Failed  after processing  " + count 
                    + " records, inserts = " + numInserts 
                    + " Updates="+numUpdates 
                    + "   Exception:" + sexc.getLocalizedMessage());
            sexc.printStackTrace();
        }

    }

}
