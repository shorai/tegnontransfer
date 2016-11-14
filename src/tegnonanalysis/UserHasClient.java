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
public class UserHasClient {

    static final Logger logger = TegnonTransfer.tegnonLogger; //.getLogger("tegnonanalysis.Users");
   
    static final String dataFields = " Clients_ClientID";
            
    static final String fields = "TegnonUsers_id" + dataFields;
   
    
    static final String loadSQL = "select *"// + fields
            + "from users_has_client "
            + "order by TegnonUsers_id,Clients_clientID";

    static PreparedStatement loadStatement = null;
// NB DateTimeStamp is a reserved word in SQL 92  MS SQL should NEVER allow it tio be used as a column name
    static final String insertSql = "insert into users_has_client("
            + fields
            + ") values(?,?)";
    static PreparedStatement insertStatement = null;
/*  This is a pure relationship
    static final String updateSql = "update ssers_has_sites set"
            + " UserName= ?"
            + "password=?, email=?, remember_token=?, "
            + "created_at=?, updated_at=?, status=?,"
            + "clientID=?, location_id = ?, SiteID=?, active_client=?,active_site=?"
            + " where UserID = ?";
  
    static PreparedStatement updateStatement = null;
*/
    static int numInserts = 0;
    static int numUpdates = 0;
    static int numErrors = 0;

    //static LocalDateTime firstTime = null;
    //static LocalDateTime lastTime = null;
    // set of all sensors touched by the file
    //  static Set<Sensor> sensorsTouched = new HashSet<>();
    //Integer id;
    
    int clientId, userId;
    
    
    static {
        logger.setUseParentHandlers(false);
        logger.addHandler(TegnonTransfer.logHandler);
    }

    static void init(Connection conn) throws SQLException {
         logger.setLevel(Level.WARNING);
    }

    void bind(ResultSet rs) throws SQLException {
        int i = 1;
    
      //  id = rs.getInt(i++);
        userId = rs.getInt(i++);
        clientId= rs.getInt(i++);
       
    }

    int update() throws SQLException {
        int i = 1;
       /*
        updateStatement.setString(i++, userName);
        updateStatement.setString(i++, password);
        updateStatement.setString(i++, email);
        updateStatement.setString(i++, remember_token);
        updateStatement.setTimestamp(i++, Timestamp.valueOf(created_at));
        updateStatement.setTimestamp(i++, Timestamp.valueOf(updated_at));
        updateStatement.setBoolean(i++, status);
        updateStatement.setInt(i++, clientId);
        updateStatement.setInt(i++, location_id);
        updateStatement.setBoolean(i++, active_client);
        updateStatement.setBoolean(i++, active_site);
        
        
        updateStatement.setInt(i++, id);

        i = updateStatement.executeUpdate();
        */
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
        insertStatement.setInt(i++, userId);
        insertStatement.setInt(i++, clientId);
           
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
       //     updateStatement = out.prepareStatement(updateSql);

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
            logger.info("Transfer User_has_sites complete after processing  " + count 
                    + " records, inserts = " + numInserts + " Updates="+numUpdates);
            System.out.println("Transfer User_has_sites  complete after processing  " 
                    + count + " records, inserts = " + numInserts  
                    + " Updates="+numUpdates);
        } catch (SQLException sexc) {
            logger.severe("Transfer User_has_client  Failed  after processing  " + count 
                    + " records, inserts = " + numInserts 
                    + " Updates="+numUpdates 
                    + "   Exception:" + sexc.getLocalizedMessage());
            System.out.println("Transfer User_has_client  Failed  after processing  " + count 
                    + " records, inserts = " + numInserts 
                    + " Updates="+numUpdates 
                    + "   Exception:" + sexc.getLocalizedMessage());
            sexc.printStackTrace();
        }

    }

}
