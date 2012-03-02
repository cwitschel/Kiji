/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package kiji.reader;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import kiji.data.GenericData;
import kiji.data.MetaData;
import kiji.interfaces.FatalException;
import kiji.util.ParameterCheck;
import org.apache.commons.configuration.HierarchicalConfiguration.Node;
import org.apache.log4j.Logger;

/**
 *
 * @author christian
 */
public class JdbcReader implements Reader{

    private static final String JDBC_DRIVER = "jdbcdriver";
    private static final String JDBC_URL = "jdbcurl";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String TRANSACTIONAL = "transactional";
    private static final String INTERFACE_TABLE = "interfacetable";
    private static final String SQL_COMMAND = "sqlcommand";
    private static final String SELECT_STATUS = "selectstatus";
    private static final String UPDATE_STATUS = "updatestatus";
    private static final String POLL_INTERVAL = "pollinterval";
    private Node rootNode;
    private Connection connection;
    private GenericData data;
    private ResultSet rst;
    Logger logger = Logger.getLogger(JdbcReader.class);
    private long connectionStart = 0;

    public void setConfig(Node n) throws Exception{
        rootNode = n;

        Class.forName(ParameterCheck.getParameter(n, JDBC_DRIVER));
        establishConnection();

        //check and create interface table
        String sql = "select * from "+ ParameterCheck.getParameter(n, INTERFACE_TABLE);
        Statement st = connection.createStatement();
        try{
            st.executeQuery(sql);
        }
        catch(SQLException e){
            logger.warn("interface table does not exist. trying to create it", e);
            sql = "create table " + ParameterCheck.getParameter(n, INTERFACE_TABLE) + "(ID INT AUTO_INCREMENT PRIMARY KEY,  DATA CLOB, BUSINESS_OBJECT VARCHAR2(100), STATUS INT, CREATE_DATE TIMESTAMP)";
            logger.info(sql);
            //CREATE TABLE TEST(ID INT PRIMARY KEY,  NAME VARCHAR(255));
            st = connection.createStatement();
            if (st.execute(sql))
                logger.info("Interface table created");
            else
                throw new Exception("Unable to create Interface Table");
        }
        connection.commit();
    }

    public GenericData getNext() throws Exception {

        if (rst == null || !rst.next()){

            String sql = "SELECT ID,  DATA, META_DATA, BUSINESS_OBJECT, CREATE_DATE FROM "
                    + ParameterCheck.getParameter(rootNode, INTERFACE_TABLE)
                    + " WHERE STATUS = " + ParameterCheck.getParameter(rootNode, SELECT_STATUS)
                    + " ORDER BY ID";
            if (ParameterCheck.getOptionalParameter(rootNode, SQL_COMMAND)!=null)
                sql = ParameterCheck.getParameter(rootNode, SQL_COMMAND);
            Statement st = connection.createStatement();

            if (!sql.toUpperCase().startsWith("SELECT")){
                int lines = st.executeUpdate(sql);
                if (lines==0){
                   logger.info("No entries found in database");
                    //no matching file found, sleep
                    Thread.sleep(Long.parseLong(ParameterCheck.getParameter(rootNode, POLL_INTERVAL)));
                    return null;
                }

                //lines where changed
                logger.info("executed sql affecting "+lines+" lines");

                data = new GenericData(new MetaData(), new ByteArrayInputStream(new byte[0]));
                return data;
            }

            rst = st.executeQuery(sql);
            if (!rst.next()){
                logger.info("No entries found in database ");
                //no matching file found, sleep
                Thread.sleep(Long.parseLong(ParameterCheck.getParameter(rootNode, POLL_INTERVAL)));
                return null;
            }
        }



        MetaData metadata = new MetaData();
        metadata.fromString(rst.getString("META_DATA"));

        metadata.put("ID", rst.getString("ID"));
        metadata.put("CREATE_DATE", rst.getDate("CREATE_DATE"));
        metadata.put("BUSINESS_OBJECT", rst.getString("BUSINESS_OBJECT"));
        try{
            data = new GenericData(metadata, rst.getClob("DATA").getAsciiStream());
        }
        catch(Exception e){
            logger.debug("Exception getting CLOB",e);
            data = new GenericData(metadata, new ByteArrayInputStream(rst.getString("DATA").getBytes()));
        }

        return data;
    }

    public GenericData getCurrent() throws Exception {
        return data;
    }

    public void commit() throws Exception {
        String id = (String)data.getMetadata().get("ID");

        if (ParameterCheck.getOptionalParameter(rootNode, SQL_COMMAND)!= null
                && !ParameterCheck.getOptionalParameter(rootNode, SQL_COMMAND).toUpperCase().startsWith("SELECT")){
            connection.commit();
            checkConnection();
            logger.info("successfully committed transaction ");
            return;
        }

        String sql = "UPDATE " + ParameterCheck.getParameter(rootNode, INTERFACE_TABLE)
                + " set STATUS = " + ParameterCheck.getParameter(rootNode, UPDATE_STATUS)
                + " WHERE ID = "+id;
        Statement st = connection.createStatement();
        int result = st.executeUpdate(sql);
        if (result == 1){
            connection.commit();
            logger.info("successfully retrieved row "+id);
        }
        else{
            if (result == 0){
                throw new Exception("no row could be update when retrieving data (key="+id+")");
            }
            if (result > 0){
                throw new Exception("too many rows where updated when retrieving data (key="+id+") rowcount = " + result);
            }
        }
        checkConnection();
    }

    public void rollback() throws Exception {
        connection.rollback();
        checkConnection();
    }

    private void establishConnection() throws Exception{

        //connection is not older then 5 min?
        if ((System.currentTimeMillis() - connectionStart) < 300000){
            //throw new FatalException("Database connectivity seems to be broken");
        }

        connection = DriverManager.getConnection(ParameterCheck.getParameter(rootNode, JDBC_URL),
                ParameterCheck.getParameter(rootNode, USER),
                ParameterCheck.getParameter(rootNode, PASSWORD));
        if (ParameterCheck.getParameter(rootNode, TRANSACTIONAL).equalsIgnoreCase("true"))
            connection.setAutoCommit(false);
        else
            connection.setAutoCommit(true);
        connectionStart = System.currentTimeMillis();
    }

    private void checkConnection() throws Exception {

        //check connection health
        if (connection == null){
            establishConnection();
        }
        else if(!isValid()){
            connection = null;
            establishConnection();
        }
        //is connection older then an hour?
        else if((System.currentTimeMillis() - connectionStart) > 3600000){
            logger.info("Connection is older then maximum age. Reconnecting.");
            try{
                connection.close();
                connection = null;
            }
            catch(Exception e){
                logger.error("Exception closing connection for reconnect", e);
            }
            establishConnection();
        }
        else{
            logger.info("connection is still ok");
        }
    }

    private boolean isValid() {
        try {
            return connection.isValid(500);
        } catch (SQLException ex) {
            logger.debug("Seems isValid is not supported by this Driver", ex);
        }

        try{
            String sql = "select count(*) from "+ ParameterCheck.getParameter(rootNode, INTERFACE_TABLE);
            Statement st = connection.createStatement();
        
            st.executeQuery(sql);
            st.close();
        }
        catch(SQLException e){
            logger.debug("Seems connection is corrupted", e);
            return false;
        }
        catch(Exception e){
            logger.debug("Something went wrong checking if connection is valid", e);
        }

        return true;
    }


}
