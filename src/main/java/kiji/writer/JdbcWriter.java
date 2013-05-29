/*
 * This class is able to write data to a database.
 */

package kiji.writer;

import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import kiji.data.GenericData;
import kiji.util.ParameterCheck;
import org.apache.commons.configuration.HierarchicalConfiguration.Node;
import org.apache.log4j.Logger;

/**
 *
 * @author Christian.Witschel
 */
public class JdbcWriter implements Writer{

    private static final String JDBC_DRIVER = "jdbcdriver";
    private static final String JDBC_URL = "jdbcurl";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String TRANSACTIONAL = "transactional";
    private static final String INTERFACE_TABLE = "interfacetable";
    private static final String INSERT_STATUS = "insertstatus";
    private static final String BUSINESS_OBJECT = "businessobject";
    private Node rootNode;
    private Connection connection;
    Logger logger = Logger.getLogger(JdbcWriter.class);

    public void setConfig(Node n) throws Exception {
        rootNode = n;

        Class.forName(ParameterCheck.getParameter(n, JDBC_DRIVER));
        connection = DriverManager.getConnection(ParameterCheck.getParameter(n, JDBC_URL),
                ParameterCheck.getParameter(n, USER),
                ParameterCheck.getParameter(n, PASSWORD));
        if (ParameterCheck.getParameter(n, TRANSACTIONAL).equalsIgnoreCase("true"))
            connection.setAutoCommit(false);
        else
            connection.setAutoCommit(true);

        //check and create interface table
        String sql = "select * from "+ ParameterCheck.getParameter(n, INTERFACE_TABLE);
        Statement st = connection.createStatement();
        try{
            st.executeQuery(sql);
        }
        catch(SQLException e){
            logger.warn("interface table does not exist. trying to create it", e);
            sql = "CREATE TABLE " + ParameterCheck.getParameter(n, INTERFACE_TABLE) + "(ID INT AUTO_INCREMENT PRIMARY KEY,  DATA CLOB, BUSINESS_OBJECT VARCHAR(100), META_DATA CLOB, STATUS INT, CREATE_DATE TIMESTAMP)";
            logger.info(sql);
            st = connection.createStatement();
            if (st.execute(sql))
                logger.info("Interface table created");
            else
                throw new Exception("Unable to create Interface Table");
        }
        connection.commit();
    }

    public void write(GenericData d) throws Exception {



        String sql = "INSERT INTO " + ParameterCheck.getParameter(rootNode, INTERFACE_TABLE) + "(DATA, META_DATA, BUSINESS_OBJECT, STATUS, CREATE_DATE) VALUES(?, ?, ?, ?, ?)";
        PreparedStatement pst = connection.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);

        Reader r = new InputStreamReader(d.getOutputAsStream());
        try{
            pst.setClob(1, r);
        }
        catch(Exception e){
            logger.debug("Exception setting CLOB",e);
            pst.setString(1, new String(d.getOutputAsByteArray()));
        }
        pst.setString(2, d.getMetadata().toString());

        if(d.getMetadata().get("BUSINESS_OBJECT") != null)
            pst.setString(3, (String)d.getMetadata().get("BUSINESS_OBJECT"));
        else
            pst.setString(3, ParameterCheck.getParameter(rootNode, BUSINESS_OBJECT));
        pst.setInt(4, Integer.parseInt(ParameterCheck.getParameter(rootNode, INSERT_STATUS)));
        pst.setDate(5, new java.sql.Date(new Date().getTime()));

        pst.execute();
        ResultSet keys = pst.getGeneratedKeys();
        keys.next();
        int key = keys.getInt(1);
        keys.close();

        logger.info("create object on database with key " + key);

        pst.close();
    }

    public void commit() throws Exception {
        connection.commit();
        logger.info("commited database transaction");
        //for testing
        //connection.rollback();
    }

    public void rollback() throws Exception {
        connection.rollback();
        logger.info("rolled back database transaction");
    }

}
