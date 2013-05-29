/*
 * This class opens a server socket and listens on the configured port for connections
 */

package kiji.reader;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Iterator;
import kiji.data.GenericData;
import kiji.data.MetaData;
import kiji.util.ParameterCheck;
import org.apache.commons.configuration.HierarchicalConfiguration.Node;
import org.apache.log4j.Logger;
import org.json.JSONObject;

/**
 *
 * @author c.witschel@gmail.com
 */
public class SocketReader implements Reader{

    private static final String PORT = "port";
    private static final String TIMEOUT = "timeout";
    private static Logger logger = Logger.getLogger(SocketReader.class);
    private Node rootNode;
    private ServerSocket server;
    private Socket activeSocket;

    public void setConfig(Node n) throws Exception {
        rootNode = n;
        server = new ServerSocket(Integer.parseInt(ParameterCheck.getParameter(rootNode, PORT)));
        server.setSoTimeout(Integer.parseInt(ParameterCheck.getParameter(rootNode, TIMEOUT)));
    }
    
    /*
     * takes the next socket connection from the listener and returns the data received
     * this may block for a while depending on the sending system. use with caution.
     * (non-Javadoc)
     * @see kiji.reader.Reader#getNext()
     */
    public GenericData getNext() throws Exception {
        activeSocket = null;
        try{
            activeSocket = server.accept();
        }
        catch(SocketTimeoutException e){
            logger.info("Socket timeout");
            return null;
        }
        
        MetaData metadata = new MetaData();
        String data = null;
        BufferedReader br = new BufferedReader(new InputStreamReader(activeSocket.getInputStream()));
        String line = null;
        StringBuilder sb = new StringBuilder();
        while((line = br.readLine())!=null){
            sb.append(line);
        }
        JSONObject json = new JSONObject(sb.toString());

        Iterator iter = json.keys();
        while(iter.hasNext()){
            String key = (String)iter.next();

            if (key.equalsIgnoreCase("data"))
                data = json.getString(key);
            else
                metadata.put(key, json.getString(key));
        }

        if (data == null)
            return  new GenericData(metadata, null);
        else
            return  new GenericData(metadata, new ByteArrayInputStream(data.getBytes()));
    }

    public void commit() throws Exception {
        activeSocket.close();
    }

    public void rollback() throws Exception {
        activeSocket.close();
    }
    
    /*
     * if not already closed the active Socket will be closed
     * (non-Javadoc)
     * @see kiji.reader.Reader#shutdown()
     */
    public void shutdown(){
        try {
        	server.close();
        	activeSocket.close();
        } catch (Throwable ex) {
            logger.error("Exception closing socket connection",ex);
        }
    }

}
