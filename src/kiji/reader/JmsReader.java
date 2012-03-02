/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package kiji.reader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Enumeration;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import kiji.data.GenericData;
import kiji.data.MetaData;
import kiji.util.ParameterCheck;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.configuration.HierarchicalConfiguration.Node;
import org.apache.log4j.Logger;

/**
 *
 * @author christian
 */
public class JmsReader implements Reader {

    private static final String JMS_PROVIDER = "jmsprovider";
    private static final String JMS_URL = "jmsurl";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String TRANSACTIONAL = "transactional";
    private static final String QUEUE = "queue";
    private static final String POLL_INTERVAL = "pollinterval";
    private Node rootNode;
    private Logger logger = Logger.getLogger(JmsReader.class.getName());
    private Connection connection;
    private Session session;
    private MessageConsumer consumer;
    private Message message;
    private GenericData data;
    private long pollInterval;

    public void setConfig(Node n) throws Exception {
        rootNode = n;

        ConnectionFactory connectionFactory = null;
        if (ParameterCheck.getParameter(n, JMS_PROVIDER).equals("ACTIVEMQ")) {
            // Create a ConnectionFactory
            connectionFactory = new ActiveMQConnectionFactory(ParameterCheck.getParameter(rootNode, USER),
                    ParameterCheck.getParameter(rootNode, PASSWORD),
                    ParameterCheck.getParameter(rootNode, JMS_URL));

        }
        else{
            throw new Exception("JMS Provider "+ParameterCheck.getParameter(n, JMS_PROVIDER)+" not supported yet.");
        }

        // Create a Connection
        connection = connectionFactory.createConnection();
        connection.start();

        //connection.setExceptionListener(this);

        // Create a Session
        boolean transacted = false;
        if (ParameterCheck.getParameter(rootNode, TRANSACTIONAL).equals("true")) {
            transacted = true;
        }
        if (transacted) {
            session = connection.createSession(transacted, Session.SESSION_TRANSACTED);
        } else {
            session = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);
        }

        // Create the destination (Topic or Queue)
        Destination destination = session.createQueue(ParameterCheck.getParameter(rootNode, QUEUE));

        // Create a MessageConsumer from the Session to the Topic or Queue
        consumer = session.createConsumer(destination);

        //initialize poll interval
        if (rootNode.getChildrenCount(POLL_INTERVAL)==1)
            pollInterval = Long.parseLong((String)((Node)rootNode.getChildren(POLL_INTERVAL).get(0)).getValue());
        else{
            throw new Exception("No or too many <"+POLL_INTERVAL+"> tags specified for this interface. aborting.");
        }

    }

    public GenericData getNext() throws Exception {
        // Wait for a message
        message = consumer.receive(pollInterval);

        if (message == null){
            logger.info("no message found");
            return null;
        }

        MetaData metadata = new MetaData();
        Enumeration en = message.getPropertyNames();
        while (en.hasMoreElements()){
            String key = (String)en.nextElement();
            metadata.put(key, message.getStringProperty(key));
        }

        if (message instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) message;
            String text = textMessage.getText();
            System.out.println("Received: " + text);
            
            data = new GenericData(metadata, new ByteArrayInputStream(text.getBytes()));
            return data;
        } else if(message instanceof BytesMessage) {
            BytesMessage bytesMessage = (BytesMessage) message;
            bytesMessage.reset();
            
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] b = new byte[1024];
            while (bytesMessage.readBytes(b) != -1) {
                bos.write(b);
            }
            b = bos.toByteArray();
            bos.close();

            data = new GenericData(metadata, new ByteArrayInputStream(b));
            return data;
        } else if(message instanceof StreamMessage) {
            StreamMessage streamMessage = (StreamMessage) message;
            streamMessage.reset();
            
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] b = new byte[1024];
            while (streamMessage.readBytes(b) != -1) {
                bos.write(b);
            }
            b = bos.toByteArray();
            bos.close();

            data = new GenericData(metadata, new ByteArrayInputStream(b));
            return data;            
        } else {
            logger.info("Received: " + message);
            data = null;
            throw new Exception("received unknow message type");
        }

    }

    public GenericData getCurrent() throws Exception {
        return data;
    }

    public void commit() throws Exception {
        message.acknowledge();
        session.commit();
    }

    public void rollback() throws Exception {
        message = null;
        session.rollback();
    }

    @Override
    public void finalize(){
        try {
            super.finalize();
            consumer.close();
            session.close();
            connection.close();
        } catch (Throwable ex) {
            logger.error("Exception closing JMS Connection",ex);
        }
    }
}
