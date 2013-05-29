/*
 * This class enables the adapter to write messages to a JMS provider such as activeMQ, hornetMQ or websphereMQ
 */
package kiji.writer;

import java.io.InputStream;
import java.util.Iterator;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import kiji.data.GenericData;
import kiji.util.ParameterCheck;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.configuration.HierarchicalConfiguration.Node;
import org.apache.log4j.Logger;

/**
 *
 * @author Christian.Witschel
 */
public class JmsWriter implements Writer {

    private static final String JMS_PROVIDER = "jmsprovider";
    private static final String JMS_URL = "jmsurl";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String TRANSACTIONAL = "transactional";
    private static final String QUEUE = "queue";
    private Node rootNode;
    private Logger logger = Logger.getLogger(JmsWriter.class.getName());
    private Writer actualWriter = null;
    private Session session;
    private Destination destination;

    public void setConfig(Node n) throws Exception {
        rootNode = n;

        ConnectionFactory connectionFactory = null;
        if (ParameterCheck.getParameter(n, JMS_PROVIDER).equals("ACTIVEMQ")) {
            // Create a ConnectionFactory
            connectionFactory = new ActiveMQConnectionFactory(ParameterCheck.getParameter(rootNode, USER),
                    ParameterCheck.getParameter(rootNode, PASSWORD),
                    ParameterCheck.getParameter(rootNode, JMS_URL));

        } else {
            throw new Exception("JMS Provider " + ParameterCheck.getParameter(n, JMS_PROVIDER) + " not supported yet.");
        }

        Connection connection;
        connection = connectionFactory.createConnection();
        connection.start();
        boolean transacted = false;
        if (ParameterCheck.getParameter(rootNode, TRANSACTIONAL).equals("true")) {
            transacted = true;
        }
        if (transacted) {
            session = connection.createSession(transacted, Session.SESSION_TRANSACTED);
        } else {
            session = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);
        }
        destination = session.createQueue(ParameterCheck.getParameter(rootNode, QUEUE));

    }

    public void write(GenericData d) throws Exception {
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        BytesMessage message = session.createBytesMessage();
        InputStream is = d.getOutputAsStream();
        byte[] b = new byte[1024];
        while (is.read(b) != -1) {
            message.writeBytes(b);
        }
        is.close();

        Iterator iter = d.getMetadata().keySet().iterator();
        while (iter.hasNext()){
            String key = (String)iter.next();
            Object o = d.getMetadata().get(key);
            if (o instanceof String)
                message.setStringProperty(key, (String)o);
            else
                logger.debug("unsupported meta type for sending: "+o.getClass().getName());
        }

        producer.send(message);
        message.reset();
        logger.info("Send message "+message.getJMSMessageID());
        logger.debug("Send message: "+message);
    }

    public void commit() throws Exception {
        session.commit();
    }

    public void rollback() throws Exception {
        session.rollback();
    }

}
