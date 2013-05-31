/*
 * this is the super class of all interfaces. it will perform reading transforming and writing of data
 * depending on its configuration.
 * you can configure more then one reader, transformation and writer.
 * However there is no aggregation feature yet. Meaning each reader is used one after the other to 
 * collect its next data chunk and then calls the configured transformers and writers.
 * YOU CANNOT MERGE THE DATA OF DIFFERENT READERS in this version. 
 * 
 */
package kiji.interfaces;

import java.util.Collection;
import java.util.Iterator;
import kiji.data.GenericData;
import kiji.reader.Reader;
import kiji.transformation.Transformer;
import kiji.util.ParameterCheck;
import kiji.writer.Writer;
import org.apache.commons.configuration.HierarchicalConfiguration.Node;
import org.apache.log4j.Logger;

/**
 *
 * @author c.witschel@gmail.com
 */
public class GenericInterface extends Thread implements Interface {

    public static final String SLEEP_ON_ERROR = "sleeponerror";
    public static final String ID = "id";
    public static final String INPUT = "input";
    public static final String TRANSFORM = "transform";
    public static final String OUTPUT = "output";
    public static final String READER = "reader";
    public static final String TRANSFORMER = "transformer";
    public static final String WRITER = "writer";
    private Node rootNode;
    private Node[] inputNodes;
    private Node[] transformerNodes;
    private Node[] outputNodes;
    private String status = "initial";
    private Reader[] readers;
    private Transformer[] transformers;
    private Writer[] writers;
    private boolean run = false;
    private long sleepOnError;
    private static Logger logger = Logger.getLogger(GenericInterface.class.getName());

    private void setStatus(String s) {
        if (status != null) {
            status = s;
        } else {
            setStatus("unknown/null");
        }
    }

    public void startup() throws Exception {

        //initialize sleep on error
        if (rootNode.getChildrenCount(SLEEP_ON_ERROR) == 1) {
            sleepOnError = Long.parseLong((String) ((Node) rootNode.getChildren(SLEEP_ON_ERROR).get(0)).getValue());
        } else {
            logger.error("No or too many <" + SLEEP_ON_ERROR + "> tags specified for this interface. aborting.");
        }

        try {
            String name = ParameterCheck.getParameter(rootNode, ID);
            setName(name);
        } catch (Exception ex) {
            logger.error("<" + ID + "> parameter nor specified", ex);
            throw new Exception("<" + ID + "> parameter nor specified", ex);
        }

        if (rootNode.getChildrenCount(INPUT) > 0) {
            Collection<Node> col = rootNode.getChildren(INPUT);
            Iterator<Node> iter = col.iterator();
            inputNodes = new Node[col.size()];
            readers = new Reader[col.size()];
            int i = 0;
            while (iter.hasNext()) {
                Node n = (Node) iter.next();
                inputNodes[i] = n;

                try {
                    readers[i] = (Reader) Class.forName((String) ((Node) n.getChildren(READER).get(0)).getValue()).newInstance();
                    readers[i].setConfig(n);
                } catch (InstantiationException ex) {
                    logger.error("Cannot create Reader", ex);
                    throw new Exception("Cannot create Reader. Aborting interface "+getName(), ex);
                } catch (IllegalAccessException ex) {
                    logger.error("Cannot create Reader", ex);
                    throw new Exception("Cannot create Reader. Aborting interface "+getName(), ex);
                } catch (Exception ex) {
                    logger.error("Cannot create Reader", ex);
                    throw new Exception("Cannot create Reader. Aborting interface "+getName(), ex);
                }
                i++;
            }
        } else {
            logger.error("No <" + INPUT + "> tags specified for this interface. aborting.");
            throw new Exception("No <" + INPUT + "> tags specified for this interface. Aborting interface "+getName());
        }
        
         if (rootNode.getChildrenCount(TRANSFORM) > 0) {
            Collection<Node> col = rootNode.getChildren(TRANSFORM);
            Iterator<Node> iter = col.iterator();
            transformerNodes = new Node[col.size()];
            transformers = new Transformer[col.size()];
            int i = 0;
            while (iter.hasNext()) {
                Node n = (Node) iter.next();
                transformerNodes[i] = n;

                try {
                    transformers[i] = (Transformer) Class.forName((String) ((Node) n.getChildren(TRANSFORMER).get(0)).getValue()).newInstance();
                    transformers[i].setConfig(n);
                } catch (InstantiationException ex) {
                    logger.error("Cannot create Transformer. Aborting", ex);
                    throw new Exception("Cannot create Transformer. Aborting interface "+getName(), ex);
                } catch (IllegalAccessException ex) {
                    logger.error("Cannot create Transformer. Aborting", ex);
                    throw new Exception("Cannot create Transformer. Aborting interface "+getName(), ex);
                } catch (ClassNotFoundException ex) {
                    logger.error("Cannot create Transformer, check config or classpath", ex);
                    throw new Exception("Cannot create Transformer. Check config or classpath", ex);
                } catch (Exception ex) {
                    logger.error("Cannot create Transformer. Aborting", ex);
                    throw new Exception("Cannot create Transformer. Aborting interface "+getName(), ex);
                }
                i++;
            }
        }

        if (rootNode.getChildrenCount(OUTPUT) > 0) {
            Collection<Node> col = rootNode.getChildren(OUTPUT);
            Iterator<Node> iter = col.iterator();
            outputNodes = new Node[col.size()];
            writers = new Writer[col.size()];
            int i = 0;
            while (iter.hasNext()) {
                Node n = (Node) iter.next();
                outputNodes[i] = n;

                try {
                    writers[i] = (Writer) Class.forName((String) ((Node) n.getChildren(WRITER).get(0)).getValue()).newInstance();
                    writers[i].setConfig(n);
                } catch (InstantiationException ex) {
                    logger.error("Cannot create Writer. Aborting", ex);
                    throw new Exception("Cannot create Writer. Aborting interface "+getName(), ex);
                } catch (IllegalAccessException ex) {
                    logger.error("Cannot create Writer. Aborting", ex);
                    throw new Exception("Cannot create Writer. Aborting interface "+getName(), ex);
                } catch (ClassNotFoundException ex) {
                    logger.error("Cannot create Reader, Check config or classpath", ex);
                    throw new Exception("Cannot create Writer. Check config or classpath", ex);
                } catch (Exception ex) {
                    logger.error("Cannot create Writer. Aborting", ex);
                    throw new Exception("Cannot create Writer. Aborting interface "+getName(), ex);
                }
                i++;
            }
        } else {
            logger.error("No <" + OUTPUT + "> tags specified for this interface. Aborting interface "+getName());
            throw new Exception("No <" + OUTPUT + "> tags specified for this interface. Aborting interface "+getName());
        }

        setStatus("ready");
        start();
        setStatus("running");
        logger.info("Interface " + getName() + " started running");
    }

    /*
     * this method keeps running the interface while the run option is true.
     * basically it loops continuously through the configured readers initiating a transaction
     * once it leaves the endless loop, it will shutdown all the readers and end this interface.
     * (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    public void run() {
        run = true;
        int i = 0;
        while (run) {
            try {
                processTransaction(readers[i]);
                i++;
                if (i >= readers.length) {
                    i = 0;
                }
            } catch (Exception ex) {
                logger.error("General exception in interface, aborting", ex);
                run = false;
                setStatus("aborted with error");
                break;
            }
        }
        //shutdown readers
        for (int x = 0; x < readers.length; x++){
        	readers[x].shutdown();
        }
        setStatus("ended");
        logger.info("Interface " + getName() + " ended");
    }

    public String getStatus() {
        return status;
    }

    public boolean isRunning() {
        return isAlive();
    }

    public void setConfig(Node n) {
        rootNode = n;
    }

    public void shutdown() {
    	run = false;
        setStatus("ending");
    }

    /*
     * first the reader is called to receive the next portion of data.
     * then the configured transformers are called on order of configuration to transform the data
     * last the configured writers are called to distribute the data to its targets.
     */
    private void processTransaction(Reader reader) throws Exception {
        GenericData data = null;
        try {
            //get input
            data = reader.getNext();
            if (data == null) {
                //nothing found
                return;
            }
            

            //if transformation is configured
            if (transformers != null){
                for (int i = 0; i < transformers.length; i++) {
                    data = transformers[i].transform(data);
                }
            }
            else{
                data.writeOutput(data.getInputAsStream(), false);
            }

            //write output
            for (int i = 0; i < writers.length; i++) {
                //write output
                writers[i].write(data);
            }

            reader.commit();
            for (int i = 0; i < writers.length; i++) {
                writers[i].commit();
            }
            logger.info("successfully processed transaction " + data.getMetadata().get("id"));
        } catch (Exception ex) {
            logger.error("Exception processing transaction", ex);
            reader.rollback();
            for (int i = 0; i < writers.length; i++) {
                writers[i].rollback();
            }
            logger.info("rolled back transaction");

            try {
                logger.info("sleeping on error for " + sleepOnError + " ms");
                Thread.sleep(sleepOnError);
            } catch (InterruptedException ex1) {
                logger.error("Exception sleeping on error", ex1);
            }
        } finally {
            
        }
    }
}
