/*
 * This Writer consumes/deletes data given to him
 * usefull if you want to delete/empty/clean up a resource like queue/table/etc.
 */

package kiji.writer;

import kiji.data.GenericData;
import org.apache.commons.configuration.HierarchicalConfiguration.Node;
import org.apache.log4j.Logger;

/**
 *
 * @author christian
 */
public class TrashWriter implements Writer{

    private Node rootNode;
    private static Logger logger = Logger.getLogger(TrashWriter.class);

    public void setConfig(Node n) throws Exception {
        rootNode = n;
    }

    public void write(GenericData d) throws Exception {
        d = null;
    }

    public void commit() throws Exception {
        logger.info("commited trash");
    }

    public void rollback() throws Exception {
        logger.info("rollback trash");
    }

}
