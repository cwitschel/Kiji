/*
 * 
 */

package kiji.reader;

import kiji.data.GenericData;
import org.apache.commons.configuration.HierarchicalConfiguration.Node;

/**
 *
 * @author Christian.Witschel
 */
public interface Reader{

    public void setConfig(Node n) throws Exception;
    public GenericData getNext() throws Exception;
    //public GenericData getCurrent() throws Exception;
    public void commit() throws Exception;
    public void rollback() throws Exception;
    public void shutdown();

}
