/*
 * General Interface that all writers have to implement.
 */

package kiji.writer;

import kiji.data.GenericData;
import org.apache.commons.configuration.HierarchicalConfiguration.Node;

/**
 *
 * @author Christian.Witschel
 */
public interface Writer {
    public void setConfig(Node n) throws Exception;
    public void write(GenericData d) throws Exception;
    public void commit() throws Exception;
    public void rollback() throws Exception;
}
