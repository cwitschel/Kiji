/*
 * All transformers have to implement this interface
 * $Author: christian.witschel $:
 * $Date: 2011-04-21 12:12:11 +0200 (Thu, 21 Apr 2011) $
 */

package kiji.transformation;

import kiji.data.GenericData;
import org.apache.commons.configuration.HierarchicalConfiguration.Node;

/**
 *
 * @author christian.witschel
 */
public interface Transformer {

    public void setConfig(Node n) throws Exception;
    public GenericData transform(GenericData data) throws Exception;
}
