/*
 * this interface contains the necessary methods all interfaces need to be able to run in the adapter
 */

package kiji.interfaces;

import org.apache.commons.configuration.HierarchicalConfiguration.Node;

/**
 *
 * @author Christian.Witschel
 */
public interface Interface {

    public void startup();
    public String getStatus();
    public boolean isRunning();
    public void setConfig(Node n);
    public void shutdown();
}
