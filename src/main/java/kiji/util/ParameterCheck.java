/*
 * Util class for reading Node Parameters.
 */

package kiji.util;

import org.apache.commons.configuration.HierarchicalConfiguration.Node;

/**
 *
 * @author c.witschel@gmail.com
 */
public class ParameterCheck {

	/*
	 * Method for mandatory parameters. Parameter nor present leads to an exception to be thrown
	 */
    public static String getParameter(Node n, String key) throws Exception{
        if (n.getChildrenCount(key) != 1) {
            throw new Exception("Config parameter <"+key+"> is missing");
        }
        else
            return (String)((Node)n.getChildren(key).get(0)).getValue();
    }

    /*
     * Method for optional parameters. Parameter not present leads to null being return. 
     */
    public static String getOptionalParameter(Node n, String key) throws Exception{
        if (n.getChildrenCount(key) != 1) {
            return null;
        }
        else
            return (String)((Node)n.getChildren(key).get(0)).getValue();
    }
}
