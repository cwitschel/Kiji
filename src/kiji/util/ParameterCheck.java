/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package kiji.util;

import org.apache.commons.configuration.HierarchicalConfiguration.Node;
import org.apache.log4j.Logger;

/**
 *
 * @author Christian.Witschel
 */
public class ParameterCheck {

    public static String getParameter(Node n, String key) throws Exception{
        if (n.getChildrenCount(key) != 1) {
            throw new Exception("Config parameter <"+key+"> is missing");
        }
        else
            return (String)((Node)n.getChildren(key).get(0)).getValue();
    }

    public static String getOptionalParameter(Node n, String key) throws Exception{
        if (n.getChildrenCount(key) != 1) {
            return null;
        }
        else
            return (String)((Node)n.getChildren(key).get(0)).getValue();
    }
}
