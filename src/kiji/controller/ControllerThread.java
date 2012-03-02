/*
 * this class controlls the running of the attached components
 */

package kiji.controller;

import java.io.FileInputStream;
import java.util.HashSet;
import java.util.Iterator;
import kiji.interfaces.GenericInterface;
import kiji.interfaces.Interface;
import org.apache.commons.configuration.HierarchicalConfiguration.Node;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

/**
 *
 * @author Christian.Witschel
 */
public class ControllerThread extends Thread{

    private XMLConfiguration adminConfig;
    private XMLConfiguration interfacesConfig;
    private HashSet interfaces;
    private Logger logger = Logger.getLogger(ControllerThread.class);
    public static boolean stop = false;
    private static ControllerThread me;

    private ControllerThread(){
        //load configuration
        try {
            adminConfig  = new XMLConfiguration();
            adminConfig.load(new FileInputStream("conf/general.xml"));
        } catch (Exception ex) {
            logger.error("Cannot load general configuration", ex);
            System.exit(1);
        }

        try {
            interfacesConfig = new XMLConfiguration();
            interfacesConfig.load(new FileInputStream("conf/interfaces.xml"));
        } catch (Exception ex) {
            logger.error("Cannot load interfaces configuration", ex);
            System.exit(1);
        }
    }

    public static ControllerThread getInstance(){
        if (me == null)
            me = new ControllerThread();
        return me;
    }


    @Override
    public void run(){
        startup();

        while(!stop){
            try {
                sleep(60000);
            } catch (InterruptedException ex) {
                logger.error("Interrupted while sleeping", ex);
            }
            //housekeeping
            System.runFinalization();
            System.gc();
        }
    }

 /**
 * this method starts the interfaces based on the current configuration
 */
    public void startup() {

        Node root = interfacesConfig.getRoot();
        Iterator iter = root.getChildren("interface").iterator();
        while (iter.hasNext()) {
            Node interfaceNode = (Node) iter.next();

            //Interface i = (Interface) Class.forName((String)((Node)interfaceNode.getChildren("class").get(0)).getValue()).newInstance();
            Interface i = new GenericInterface();
            i.setConfig(interfaceNode);
            i.startup();
        }
    }

    /**
     * this method stops the interfaces based on the current configuration
     */
    public void shutdown(){

        stop = true;

        while (interfaces.size() > 0){
            Iterator iter = interfaces.iterator();
            while(iter.hasNext()){
                Interface it = (Interface)iter.next();

                if (it == null){
                    iter.remove();
                }
                else if(it.isRunning()){
                    it.shutdown();
                }
                else{
                    iter.remove();
                }
            }
        }
    }
}
