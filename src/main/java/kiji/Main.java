/*
 * This is the Main class. Convenience Class to start he Adapter directly from a jar file. 
 * $Author: christian.witschel $:
 * $Date: 2011-04-30 20:59:07 +0200 (Sa, 30 Apr 2011) $:
 */
package kiji;

import kiji.controller.ControllerThread;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author c.witschel@gmail.com
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        PropertyConfigurator.configure("./conf/log4j.properties");

        Logger logger = Logger.getLogger(Main.class.getName());

        logger.info("Adapter starting");
        logger.info("Adapter version $Rev: 30 $:");
 
        ControllerThread.getInstance().start();
    }
}
