/*
 * this class controls the running of the attached components
 * The individual interfaces will run parallel independent from each other
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
 * @author c.witschel@gmail.com
 */
public class ControllerThread extends Thread {

	private XMLConfiguration adminConfig;
	private XMLConfiguration interfacesConfig;
	private HashSet<Interface> interfaces;
	private Logger logger = Logger.getLogger(ControllerThread.class);
	public static boolean stop = false;
	private static ControllerThread me;

	private ControllerThread() throws Exception {
		// load configuration
		try {
			adminConfig = new XMLConfiguration();
			adminConfig.load(new FileInputStream("conf/general.xml"));
		} catch (Exception ex) {
			logger.error("Cannot load general configuration", ex);
			throw new Exception("Cannot load general configuration", ex);
		}

		try {
			interfacesConfig = new XMLConfiguration();
			interfacesConfig.load(new FileInputStream("conf/interfaces.xml"));
		} catch (Exception ex) {
			logger.error("Cannot load interfaces configuration", ex);
			throw new Exception("Cannot load interfaces configuration", ex);
		}
	}

	public static ControllerThread getInstance() {
		if (me == null){
			try {
				me = new ControllerThread();
			} catch (Exception e) {
				return null;
			}
		}
		return me;
	}

	@Override
	public void run() {
		// now that the controller is starting, its time to register the
		// shutdown hook to listen for the JVM to halt
		Runtime.getRuntime().addShutdownHook(new ShutdownListener(this));

		startup();

		while (!stop) {
			try {
				sleep(60000);
			} catch (InterruptedException ex) {
				logger.error("Interrupted while sleeping", ex);
			}

			check();

			// housekeeping
			System.runFinalization();
			System.gc();
		}
	}

	private void check() {
		if (interfaces.size() > 0) {
			Iterator<Interface> iter = interfaces.iterator();
			while (iter.hasNext()) {
				Interface it = (Interface) iter.next();

				if (it == null) {
					iter.remove();
				} else if (it.isRunning()) {
					// all is good
				} else {
					// Warning interface is not running
					logger.warn("Interface " + ((Thread) it).getName()
							+ "is not running");
					// [TODO:] decide whether to shutdown or restart the
					// interface. make configurable.
				}
			}
		}
	}

	/**
	 * this method starts the interfaces based on the current configuration
	 */
	public void startup() {

		//make sure this method is only called once during lifetime
		if (interfaces == null)
			interfaces = new HashSet<Interface>(); 
		else
			return;
		
		Node root = interfacesConfig.getRoot();
		Iterator<Node> iter = root.getChildren("interface").iterator();
		while (iter.hasNext()) {
			Node interfaceNode = (Node) iter.next();

			Interface i = new GenericInterface();
			i.setConfig(interfaceNode);
			try {
				i.startup();
			} catch (Exception e) {
				logger.error("Could not start interface " + ((GenericInterface)i).getName(), e);
				continue;
			}
			interfaces.add(i);
			logger.info("started interface " + ((GenericInterface)i).getName());
		}
	}

	/**
	 * this method stops the interfaces based on the current configuration
	 */
	public void shutdown() {

		stop = true;
		
		while (interfaces.size() > 0) {
			Iterator iter = interfaces.iterator();
			while (iter.hasNext()) {
				Interface it = (Interface) iter.next();

				if (it == null) {
					iter.remove();
				} else if (it.isRunning()) {
					it.shutdown();	
					System.out.println("interface " + ((GenericInterface)it).getName() + " is still running. Initiating shutdown.");
				} else {
					iter.remove();
				}
			}
			
			//give the interfaces some time to shutdown
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				//no big deal
				e.printStackTrace();
			}
		}
		
		
	}
}
