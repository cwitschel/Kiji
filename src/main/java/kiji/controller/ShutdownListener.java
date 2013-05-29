/*
 * This class listeners for the vm to be ended and then initiates the shutdown of the controller thread.
 */

package kiji.controller;

/**
 * 
 * @author c.witschel@gmail.com
 */
public class ShutdownListener extends Thread {

	private ControllerThread controller = null;

	public ShutdownListener(ControllerThread ct) {
		controller = ct;
	}

	public void run() {
		controller.shutdown();
		try {
			controller.join();
		} catch (InterruptedException e) {
			// this is not supposed to happen. Possibly someone force killed the
			// process.
			e.printStackTrace();
		}
	}

}
