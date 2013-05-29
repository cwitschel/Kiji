/*
 * Exception indicates to the GenericInterface Class and ControllerThread
 * that this interface is beyond recovery or restart
 */

package kiji.interfaces;

/**
 *
 * @author christian
 */
public class FatalException extends Exception{

    public FatalException(String in){
        super(in);
    }

    public FatalException(String in, Exception e){
        super(in, e);
    }
}
