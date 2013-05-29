/*
 * This class load a remotely maintained configuration 
 */

package kiji.controller;

/**
 *
 * @author Christian.Witschel
 */
public class RemoteConfigThread extends Thread{

    private String url;

    public RemoteConfigThread(String u){
        url = u;
    }

    public void run(){

    }
}
