/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 * $Author: christian.witschel $:
 * $Date: 2011-04-21 12:12:11 +0200 (Thu, 21 Apr 2011) $:
 */

package kiji.activiti;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.Iterator;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.JavaDelegate;
import org.json.JSONObject;

/**
 *
 * @author christian.witschel
 */
public class ActivitiSocketTask implements JavaDelegate{


  public void execute(DelegateExecution execution) throws Exception {
    
    Socket s = new Socket("localhost", 41462);
    PrintWriter out = new PrintWriter(s.getOutputStream(), true);

    JSONObject json = new JSONObject();

    Iterator iter = execution.getVariableNames().iterator();
    while(iter.hasNext()){
        String key = (String)iter.next();
        json.put(key, execution.getVariable(key));
    }
    out.print(json.toString());
    out.close();
    s.close();

    System.gc();
  }

}
