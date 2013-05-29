/*
 * represends the Meta Data
 */

package kiji.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.log4j.Logger;

/**
 *
 * @author c.witschel@gmail.com
 */
@SuppressWarnings("rawtypes")
public class MetaData extends HashMap{

    /**
	 * 
	 */
	private static final long serialVersionUID = 6382184518734075129L;
	Logger logger = Logger.getLogger(MetaData.class);

    //this method is for testing, loading the meta data from a file
    public void loadFromFile(File f){
        
    }

    public void fromString(String in) throws IOException{

        BufferedReader br = new BufferedReader(new StringReader(in));
        String line = null;
        while ((line = br.readLine())!=null){
            //bad Line - ignore
            if (line.indexOf('=') == -1){
                logger.warn("bad line while parsing metadata: "+line);
                continue;
            }

            String key = line.substring(0,line.indexOf('='));
            String value = line.substring(line.indexOf('=')+1);

            put(key, value);
        }
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        Iterator iter = keySet().iterator();
        while (iter.hasNext()){
            String key = (String)iter.next();
            Object o = get(key);
            if (o instanceof String)
                sb = sb.append(key).append("=").append(o.toString()).append('\n');
            else
                logger.warn("unsupported meta type for sending: "+o.getClass().getName());
        }


        return sb.toString();
    }
}
