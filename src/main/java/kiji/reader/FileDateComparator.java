/*
 * this class expects 2 file objects and compares their last modified date
 */

package kiji.reader;

import java.io.File;
import java.util.Comparator;

/**
 *
 * @author c.witschel@gmail.com
 */
@SuppressWarnings("rawtypes")
public class FileDateComparator implements Comparator{

    public int compare(Object o1, Object o2) {
        File f1 = (File)o1;
        File f2 = (File)o2;

        if (f1.lastModified() < f2.lastModified()){
            return -1;
        }
        else if(f1.lastModified() > f2.lastModified()){
            return 1;
        }
        return 0;
    }
}
