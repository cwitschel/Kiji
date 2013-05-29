/*
 * This Class expects 2 file objects to be compared. compared is the absolute path.
 */

package kiji.reader;

import java.io.File;
import java.util.Comparator;

/**
 *
 * @author c.witschel@gmail.com
 */
@SuppressWarnings("rawtypes")
public class FileNameComparator implements Comparator{

    public int compare(Object o1, Object o2) {
        File f1 = (File) o1;
        File f2 = (File) o2;

        return f1.getAbsolutePath().compareToIgnoreCase(f2.getAbsolutePath());

    }
}
