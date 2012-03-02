/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package kiji.reader;

import java.io.File;
import java.util.Comparator;

/**
 *
 * @author Christian.Witschel
 */
public class FileLengthComparator implements Comparator {

    public int compare(Object o1, Object o2) {
        File f1 = (File) o1;
        File f2 = (File) o2;
        if (f1.length() < f2.length()) {
            return -1;
        } else if (f1.length() > f2.length()) {
            return 1;
        }

        return 0;
    }
}
