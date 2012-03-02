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
public class FileNameComparator implements Comparator{

    public int compare(Object o1, Object o2) {
        File f1 = (File) o1;
        File f2 = (File) o2;

        return f1.getAbsolutePath().compareToIgnoreCase(f2.getAbsolutePath());

    }
}
