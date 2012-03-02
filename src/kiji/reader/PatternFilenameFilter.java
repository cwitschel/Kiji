/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package kiji.reader;

import java.io.File;
import java.io.FilenameFilter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Christian.Witschel
 */
public class PatternFilenameFilter implements FilenameFilter{

    Pattern filePattern;
    public PatternFilenameFilter(Pattern p){
        filePattern = p;
    }

    @Override
    public boolean accept(File dir, String name) {
        if (name.endsWith(".part"))
            return false;

        Matcher matcher = filePattern.matcher(name);
        return matcher.matches();
    }

    public String getPattern(){
        return filePattern.pattern();
    }
}
