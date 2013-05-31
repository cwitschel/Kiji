/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package kiji.reader;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import kiji.data.GenericData;
import kiji.data.MetaData;
import kiji.interfaces.FatalException;
import org.apache.commons.configuration.HierarchicalConfiguration.Node;
import org.apache.log4j.Logger;

/**
 *
 * @author Christian.Witschel
 */
public class FileReader implements Reader{

    private static final String POLL_INTERVAL = "pollinterval";
    private static final String ARCHIVE_DIRECTORY = "archivedirectory";
    private static final String INPUT_FILE = "inputfile";
    private static Logger logger = Logger.getLogger(FileReader.class);
    private Node rootNode;
    private File sourceDir;
    private File archiveDir;
    private File currentFile;
    private PatternFilenameFilter fileFilter;
    private long pollInterval;
    private long housekeepingInterval;
    private MetaData metadata;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmssSSS");

    public void setConfig(Node n) throws Exception {
        rootNode = n;

        //initialize poll interval
        if (rootNode.getChildrenCount(POLL_INTERVAL)==1)
            pollInterval = Long.parseLong((String)((Node)rootNode.getChildren(POLL_INTERVAL).get(0)).getValue());
        else{
            logger.error("No or too many <"+POLL_INTERVAL+"> tags specified for this interface. aborting.");
        }
        
        //initialize archiving directory
        if (rootNode.getChildrenCount(ARCHIVE_DIRECTORY)==1){
            archiveDir = new File((String)((Node)rootNode.getChildren(ARCHIVE_DIRECTORY).get(0)).getValue());
            if (!archiveDir.exists())
                archiveDir.mkdirs();
        }
        else{
            logger.warn("Archiving disabled. No or too many <"+ARCHIVE_DIRECTORY+"> tags specified for this interface.");
        }
        

        //initialize source directory
        String s = null;
        if (rootNode.getChildrenCount(INPUT_FILE)==1)
            s = (String)((Node)rootNode.getChildren(INPUT_FILE).get(0)).getValue();
        else{
            throw new FatalException("No or too many <"+INPUT_FILE+"> tags specified for this interface. aborting.");
        }
        s = s.replace('\\', '/');
        sourceDir = new File(s.substring(0, s.lastIndexOf('/')));

        //if the directory is not existing create it
        if (!sourceDir.exists())
            sourceDir.mkdirs();

        String stPattern = s.substring(s.lastIndexOf('/') + 1);
        //replace . with \.
        Pattern pattern = Pattern.compile("\\.");
        Matcher matcher = pattern.matcher(stPattern);
        if (matcher.find()) {
            stPattern = matcher.replaceAll("\\.");
        }

        //replace * with .*?
        pattern = Pattern.compile("\\*");
        matcher = pattern.matcher(stPattern);
        if (matcher.find()) {
            stPattern = matcher.replaceAll(".*?");
        }

        pattern = Pattern.compile(stPattern);

        fileFilter = new PatternFilenameFilter(pattern);

    }

    public GenericData getNext() throws Exception{
        try {
            if (rootNode == null) {
                throw new Exception("no config specified");
            }

            if (currentFile != null){
                logger.warn("Reading same file again?!? "+currentFile.getAbsolutePath()+" for pattern");

                metadata = new MetaData();
                metadata.put("file", currentFile.getName());
                metadata.put("timestamp", sdf.format(new Date()));
                InputStream is = new FileInputStream(currentFile);

                return new GenericData(getMetadata(), is);
            }

            InputStream is = null;
            File[] list = sourceDir.listFiles(fileFilter);
            if (list.length == 0) {
                logger.info("No File found in directory "+sourceDir+" for pattern "+fileFilter.getPattern());
                //no matching file found, sleep
                Thread.sleep(pollInterval);
                return null;
            }

            metadata = new MetaData();
            metadata.put("id", list[0].getAbsolutePath());
            metadata.put("file", list[0].getName());
            metadata.put("timestamp", sdf.format(new Date()));
            is = new FileInputStream(list[0]);
            currentFile = list[0];
            
            return new GenericData(getMetadata(), is);
        } catch (Exception ex) {
            logger.error("Exception getting next File", ex);
            throw ex;
        }
    }

    private void doHouseKeeping() {
		// TODO Auto-generated method stub
		
	}

	public MetaData getMetadata() {
        return metadata;
    }

    public void commit() throws Exception{

        if (archiveDir == null){
            currentFile.delete();
            currentFile = null;
            return;
        }

        if (!currentFile.renameTo(new File(archiveDir.getAbsolutePath()+"/"+metadata.get("timestamp")+"_"+currentFile.getName()))){
            throw new Exception("Cannot move file"+currentFile.getCanonicalPath()+"to archive");
        }
        else{
            currentFile = null;
            //do housekeeping
            doHouseKeeping();
        }
    }

    public void rollback() {
        currentFile = null;
    }
    
    /*
     * there is nothing to do here, since no resources are held permanently during lifetime
     * (non-Javadoc)
     * @see kiji.reader.Reader#shutdown()
     */
    public void shutdown(){
        try {
     
        } catch (Throwable ex) {
            logger.error("Exception closing file Connection",ex);
        }
    }
}
