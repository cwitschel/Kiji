/*
 * Writes the given Data to a file
 */
package kiji.writer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.log4j.Logger;
import kiji.data.GenericData;
import kiji.data.MetaData;
import org.apache.commons.configuration.HierarchicalConfiguration.Node;

/**
 *
 * @author Christian.Witschel
 */
public class FileWriter implements Writer {

    private static final String OUTPUT_DIRECTORY = "outputdirectory";
    private Node rootNode;
    File outFile;
    File stagingFile;
    private Logger logger = Logger.getLogger(FileWriter.class.getName());

    public void setConfig(Node n) {
        rootNode = n;
    }

    public void write(GenericData data) throws Exception{
        MetaData metadata = data.getMetadata();
        InputStream is = data.getOutputAsStream();

        String file = "file_"+System.currentTimeMillis();
        if (metadata.get("file")!=null)
            file = (String)metadata.get("file");

        if (metadata.get("dir") == null) {

            if (rootNode.getChildrenCount(OUTPUT_DIRECTORY) == 1) {
                String dir = (String) ((Node) rootNode.getChildren(OUTPUT_DIRECTORY).get(0)).getValue();
                outFile = new File(dir + "/" + file + ".part");
            } else {
                logger.error("<"+OUTPUT_DIRECTORY+"> config entry is missing");
                throw new Exception("<"+OUTPUT_DIRECTORY+"> config entry is missing");
            }
        }
        else{
            outFile = new File(metadata.get("dir") + "/" + file + ".part");
        }

        BufferedOutputStream bos = null;
        try{
            bos = new BufferedOutputStream(new FileOutputStream(outFile, true));
            byte[] b = new byte[1024];
            while(is.read(b)!=-1)
                bos.write(b);
        } catch (FileNotFoundException ex) {
            logger.error("Exception opening output file", ex);
            throw ex;
        } catch (IOException e) {
            logger.error("Exceptione writing output file", e);
            throw e;
        }
        finally{
            if (bos != null)
                try {
                bos.close();
            } catch (IOException ex) {
                logger.error("Excption closing output file", ex);
            }
            if (is != null)
                try {
                is.close();
            } catch (IOException ex) {
                logger.error("Exception closing output", ex);
            }
        }

        logger.info("File .part written successfull, renaming to final filename");
        outFile.renameTo(new File(outFile.getAbsolutePath().substring(0, outFile.getAbsolutePath().length()-5)));
    }

    public void commit() {
        outFile = null;
    }

    public void rollback() throws Exception{
        //writer not used yet
        if(outFile == null)
            return;
        //no output file to delete
        else if(!outFile.exists())
            return;
        //delete partial file
        else if(!outFile.delete())
            throw new Exception("Cannot rollback file "+outFile);
    }
}
