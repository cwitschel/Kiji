/*
 * Generic Object holding all the data that is passed around inside the
 * Framework
 */
package kiji.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import org.apache.log4j.Logger;

/**
 *
 * @author Christian.Witschel
 */
public class GenericData {

    private MetaData metadata;
    private byte[] output;
    private byte[] input;
    private byte[] response;
    private static Logger logger = Logger.getLogger(GenericData.class);

    public GenericData(MetaData m, InputStream is) {
        metadata = m;

        if (is == null) {
            input = null;
        } else {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try {
                int b = -1;
                while ((b = is.read()) != -1) {
                    bos.write(b);
                }
                is.close();
                bos.close();
                input = bos.toByteArray();
            } catch (Exception e) {
                logger.error("Exception taking input", e);
            }
        }
        logger.debug("Input = "+new String(input));
    }

    /*
     * returns an inputstream of the data represented by this object
     */
    public InputStream getInputAsStream() {
        return new ByteArrayInputStream(input);
    }

    /*
     * returns a bytearray of the data represented by this object
     */
    public byte[] getInputAsByteArray() {
        return input;
    }

    /*
     * returns the meta data related to this data object
     * meta data holds some context information about the data. for example the original file name
     */
    public MetaData getMetadata() {
        if (metadata == null) {
            metadata = new MetaData();
        }
        return metadata;
    }

    public void setMetadata(MetaData metadata) {
        this.metadata = metadata;
    }

    public InputStream getOutputAsStream() {
        if (output == null) {
            output = new byte[0];
        }
        return new ByteArrayInputStream(output);
    }

    public byte[] getOutputAsByteArray() {
        if (output == null) {
            output = new byte[0];
        }
        return output;
    }

    public void writeOutput(InputStream is, boolean append) {

        if (append) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try {
                //appending, first write the old output
                if (output != null && output.length > 0) {
                    bos.write(output, 0, output.length);
                }

                //now append the new content
                int b = -1;
                while ((b = is.read()) != -1) {
                    bos.write(b);
                }
                is.close();
                bos.close();

                //set the new output
                output = bos.toByteArray();
            } catch (Exception e) {
                logger.error("Exception append writing output", e);
            }
        } else {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try {
                int b = -1;
                while ((b = is.read()) != -1) {
                    bos.write(b);
                }
                is.close();
                bos.close();
                //set the new output
                output = bos.toByteArray();
            } catch (Exception e) {
                logger.error("Exception writing output", e);
            }
        }
    }

    public InputStream getResponseAsStream() {
        if (response == null) {
            response = new byte[0];
        }
        return new ByteArrayInputStream(response);
    }

    public void writeResponse(InputStream is, boolean append) {

        if (append) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try {
                //appending, first write the old response
                if (response != null && response.length > 0) {
                    bos.write(response, 0, response.length);
                }

                //now append the new content
                int b = -1;
                while ((b = is.read()) != -1) {
                    bos.write(b);
                }
                is.close();
                bos.close();

                //set the new response
                response = bos.toByteArray();
            } catch (Exception e) {
                logger.error("Exception append writing output", e);
            }
        } else {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try {
                int b = -1;
                while ((b = is.read()) != -1) {
                    bos.write(b);
                }
                is.close();
                bos.close();
                //set the new response
                response = bos.toByteArray();
            } catch (Exception e) {
                logger.error("Exception writing output", e);
            }
        }
    }
}
