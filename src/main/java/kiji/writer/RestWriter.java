/*
 * This class does currently not support transactional behaviour. the data will be send immediately once the write method is called.
 * $Author: christian.witschel $:
 * $Date: 2011-04-21 12:12:11 +0200 (Thu, 21 Apr 2011) $:
 */
package kiji.writer;

import java.io.IOException;
import java.io.InputStream;
import kiji.data.GenericData;
import kiji.data.MetaData;
import kiji.util.ParameterCheck;
import org.apache.commons.configuration.HierarchicalConfiguration.Node;
import org.apache.log4j.Logger;
import org.restlet.data.ChallengeScheme;
import org.restlet.resource.ClientResource;

/**
 *
 * @author Christian.Witschel
 */
public class RestWriter implements Writer {

    private static final String URL = "url";
    private static final String PROXY = "proxy";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static Logger logger = Logger.getLogger(RestWriter.class);

    private Node rootNode;

    public void setConfig(Node n) {
        rootNode = n;
    }

    public void write(GenericData data) throws Exception {
        MetaData metadata = data.getMetadata();
        InputStream is = data.getOutputAsStream();
        
        try {
            ClientResource cr = new ClientResource(ParameterCheck.getParameter(rootNode, URL));
            String user = ParameterCheck.getOptionalParameter(rootNode, USER);
            if (user != null)
                cr.setChallengeResponse(ChallengeScheme.HTTP_BASIC, user, ParameterCheck.getParameter(rootNode, PASSWORD));
            String reply = cr.post(is).getText();

            logger.debug(reply);

        } catch (Exception ex) {
            logger.error("Exception writing output to REST endpoint", ex);
            throw ex;
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException ex) {
                    logger.error("Excption closing output input", ex);
                }
            }
        }
    }

    public void commit() throws Exception {
        //nothing to commit!!!
    }

    public void rollback() throws Exception {
        //and also nothing to rollback!!!
    }
}
