/*
 * $Author: christian.witschel $:
 * $Date: 2011-04-21 12:12:11 +0200 (Thu, 21 Apr 2011) $:
 */

package kiji.transformation;

import java.io.ByteArrayInputStream;
import java.util.Iterator;
import kiji.data.GenericData;
import kiji.data.MetaData;
import kiji.util.ParameterCheck;
import org.apache.commons.configuration.HierarchicalConfiguration.Node;
import org.apache.log4j.Logger;
import org.json.JSONObject;

/**
 *
 * @author christian.witschel
 */
public class ActivitiJsonTransformer implements Transformer{

    private static final String PROCESS_DEFINITION_ID = "processdefinitionid";
    private static final String BUSINESS_KEY = "businesskey";
    private static Logger logger = Logger.getLogger(ActivitiJsonTransformer.class);
    private Node rootNode;

    public GenericData transform(GenericData data) throws Exception{

//       {
//            "processDefinitionId":"financialReport:1:1700",
//            "businessKey":"order-4711"
//       }

        JSONObject json = new JSONObject();

        json.put("processDefinitionId", ParameterCheck.getParameter(rootNode, PROCESS_DEFINITION_ID));
        json.put("businessKey", ParameterCheck.getParameter(rootNode, BUSINESS_KEY));
        MetaData metadata = data.getMetadata();
        Iterator iter = metadata.keySet().iterator();
        while(iter.hasNext()){
            String key = (String)iter.next();
            json.put(key, metadata.get(key));
        }
        json.put("data", new String(data.getInputAsByteArray()));

        data.writeOutput(new ByteArrayInputStream(json.toString().getBytes()), false);

        logger.debug("transformed into "+json.toString());
        return data;
    }

    public void setConfig(Node n) throws Exception {
        rootNode = n;
    }
}
