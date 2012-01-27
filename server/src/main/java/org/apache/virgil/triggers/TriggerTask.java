package org.apache.virgil.triggers;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerTask extends TimerTask {
    private static Logger logger = LoggerFactory.getLogger(TriggerTask.class);

    @Override
    public void run() {
        Map<String, List<Trigger>> triggerMap = null;
        try {
            logger.debug("Running triggers @ [" + new Date() + "]");
            triggerMap = TriggerStore.getTriggers();
        } catch (Exception e){
            logger.error("Could not retrieve triggers.", e);
        }
        
        for (String path : triggerMap.keySet()){
            String keyspace = path.substring(path.indexOf(':'));
            String columnFamily = path.substring(path.indexOf(':'));
            logger.debug("[" + keyspace + "]:[" + columnFamily + "]");
        }
    }
    
}
