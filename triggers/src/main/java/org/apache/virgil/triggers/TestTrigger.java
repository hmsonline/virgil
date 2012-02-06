package org.apache.virgil.triggers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTrigger implements Trigger {
    private static Logger logger = LoggerFactory.getLogger(TestTrigger.class);

    public void process(LogEntry logEntry) {
        logger.debug("Trigger processing : [" + logEntry.getUuid() + "]");          
    }
}
