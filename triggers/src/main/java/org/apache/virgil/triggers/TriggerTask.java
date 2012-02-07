package org.apache.virgil.triggers;

import java.util.List;
import java.util.Map;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerTask extends TimerTask {
    private static Logger logger = LoggerFactory.getLogger(TriggerTask.class);

    @Override
    public void run() {
        try {
            if (ConfigurationStore.getStore().isCommitLogEnabled()) {
                Map<String, List<Trigger>> triggerMap = null;
                logger.debug("Running triggers.");
                triggerMap = TriggerStore.getStore().getTriggers();
                List<LogEntry> logEntries = DistributedCommitLog.getLog().getPending();
                for (LogEntry logEntry : logEntries) {
                    logger.debug("Processing Entry [" + logEntry.getUuid() + "]:[" + logEntry.getKeyspace() + "]:["
                            + logEntry.getColumnFamily() + "]");
                    String path = logEntry.getKeyspace() + ":" + logEntry.getColumnFamily();
                    List<Trigger> triggers = triggerMap.get(path);
                    if (triggers != null) {
                        for (Trigger trigger : triggers) {
                            trigger.process(logEntry);
                        }
                    }

                    // Provided all processed properly, remove the logEntry
                    DistributedCommitLog.getLog().removeLogEntry(logEntry);
                }
            } else {
                logger.debug("Skipping trigger execution because commit log is disabled.");
            }
        } catch (Throwable t) {
            logger.error("Could not execute triggers.", t);
        }
    }
}
