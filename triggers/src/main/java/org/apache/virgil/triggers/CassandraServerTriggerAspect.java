package org.apache.virgil.triggers;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
public class CassandraServerTriggerAspect {
    private static Logger logger = LoggerFactory.getLogger(CassandraServerTriggerAspect.class);

    @Around("execution(* org.apache.cassandra.thrift.CassandraServer.doInsert(..))")
    public void writeToCommitLog(ProceedingJoinPoint thisJoinPoint) throws Throwable {
        if (ConfigurationStore.getStore().isCommitLogEnabled()) {
            try {
                ConsistencyLevel consistencyLevel = (ConsistencyLevel) thisJoinPoint.getArgs()[0];
                @SuppressWarnings("unchecked")
                List<IMutation> mutations = (List<IMutation>) thisJoinPoint.getArgs()[1];
                List<LogEntry> logEntries = writePending(consistencyLevel, mutations);
                thisJoinPoint.proceed(thisJoinPoint.getArgs());
                writeCommitted(logEntries);
                // TODO: Catch Invalid Request separately, and remove the
                // pending.
            } catch (Throwable t) {
                logger.error("Could not write to cassandra!", t);
                t.printStackTrace();
                throw t;
            }
        } else {
            thisJoinPoint.proceed(thisJoinPoint.getArgs());
        }
    }

    private List<LogEntry> writePending(ConsistencyLevel consistencyLevel, List<IMutation> mutations) throws Throwable {
        List<LogEntry> logEntries = new ArrayList<LogEntry>();
        for (IMutation mutation : mutations) {
            if (mutation instanceof RowMutation) {
                RowMutation rowMutation = (RowMutation) mutation;
                logger.debug("Mutation for [" + rowMutation.getTable() + "] with consistencyLevel [" + consistencyLevel
                        + "]");
                if (!rowMutation.getTable().equals(DistributedCommitLog.KEYSPACE)) {
                    logEntries.addAll(DistributedCommitLog.getLog().writePending(consistencyLevel, rowMutation));
                }
            }
        }
        return logEntries;
    }

    private void writeCommitted(List<LogEntry> logEntries) throws Throwable {
        for (LogEntry logEntry : logEntries) {
            logEntry.setStatus(LogEntryStatus.COMMITTED);
            DistributedCommitLog.getLog().writeLogEntry(logEntry);
        }
    }

}
