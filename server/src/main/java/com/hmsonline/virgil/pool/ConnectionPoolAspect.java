package com.hmsonline.virgil.pool;

import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
public class ConnectionPoolAspect {
    private static Logger logger = LoggerFactory.getLogger(ConnectionPoolAspect.class);
    private static final int MAX_CONNECTION_RETRIES = 2;

    public Object delegate(ProceedingJoinPoint thisJoinPoint) throws Throwable {
        int tries = 0;
        Exception exception = null;
        ConnectionPoolClient client = (ConnectionPoolClient) thisJoinPoint.getTarget();
        VirgilConnection connection = ConnectionPool.getConnection(client);
        try {
//            connection.open();
            client.setConnection(connection.getThriftConnection());
            while (tries < MAX_CONNECTION_RETRIES) {
                try {
                    return thisJoinPoint.proceed(thisJoinPoint.getArgs());
                    // TODO: Collapse into one w/ JDK7 (and remove finally)
                } catch (TException te) {
                    exception = te;
                    connection = ConnectionPool.expel(connection, exception);
                    client.setConnection(connection.getThriftConnection());
                } catch (TimedOutException tmoe) {
                    exception = tmoe;
                    connection = ConnectionPool.expel(connection, exception);
                    client.setConnection(connection.getThriftConnection());
                } catch (UnavailableException ue) {
                    exception = ue;
                    connection = ConnectionPool.expel(connection, exception);
                    client.setConnection(connection.getThriftConnection());
                } finally {
                    tries++;
                }
            }
        } finally {
//            connection.close();
            ConnectionPool.release(client, connection);
        }
        throw exception;
    }

    @Pointcut("execution(@PooledConnection * com.hmsonline.virgil..*.*(..))")
    public void methodAnnotatedWithPooledConnection() {
    }

    @Around("methodAnnotatedWithPooledConnection()")
    public Object handleStorage(ProceedingJoinPoint thisJoinPoint) throws Throwable {
        logger.debug("AOP:STORAGE connection for [" + thisJoinPoint.getSignature() + "]");
        return delegate(thisJoinPoint);
    }
}
