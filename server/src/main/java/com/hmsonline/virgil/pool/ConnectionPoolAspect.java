package com.hmsonline.virgil.pool;

import org.apache.cassandra.thrift.Cassandra;
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
        Cassandra.Iface connection = ConnectionPool.getConnection(client);
        client.setConnection(connection);
        try {
            while (tries < MAX_CONNECTION_RETRIES) {
                try {
                    return thisJoinPoint.proceed(thisJoinPoint.getArgs());

                    // If we get an exception, let's retry (naively for now)
                    // with a new connection
                    // TODO: Collapse these into one when we move to JDK7 (And remove finally)
                } catch (TException te) {
                    exception = te;
                    Cassandra.Iface newconnection = ConnectionPool.expel(connection);
                    client.setConnection(newconnection);
                } catch (TimedOutException tmoe) {
                    exception = tmoe;
                    Cassandra.Iface newconnection = ConnectionPool.expel(connection);
                    client.setConnection(newconnection);
                } catch (UnavailableException ue) {
                    exception = ue;
                    Cassandra.Iface newconnection = ConnectionPool.expel(connection);
                    client.setConnection(newconnection);
                } finally {
                    tries++;
                }
            }
        } finally {
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
