package com.hmsonline.virgil.pool;

import java.util.LinkedList;
import java.util.NoSuchElementException;

import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.virgil.config.VirgilConfiguration;

public class ConnectionPool {
    private static Logger logger = LoggerFactory.getLogger(ConnectionPool.class);
    // TODO: May want this to match the HTTP thread configuration.
    private static final int MAX_POOL_SIZE = 100;
    private static final int MAX_TRIES_FOR_CONNECTION = 2;
    private static final int CONNECTION_WAIT_TIME = 500;
    private static Object LOCK = new Object();

    private static LinkedList<VirgilConnection> free = new LinkedList<VirgilConnection>();
    private static VirgilConnection embeddedServer = null;

    public static void initializePool() throws TTransportException {
        logger.debug("Creating connection pool, initializing [" + MAX_POOL_SIZE + "] connections.");
        // Don't need pooling if we are embedded
        if (VirgilConfiguration.isEmbedded()) {
            embeddedServer = new VirgilConnection(VirgilConfiguration.isEmbedded());
        } else {
            for (int i = 0; i < MAX_POOL_SIZE; i++) {
                free.add(createConnection());
            }
        }
    }

    public static VirgilConnection createConnection() throws TTransportException {
        return new VirgilConnection(VirgilConfiguration.isEmbedded());
    }

    public static VirgilConnection getConnection(Object requestor) throws EmptyConnectionPoolException, TTransportException {
        // Short circuit if embedded.
        if (VirgilConfiguration.isEmbedded()) {
            return embeddedServer;
        }

        for (int x = 0; x < MAX_TRIES_FOR_CONNECTION; x++) {
            try {
                VirgilConnection connection = free.pop();
                logger.debug("Releasing connection to [" + requestor.getClass() + "] [" + free.size() + "] remain.");
                return connection;
            } catch (NoSuchElementException nsee) {
                logger.warn("Waiting " + CONNECTION_WAIT_TIME + "ms for cassandra connection, attempt [" + x + "]");
            }
            try {
                synchronized (LOCK) {
                    logger.warn("LOCKING for cassandra connection.");
                    LOCK.wait(CONNECTION_WAIT_TIME);
                }
            } catch (InterruptedException ie) {
                throw new EmptyConnectionPoolException("No cassandra connection, and interupted while waiting.", ie);
            }
        }
        throw new EmptyConnectionPoolException("No cassandra connections left in pool.");
    }

    public static void release(Object requestor, VirgilConnection connection) throws Exception {
        // (TODO: Could make to ConnectionPool implementations based on embedded or not.)
        if (VirgilConfiguration.isEmbedded()) {
            return;
        }

        free.add(connection);
        logger.debug("Returning connection from [" + requestor.getClass() + "] [" + free.size() + "] remain.");
        synchronized (LOCK) {
            LOCK.notify();
        }
    }

    public static VirgilConnection expel(VirgilConnection connection, Exception reason) throws Exception {
        logger.warn("Expelling connection [" + connection.getId() + "] from remain because :", reason.toString());
        connection.close();
        return createConnection();
    }

}
