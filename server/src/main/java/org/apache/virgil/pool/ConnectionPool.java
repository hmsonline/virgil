package org.apache.virgil.pool;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.virgil.config.VirgilConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionPool {
    private static Logger logger = LoggerFactory.getLogger(ConnectionPool.class);
    // TODO: May want this to match the HTTP thread configuration.
    private static final int MAX_POOL_SIZE = 100;
    private static final int MAX_TRIES_FOR_CONNECTION = 3;
    private static final int CONNECTION_WAIT_TIME = 500;
    private static Object LOCK = new Object();

    private static LinkedList<Cassandra.Iface> free = new LinkedList<Cassandra.Iface>();
    private static Map<Cassandra.Iface, TTransport> socketMap = new HashMap<Cassandra.Iface, TTransport>();
    private static Cassandra.Iface embeddedServer = null;

    public static void initializePool() throws Exception {
        logger.debug("Creating connection pool, initializing [" + MAX_POOL_SIZE + "] connections.");
        // Don't need pooling if we are embedded
        if (VirgilConfiguration.isEmbedded()) {
            embeddedServer = new CassandraServer();
        } else {
            for (int i = 0; i < MAX_POOL_SIZE; i++) {
                free.add(createConnection());
            }
        }
    }

    public static Cassandra.Iface createConnection() throws Exception {
        if (VirgilConfiguration.isEmbedded()) {
            return new CassandraServer();
        } else {
            TTransport transport = new TFramedTransport(new TSocket(VirgilConfiguration.getHost(),
                    VirgilConfiguration.getPort()));
            TProtocol proto = new TBinaryProtocol(transport);
            transport.open();
            Cassandra.Iface connection = new Cassandra.Client(proto);
            socketMap.put(connection, transport);
            return connection;
        }
    }

    public static Cassandra.Iface getConnection(Object requestor) throws EmptyConnectionPoolException {
        // Short circuit if embedded.
        if (VirgilConfiguration.isEmbedded()) {
            return embeddedServer;
        }

        for (int x = 0; x < MAX_TRIES_FOR_CONNECTION; x++) {
            try {
                Cassandra.Iface connection = free.pop();
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

    public static void release(Object requestor, Cassandra.Iface connection) throws Exception {
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
}
