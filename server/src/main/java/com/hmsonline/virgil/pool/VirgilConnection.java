package com.hmsonline.virgil.pool;

import java.util.Date;
import java.util.UUID;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.virgil.config.VirgilConfiguration;

public class VirgilConnection {
    private static Logger logger = LoggerFactory.getLogger(VirgilConnection.class);

    private TTransport transport = null;
    private TSocket socket = null;
    private Cassandra.Iface connection = null;
    private UUID id = null;
    private long createTime;

    public VirgilConnection(boolean embedded) throws TTransportException {
        id = UUID.randomUUID();
        createTime = System.currentTimeMillis();
        logger.debug("Created connection [" + this.id + "] at [" + new Date(this.createTime) + "]");
        if (!embedded) {
            this.socket = new TSocket(VirgilConfiguration.getHost(), VirgilConfiguration.getPort());
            this.transport = new TFramedTransport(socket);
            TProtocol proto = new TBinaryProtocol(transport);
            this.connection = new Cassandra.Client(proto);
            transport.open();
        } else {
            this.connection = new CassandraServer();
        }
    }

    public Cassandra.Iface getThriftConnection() {
        return connection;
    }

    public void open() throws TTransportException {
        logger.debug("Opening connection [" + this.id + "] created at [" + new Date(this.createTime) + "]");
        transport.open();
    }

    public void close() {
        logger.debug("Closing connection [" + this.id + "] created at [" + new Date(this.createTime) + "]");
        if (transport != null && transport.isOpen()) {
            try {
                transport.flush();
            } catch (Exception e) {
                logger.error("Could not flush thrift transport." + toString(), e.getMessage());
            } finally {
                try {
                    transport.close();
                    socket.close();
                } catch (Exception e) {
                    logger.error("Could not close thrift transport (okay if the server has gone away).", e);
                }
            }
        }
    }

    public UUID getId() {
        return id;
    }
}
