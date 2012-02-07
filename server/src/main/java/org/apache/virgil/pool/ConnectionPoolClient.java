package org.apache.virgil.pool;

import org.apache.cassandra.thrift.Cassandra;

public class ConnectionPoolClient {
    ThreadLocal<Cassandra.Iface> connection = new ThreadLocal<Cassandra.Iface>() {
        @Override
        protected Cassandra.Iface initialValue() {
           throw new RuntimeException("Using connection w/o attaining from the pool.");
        }
    };

    public void setConnection(Cassandra.Iface connection) {
        this.connection.set(connection);
    }

    public Cassandra.Iface getConnection(String keyspace) throws Exception {
        Cassandra.Iface connection = this.connection.get();
        if (keyspace != null)
            connection.set_keyspace(keyspace);
        return connection;
    }
}
