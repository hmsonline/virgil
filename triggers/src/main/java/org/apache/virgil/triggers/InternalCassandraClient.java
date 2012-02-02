package org.apache.virgil.triggers;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CassandraServer;

public class InternalCassandraClient {
    Cassandra.Iface getConnection(String keyspace) throws Exception{
        CassandraServer server = new CassandraServer();
        if (keyspace != null){
            server.set_keyspace(keyspace);
        }
        return server;        
    }
}
