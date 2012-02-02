package org.apache.virgil.triggers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedCommitLog extends InternalCassandraClient  {
    private static Logger logger = LoggerFactory.getLogger(DistributedCommitLog.class);

    public static final String KEYSPACE = "cirrus";
    public static final String COLUMN_FAMILY = "CommitLog";
    private static boolean initialized = false;
    private static DistributedCommitLog instance = null;

    public static synchronized DistributedCommitLog getLog() {
        if (instance == null)
            instance = new DistributedCommitLog();
        return instance;
    }

    public synchronized void create() throws Exception {
        if (!initialized) {
            try {
                List<CfDef> cfDefList = new ArrayList<CfDef>();
                KsDef ksDef = new KsDef(KEYSPACE, "org.apache.cassandra.locator.SimpleStrategy", cfDefList);
                ksDef.putToStrategy_options("replication_factor", "1");
                getConnection(null).system_add_keyspace(ksDef);
            } catch (Exception e) {
                logger.debug("Did not create System.CommitLog. (probably already there)");
            } 
            try {
                CfDef columnFamily = new CfDef(KEYSPACE, COLUMN_FAMILY);
                columnFamily.setKey_validation_class("TimeUUIDType");
                getConnection(KEYSPACE).system_add_column_family(columnFamily);
                initialized = true;
            } catch (Exception e) {
                logger.debug("Did not create System.CommitLog. (probably already there)");
            }
        }
    }

    public void writeMutation(ConsistencyLevel consistencyLevel, RowMutation rowMutation) throws Exception {
        List<Mutation> slice = new ArrayList<Mutation>();
        Column c = new Column();
        c.setName(ByteBufferUtil.bytes("mutation"));
        c.setValue(rowMutation.getSerializedBuffer(MessagingService.version_));
        c.setTimestamp(System.currentTimeMillis() * 1000);

        Mutation m = new Mutation();
        ColumnOrSuperColumn cc = new ColumnOrSuperColumn();
        cc.setColumn(c);
        m.setColumn_or_supercolumn(cc);
        slice.add(m);
        Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
        Map<String, List<Mutation>> cfMutations = new HashMap<String, List<Mutation>>();
        cfMutations.put(COLUMN_FAMILY, slice);
        byte[] rowKey = UUIDGen.getTimeUUIDBytes();
        mutationMap.put(ByteBuffer.wrap(rowKey), cfMutations);
        // TODO: Add Exception Handling.
        getConnection(KEYSPACE).batch_mutate(mutationMap, consistencyLevel);
    }
}
