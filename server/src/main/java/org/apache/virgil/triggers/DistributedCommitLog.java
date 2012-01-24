package org.apache.virgil.triggers;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.virgil.CassandraStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedCommitLog {
    public static final String KEYSPACE = "cirrus";
    public static final String COLUMN_FAMILY = "CommitLog";
    private static boolean initialized = false;
    private static Logger logger = LoggerFactory.getLogger(DistributedCommitLog.class);

    public static void create() {
        if (!initialized) {
            try {
                List<CfDef> cfDefList = new ArrayList<CfDef>();
                KsDef ksDef = new KsDef(KEYSPACE, "org.apache.cassandra.locator.SimpleStrategy", cfDefList);
                ksDef.putToStrategy_options("replication_factor", "1");
                CassandraStorage.getCassandra(null).system_add_keyspace(ksDef);
            } catch (Exception e) {
                logger.warn("Did not create System.CommitLog. (probably already there)");
            }

            try {
                CfDef columnFamily = new CfDef(KEYSPACE, COLUMN_FAMILY);
                columnFamily.setKey_validation_class("UTF8Type");
                columnFamily.setComparator_type("UTF8Type");
                columnFamily.setDefault_validation_class("UTF8Type");
                CassandraStorage.getCassandra(KEYSPACE).system_add_column_family(columnFamily);
                initialized = true;
            } catch (Exception e) {
                e.printStackTrace();
                logger.warn("Did not create System.CommitLog. (probably already there)");
            }

        }
    }

    public static void writeMutation(IMutation mutation) {
        DistributedCommitLog.create();
    }
}
