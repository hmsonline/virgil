package org.apache.virgil.triggers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.virgil.CassandraStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerStore {
    public static final String KEYSPACE = "cirrus";
    public static final String COLUMN_FAMILY = "Trigger";
    public static final String ENABLED = "enabled";
    private static boolean initialized = false;
    private static Logger logger = LoggerFactory.getLogger(TriggerStore.class);

    public static void create() {
        if (!initialized) {
            try {
                List<CfDef> cfDefList = new ArrayList<CfDef>();
                KsDef ksDef = new KsDef(KEYSPACE, "org.apache.cassandra.locator.SimpleStrategy", cfDefList);
                ksDef.putToStrategy_options("replication_factor", "1");
                CassandraStorage.getCassandra(null).system_add_keyspace(ksDef);
            } catch (Exception e) {
                logger.debug("Did not create System.CommitLog. (probably already there)");
            }
            try {
                CfDef columnFamily = new CfDef(KEYSPACE, COLUMN_FAMILY);
                columnFamily.setKey_validation_class("UTF8Type");
                columnFamily.setComparator_type("UTF8Type");
                columnFamily.setDefault_validation_class("UTF8Type");
                CassandraStorage.getCassandra(KEYSPACE).system_add_column_family(columnFamily);
                initialized = true;
            } catch (Exception e) {
                logger.debug("Did not create System.CommitLog. (probably already there)");
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static Trigger getTrigger(String triggerClass) throws Exception{
        Class<Trigger> clazz = (Class<Trigger>) Class.forName(triggerClass);
        return clazz.newInstance();        
    }
    
    
    public static Map<String, List<Trigger>> getTriggers() throws Exception{
        // TODO: Cache this.       
        Map<String,List<Trigger>> triggerMap = new HashMap<String,List<Trigger>>();
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange(ByteBufferUtil.bytes(""), ByteBufferUtil.bytes(""), false, 10);
        predicate.setSlice_range(range);

        KeyRange keyRange = new KeyRange(1000);
        keyRange.setStart_key(ByteBufferUtil.bytes(""));
        keyRange.setEnd_key(ByteBufferUtil.EMPTY_BYTE_BUFFER);
        ColumnParent parent = new ColumnParent(COLUMN_FAMILY);
        List<KeySlice> rows = CassandraStorage.getCassandra(KEYSPACE).get_range_slices(parent, predicate, keyRange, ConsistencyLevel.ALL);
        for (KeySlice slice : rows){
            String columnFamily = ByteBufferUtil.string(slice.key);
            List<Trigger> triggers = new ArrayList<Trigger>();
            for (ColumnOrSuperColumn column : slice.columns){
                String className = ByteBufferUtil.string(column.column.name);
                String enabled = ByteBufferUtil.string(column.column.value);                
                if (enabled.equals(ENABLED)){
                    triggers.add(getTrigger(className));
                }
            }
            triggerMap.put(columnFamily, triggers);
        }        
        return triggerMap;
    }
}
