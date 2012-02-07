package org.apache.virgil.triggers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationStore extends CassandraStore {
    public static final String KEYSPACE = "virgil";
    public static final String COLUMN_FAMILY = "Configuration";
    private static Logger logger = LoggerFactory.getLogger(ConfigurationStore.class);
    private static ConfigurationStore instance = null;
    private long lastFetchTime = -1;
    private static final int REFRESH_INTERVAL = 1000 * 30; // 30 seconds
    private Map<String, Map<String, String>> cache = null;

    public ConfigurationStore(String keyspace, String columnFamily) throws Exception {
        super(keyspace, columnFamily);
        logger.debug("Instantiated configuration store.");
    }

    public static synchronized ConfigurationStore getStore() throws Exception {
        if (instance == null)
            instance = new ConfigurationStore(KEYSPACE, COLUMN_FAMILY);
        return instance;
    }

    public boolean isCommitLogEnabled() throws Throwable {
        Map<String, String> commitLogProperties = this.getConfiguration().get("CommitLog");
        if (commitLogProperties != null) {
            String enabled = commitLogProperties.get("enabled");
            return (enabled != null && enabled.equals("true"));
        }
        return false;
    }

    public Map<String, Map<String, String>> getConfiguration() throws Throwable {
        long currentTime = System.currentTimeMillis();
        long timeSinceRefresh = currentTime - this.lastFetchTime;
        if (timeSinceRefresh > REFRESH_INTERVAL) {
            logger.debug("Refreshing virgil run-time configuration.");
            SlicePredicate predicate = new SlicePredicate();
            SliceRange range = new SliceRange(ByteBufferUtil.bytes(""), ByteBufferUtil.bytes(""), false, 10);
            predicate.setSlice_range(range);

            Map<String, Map<String, String>> configuration = new HashMap<String, Map<String, String>>();
            KeyRange keyRange = new KeyRange(1000);
            keyRange.setStart_key(ByteBufferUtil.bytes(""));
            keyRange.setEnd_key(ByteBufferUtil.EMPTY_BYTE_BUFFER);
            ColumnParent parent = new ColumnParent(COLUMN_FAMILY);
            List<KeySlice> rows = getConnection(KEYSPACE).get_range_slices(parent, predicate, keyRange,
                    ConsistencyLevel.ALL);
            for (KeySlice slice : rows) {
                String component = ByteBufferUtil.string(slice.key);
                Map<String, String> properties = new HashMap<String, String>();
                for (ColumnOrSuperColumn column : slice.columns) {
                    String key = ByteBufferUtil.string(column.column.name);
                    String value = ByteBufferUtil.string(column.column.value);
                    properties.put(key, value);
                }
                configuration.put(component, properties);
            }
            
            this.lastFetchTime = currentTime;
            cache = configuration;
        }
        return cache;
    }
}
