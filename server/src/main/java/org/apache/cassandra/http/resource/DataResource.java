package org.apache.cassandra.http.resource;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.apache.cassandra.http.CassandraStorage;
import org.apache.cassandra.http.config.VirgilConfiguration;
import org.apache.cassandra.http.ext.PATCH;
import org.apache.cassandra.http.mapreduce.JobSpawner;
import org.apache.cassandra.http.ws.VirgilService;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/data/")
public class DataResource {
	private static Logger logger = LoggerFactory.getLogger(DataResource.class);
	private VirgilService virgilService = null;
	private VirgilConfiguration config = null;
	public static final String CONSISTENCY_LEVEL_HEADER = "X-Consistency-Level";

	public DataResource(VirgilService virgilService) {
		this.virgilService = virgilService;
		this.config = virgilService.getConfig();
	}

	// ================================================================================================================
	// Key Space Operations
	// ================================================================================================================
	@GET
	@Path("/ping")
	@Produces({ "text/plain" })
	public String healthCheck() throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("Ping.");
		getCassandraStorage().getSlice("system", "Versions", "build", ConsistencyLevel.ONE);
		return "OK\n";
	}

	// ================================================================================================================
	// Key Space Operations
	// ================================================================================================================
	@GET
	@Path("/")
	@Produces({ "application/json" })
	public JSONArray getKeyspaces() throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("Listing keyspaces.");
		return getCassandraStorage().getKeyspaces();
	}

	@PUT
	@Path("/{keyspace}")
	@Produces({ "application/json" })
	public void createKeyspace(@PathParam("keyspace") String keyspace) throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("Creating keyspace [" + keyspace + "]");
		getCassandraStorage().addKeyspace(keyspace);
	}

	@DELETE
	@Path("/{keyspace}")
	@Produces({ "application/json" })
	public void dropKeyspace(@PathParam("keyspace") String keyspace) throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("Dropping keyspace [" + keyspace + "]");
		getCassandraStorage().dropKeyspace(keyspace);
	}

	// ================================================================================================================
	// Column Family Operations
	// ================================================================================================================
	@GET
	@Path("/{keyspace}/{columnFamily}")
	@Produces({ "application/json" })
	public JSONArray getColumnFamily(@PathParam("keyspace") String keyspace,
			@PathParam("columnFamily") String columnFamily,
			@HeaderParam(CONSISTENCY_LEVEL_HEADER) String consistencyLevel) throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("Listing column family [" + keyspace + "]:[" + columnFamily + "]");
		logger.debug("STORAGE [" + getCassandraStorage() + "]");
		logger.debug("CONFG [" +config + "]");
		
		return getCassandraStorage().getRows(keyspace, columnFamily, config.getConsistencyLevel(consistencyLevel));
	}

	/*
	 * Adds or updates rows in the column family.
	 */
	@PATCH
	@Path("/{keyspace}/{columnFamily}")
	@Produces({ "application/json" })
	public void patchColumnFamily(@PathParam("keyspace") String keyspace,
			@PathParam("columnFamily") String columnFamily, @QueryParam("index") boolean index, String body,
			@HeaderParam(CONSISTENCY_LEVEL_HEADER) String consistencyLevel) throws Exception {
		JSONObject json = (JSONObject) JSONValue.parse(body);

		if (json == null)
			throw new RuntimeException("Could not parse the JSON object [" + body + "]");

		if (logger.isDebugEnabled())
			logger.debug("Patching column [" + keyspace + "]:[" + columnFamily + "] -> [" + json + "]");

		// TODO: Should probably make this "more atomic" than it is batching
		// everything into a single set of mutations.
		for (Object rowKey : json.keySet()) {
			JSONObject rowJson = (JSONObject) json.get(rowKey);
			String key = (String) rowKey;
			getCassandraStorage().setColumn(keyspace, columnFamily, key, rowJson,
					config.getConsistencyLevel(consistencyLevel), index);
		}
	}

	@PUT
	@Path("/{keyspace}/{columnFamily}")
	@Produces({ "application/json" })
	public void createColumnFamily(@PathParam("keyspace") String keyspace,
			@PathParam("columnFamily") String columnFamily) throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("Creating column family [" + keyspace + "]:[" + columnFamily + "]");
		getCassandraStorage().createColumnFamily(keyspace, columnFamily);
	}

	@DELETE
	@Path("/{keyspace}/{columnFamily}")
	@Produces({ "application/json" })
	public void deleteColumnFamily(@PathParam("keyspace") String keyspace,
			@PathParam("columnFamily") String columnFamily) throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("Deleteing column family [" + keyspace + "]:[" + columnFamily + "]");
		getCassandraStorage().dropColumnFamily(keyspace, columnFamily);
	}

	// ================================================================================================================
	// Row Operations
	// ================================================================================================================
	/*
	 * Adds or appends to a row, each entry in the JSON map is a column
	 */
	@PATCH
	@Path("/{keyspace}/{columnFamily}/{key}")
	@Produces({ "application/json" })
	public void patchRow(@PathParam("keyspace") String keyspace, @PathParam("columnFamily") String columnFamily,
			@PathParam("key") String key, @QueryParam("index") boolean index, String body,
			@HeaderParam(CONSISTENCY_LEVEL_HEADER) String consistencyLevel) throws Exception {
		JSONObject json = (JSONObject) JSONValue.parse(body);

		if (json == null)
			throw new RuntimeException("Could not parse the JSON object [" + body + "]");

		if (logger.isDebugEnabled())
			logger.debug("Patching column [" + keyspace + "]:[" + columnFamily + "]:[" + key + "] -> [" + json + "]");
		getCassandraStorage().setColumn(keyspace, columnFamily, key, json,
				config.getConsistencyLevel(consistencyLevel), index);
	}

	/*
	 * Add or replaces a row, each entry in the JSON map is a column
	 */
	@PUT
	@Path("/{keyspace}/{columnFamily}/{key}")
	@Produces({ "application/json" })
	public void setRow(@PathParam("keyspace") String keyspace, @PathParam("columnFamily") String columnFamily,
			@PathParam("key") String key, @QueryParam("index") boolean index, String body,
			@HeaderParam(CONSISTENCY_LEVEL_HEADER) String consistencyLevel) throws Exception {
		JSONObject json = (JSONObject) JSONValue.parse(body);

		if (json == null)
			throw new RuntimeException("Could not parse the JSON object [" + body + "]");

		if (logger.isDebugEnabled())
			logger.debug("Adding or updating row [" + keyspace + "]:[" + columnFamily + "]:[" + key + "] -> [" + json
					+ "]");

		long deleteTime = this.deleteRow(keyspace, columnFamily, key, index, consistencyLevel);

		getCassandraStorage().setColumn(keyspace, columnFamily, key, json,
				config.getConsistencyLevel(consistencyLevel), index, deleteTime + 1);
	}

	/*
	 * Fetches a row
	 */
	@GET
	@Path("/{keyspace}/{columnFamily}/{key}")
	@Produces({ "application/json" })
	public JSONObject getRow(@PathParam("keyspace") String keyspace, @PathParam("columnFamily") String columnFamily,
			@PathParam("key") String key, @HeaderParam(CONSISTENCY_LEVEL_HEADER) String consistencyLevel)
			throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("Getting row [" + keyspace + "]:[" + columnFamily + "]:[" + key + "]");

		return getCassandraStorage().getSlice(keyspace, columnFamily, key,
				config.getConsistencyLevel(consistencyLevel));
	}

	/*
	 * Deletes a row
	 */
	@DELETE
	@Path("/{keyspace}/{columnFamily}/{key}")
	@Produces({ "application/json" })
	public long deleteRow(@PathParam("keyspace") String keyspace, @PathParam("columnFamily") String columnFamily,
			@PathParam("key") String key, @QueryParam("purgeIndex") boolean purgeIndex,
			@HeaderParam(CONSISTENCY_LEVEL_HEADER) String consistencyLevel) throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("Deleting row [" + keyspace + "]:[" + columnFamily + "]:[" + key + "]");

		return getCassandraStorage().deleteRow(keyspace, columnFamily, key,
				config.getConsistencyLevel(consistencyLevel), purgeIndex);
	}

	// ================================================================================================================
	// Column Operations
	// ================================================================================================================

	/*
	 * Fetches a column
	 */
	@GET
	@Path("/{keyspace}/{columnFamily}/{key}/{columnName}")
	public String getColumn(@PathParam("keyspace") String keyspace, @PathParam("columnFamily") String columnFamily,
			@PathParam("key") String key, @PathParam("columnName") String columnName,
			@HeaderParam(CONSISTENCY_LEVEL_HEADER) String consistencyLevel) throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("Getting column [" + keyspace + "]:[" + columnFamily + "]:[" + key + "]:[" + columnName + "]");

		return getCassandraStorage().getColumn(keyspace, columnFamily, key, columnName,
				config.getConsistencyLevel(consistencyLevel));
	}

	/*
	 * Adds a column
	 */
	@PUT
	@Path("/{keyspace}/{columnFamily}/{key}/{columnName}")
	@Produces({ "application/json" })
	public void addColumn(@PathParam("keyspace") String keyspace, @PathParam("columnFamily") String columnFamily,
			@PathParam("key") String key, @PathParam("columnName") String columnName,
			@QueryParam("index") boolean index, String body,
			@HeaderParam(CONSISTENCY_LEVEL_HEADER) String consistencyLevel) throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("Deleting row [" + keyspace + "]:[" + columnFamily + "]:[" + key + "] => [" + body + "]");
		getCassandraStorage().addColumn(keyspace, columnFamily, key, columnName, body,
				config.getConsistencyLevel(consistencyLevel), index);
	}

	/*
	 * Deletes a column
	 */
	@DELETE
	@Path("/{keyspace}/{columnFamily}/{key}/{columnName}")
	@Produces({ "application/json" })
	public void deleteColumn(@PathParam("keyspace") String keyspace, @PathParam("columnFamily") String columnFamily,
			@PathParam("key") String key, @PathParam("columnName") String columnName,
			@QueryParam("purgeIndex") boolean purgeIndex, @HeaderParam(CONSISTENCY_LEVEL_HEADER) String consistencyLevel)
			throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("Deleting row [" + keyspace + "]:[" + columnFamily + "]:[" + key + "]");
		getCassandraStorage().deleteColumn(keyspace, columnFamily, key, columnName,
				config.getConsistencyLevel(consistencyLevel), purgeIndex);
	}

	// ================================================================================================================
	// Map Reduce
	// ================================================================================================================

	@POST
	@Path("/job")
	@Produces({ "text/plain" })
	public void mapReduce(
            @QueryParam("params") String params,
	        @QueryParam("jobName") String jobName, @QueryParam("inputKeyspace") String inputKeyspace,
			@QueryParam("inputColumnFamily") String inputColumnFamily,
			@QueryParam("outputKeyspace") String outputKeyspace,
			@QueryParam("outputColumnFamily") String outputColumnFamily, @QueryParam("remote") boolean remote,
			String source) throws Exception {
		if (inputKeyspace == null)
			throw new RuntimeException("Must supply inputKeyspace.");
		if (inputColumnFamily == null)
			throw new RuntimeException("Must supply inputColumnFamily.");
		if (outputKeyspace == null)
			throw new RuntimeException("Must supply outputKeyspace.");
		if (outputColumnFamily == null)
			throw new RuntimeException("Must supply outputColumnFamily.");

		if (logger.isDebugEnabled()) {
			logger.debug("Launching job [" + jobName + "]");
			logger.debug("  --> Input  : Keyspace [" + inputKeyspace + "], ColumnFamily [" + inputColumnFamily + "]");
			logger.debug("  <-- Output : Keyspace [" + outputKeyspace + "], ColumnFamily [" + outputColumnFamily + "]");
		}

		if (remote) {
			
		} else {
			JobSpawner.spawnLocal(jobName, this.getCassandraStorage().getHost(), 
					this.getCassandraStorage().getPort(), inputKeyspace,
					inputColumnFamily, outputKeyspace, outputColumnFamily, source, params);
		}
	}
	
	public CassandraStorage getCassandraStorage(){
		return this.virgilService.getStorage();
	}
}
