package org.apache.cassandra.http;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.apache.cassandra.http.ext.PATCH;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/virgil/")
public class CassandraRestService {
	private static Logger logger = LoggerFactory.getLogger(CassandraRestService.class);
	private CassandraStorage cassandraStorage = null;

	public CassandraRestService(CassandraStorage cassandraStorage) {
		this.cassandraStorage = cassandraStorage;
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
		cassandraStorage.setKeyspace("system");
		cassandraStorage.getSlice("system", "Versions", "build", ConsistencyLevel.ONE);
		return "OK";
	}
	
	// ================================================================================================================
	// Key Space Operations
	// ================================================================================================================
	@GET
	@Path("/data/")
	@Produces({ "application/json" })
	public String getKeyspaces(@PathParam("keyspace") String keyspace) throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("Listing keyspaces.");
		return cassandraStorage.getKeyspaces();
	}

	@PUT
	@Path("/data/{keyspace}")
	@Produces({ "application/json" })
	public void createKeyspace(@PathParam("keyspace") String keyspace) throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("Creating keyspace [" + keyspace + "]");
		cassandraStorage.addKeyspace(keyspace);
	}

	@DELETE
	@Path("/data/{keyspace}")
	@Produces({ "application/json" })
	public void dropKeyspace(@PathParam("keyspace") String keyspace) throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("Dropping keyspace [" + keyspace + "]");
		cassandraStorage.dropKeyspace(keyspace);
	}

	// ================================================================================================================
	// Column Family Operations
	// ================================================================================================================
	@GET
	@Path("/data/{keyspace}/{columnFamily}")
	@Produces({ "application/json" })
	public String getColumnFamily(@PathParam("keyspace") String keyspace,
			@PathParam("columnFamily") String columnFamily) throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("Creating column family [" + keyspace + "]:[" + columnFamily + "]");
		cassandraStorage.setKeyspace(keyspace);
		return cassandraStorage.getRows(columnFamily, ConsistencyLevel.ALL);
	}

	@PUT
	@Path("/data/{keyspace}/{columnFamily}")
	@Produces({ "application/json" })
	public void createColumnFamily(@PathParam("keyspace") String keyspace,
			@PathParam("columnFamily") String columnFamily) throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("Creating column family [" + keyspace + "]:[" + columnFamily + "]");
		cassandraStorage.setKeyspace(keyspace);
		cassandraStorage.createColumnFamily(keyspace, columnFamily);
	}

	
	@DELETE
	@Path("/data/{keyspace}/{columnFamily}")
	@Produces({ "application/json" })
	public void deleteColumnFamily(@PathParam("keyspace") String keyspace,
			@PathParam("columnFamily") String columnFamily) throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("Deleteing column family [" + keyspace + "]:[" + columnFamily + "]");
		cassandraStorage.setKeyspace(keyspace);
		cassandraStorage.dropColumnFamily(columnFamily);
	}

	// ================================================================================================================
	// Row Operations
	// ================================================================================================================
	/*
	 * Add's a row, each entry in the JSON map is a column
	 */
	@PATCH
	@Path("/data/{keyspace}/{columnFamily}/{key}")
	@Produces({ "application/json" })
	public void patchRow(@PathParam("keyspace") String keyspace, @PathParam("columnFamily") String columnFamily,
			@PathParam("key") String key, @QueryParam("index") boolean index, String body) throws Exception {
		cassandraStorage.setKeyspace(keyspace);
		JSONObject json = (JSONObject) JSONValue.parse(body);

		if (json == null)
			throw new RuntimeException("Could not parse the JSON object [" + body + "]");

		if (logger.isDebugEnabled())
			logger.debug("Setting column [" + keyspace + "]:[" + columnFamily + "]:[" + key + "] -> [" + json + "]");
		cassandraStorage.setColumn(keyspace, columnFamily, key, json, ConsistencyLevel.ALL, index);
	}

	/*
	 * Add's a row, each entry in the JSON map is a column
	 */
	@PUT
	@Path("/data/{keyspace}/{columnFamily}/{key}")
	@Produces({ "application/json" })
	public void setRow(@PathParam("keyspace") String keyspace, @PathParam("columnFamily") String columnFamily,
			@PathParam("key") String key, @QueryParam("index") boolean index, String body) throws Exception {
		cassandraStorage.setKeyspace(keyspace);
		JSONObject json = (JSONObject) JSONValue.parse(body);

		if (json == null)
			throw new RuntimeException("Could not parse the JSON object [" + body + "]");

		if (logger.isDebugEnabled())
			logger.debug("Setting column [" + keyspace + "]:[" + columnFamily + "]:[" + key + "] -> [" + json + "]");

		long deleteTime = this.deleteRow(keyspace, columnFamily, key, index);

		cassandraStorage.setColumn(keyspace, columnFamily, key, json, ConsistencyLevel.ALL, index, deleteTime + 1);
	}

	/*
	 * Fetches a row
	 */
	@GET
	@Path("/data/{keyspace}/{columnFamily}/{key}")
	@Produces({ "application/json" })
	public String getRow(@PathParam("keyspace") String keyspace, @PathParam("columnFamily") String columnFamily,
			@PathParam("key") String key) throws Exception {
		cassandraStorage.setKeyspace(keyspace);
		if (logger.isDebugEnabled())
			logger.debug("Getting row [" + keyspace + "]:[" + columnFamily + "]:[" + key + "]");

		return cassandraStorage.getSlice(keyspace, columnFamily, key, ConsistencyLevel.ALL);
	}

	/*
	 * Deletes a row
	 */
	@DELETE
	@Path("/data/{keyspace}/{columnFamily}/{key}")
	@Produces({ "application/json" })
	public long deleteRow(@PathParam("keyspace") String keyspace, @PathParam("columnFamily") String columnFamily,
			@PathParam("key") String key, @QueryParam("purgeIndex") boolean purgeIndex) throws Exception {
		cassandraStorage.setKeyspace(keyspace);
		if (logger.isDebugEnabled())
			logger.debug("Deleting row [" + keyspace + "]:[" + columnFamily + "]:[" + key + "]");

		return cassandraStorage.deleteRow(keyspace, columnFamily, key, ConsistencyLevel.ALL, purgeIndex);
	}

	// ================================================================================================================
	// Column Operations
	// ================================================================================================================

	/*
	 * Fetches a column
	 */
	@GET
	@Path("/data/{keyspace}/{columnFamily}/{key}/{columnName}")
	@Produces({ "application/json" })
	public String getColumn(@PathParam("keyspace") String keyspace, @PathParam("columnFamily") String columnFamily,
			@PathParam("key") String key, @PathParam("columnName") String columnName) throws Exception {
		cassandraStorage.setKeyspace(keyspace);
		if (logger.isDebugEnabled())
			logger.debug("Getting column [" + keyspace + "]:[" + columnFamily + "]:[" + key + "]:[" + columnName + "]");

		return cassandraStorage.getColumn(keyspace, columnFamily, key, columnName, ConsistencyLevel.ALL);
	}

	/*
	 * Adds a column
	 */
	@PUT
	@Path("/data/{keyspace}/{columnFamily}/{key}/{columnName}")
	@Produces({ "application/json" })
	public void addColumn(@PathParam("keyspace") String keyspace, @PathParam("columnFamily") String columnFamily,
			@PathParam("key") String key, @PathParam("columnName") String columnName,
			@QueryParam("index") boolean index, String body) throws Exception {
		cassandraStorage.setKeyspace(keyspace);
		if (logger.isDebugEnabled())
			logger.debug("Deleting row [" + keyspace + "]:[" + columnFamily + "]:[" + key + "] => [" + body + "]");
		cassandraStorage.addColumn(keyspace, columnFamily, key, columnName, body, ConsistencyLevel.ALL, index);
	}

	/*
	 * Deletes a column
	 */
	@DELETE
	@Path("/data/{keyspace}/{columnFamily}/{key}/{columnName}")
	@Produces({ "application/json" })
	public void deleteColumn(@PathParam("keyspace") String keyspace, @PathParam("columnFamily") String columnFamily,
			@PathParam("key") String key, @PathParam("columnName") String columnName,
			@QueryParam("purgeIndex") boolean purgeIndex) throws Exception {
		cassandraStorage.setKeyspace(keyspace);
		if (logger.isDebugEnabled())
			logger.debug("Deleting row [" + keyspace + "]:[" + columnFamily + "]:[" + key + "]");
		cassandraStorage.deleteColumn(keyspace, columnFamily, key, columnName, ConsistencyLevel.ALL, purgeIndex);
	}
}
