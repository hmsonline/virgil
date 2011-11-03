

===============================================================================
Getting Started
===============================================================================
1. Compile the project using "mvn install"
   * This will pull dependencies, then build the rest layer.
   * By default, the REST service will use the target directory for
     cassandra data storage.
     
     This can be overridden using the maven property "cassandra.data.dir"
     
     For example to use the cassandra default, specify: "/var/lib/":
     mvn install -Dcassandra.data.dir=/var/lib
     
     This will make cassandra use /var/lib/cassandra as the data directory.

2. Run the daemon using "bin/virgil"
   * This will start a cassandra server using the cassandra.yaml.

3. This uses the log4j.xml file in src/main/resources, which logs http activity to http.log
   * Tail that file to see the http interactions using "tail -f http.log"

4. Use the following curl commands to test it out.


===============================================================================
REST Commands
===============================================================================

Create Keyspace (playground)
----------------------------
   curl -X PUT http://localhost:8080/virgil/data/playground/

Create Column Family (toys)
-------------------------------------------------------------------------------------------------------
   curl -X PUT http://localhost:8080/virgil/data/playground/toys/

Insert Row (rowkey = "swingset", columns [ foo => 1, bar => 22 ])
-------------------------------------------------------------------------------------------------------
   curl -X PUT http://localhost:8080/virgil/data/playground/toys/swingset -d "{\"foo\":\"1\",\"bar\":\"33\"}"

Fetch Row (rowkey = "swingset")
-------------------------------------------------------------------------------------------------------
   curl -X GET http://localhost:8080/virgil/data/playground/toys/swingset/

Insert Column (rowkey = "swingset", columns [ snaf => lisa ])
-------------------------------------------------------------------------------------------------------
   curl -X PUT http://localhost:8080/virgil/data/playground/toys/swingset/snaf -d "lisa"

Delete Column (rowkey = "swingset")
-------------------------------------------------------------------------------------------------------
   curl -X DELETE http://localhost:8080/virgil/data/playground/toys/swingset/snaf

Delete Row
-------------------------------------------------------------------------------------------------------
   curl -X DELETE http://localhost:8080/virgil/data/playground/toys/swingset/

Delete Column Family
-------------------------------------------------------------------------------------------------------
   curl -X DELETE http://localhost:8080/virgil/data/playground/toys/

Delete Keyspace
-------------------------------------------------------------------------------------------------------
   curl -X DELETE http://localhost:8080/virgil/data/playground/


===============================================================================
TODO LIST
===============================================================================
* Security
* XML 
* Exception Handling 

===============================================================================
Development
===============================================================================

To get setup in eclipse, use m2eclipse and import as existing maven project.
Then, set build path to include ../../build/classes/
Also, set the build path to include the cassandra/lib/*.jar.
Also, add build/jars/lib to the classpath
Also, add the conf file to the build path as an class directory (so unit tests can get to the conf)
(Use External Class & External Jars to do this)


