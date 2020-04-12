# snowflake-jdbc-large-variant
Sample app to demonstrate loading large variants using Snowflake JDBC driver and batch inserts

Also, this is for demo purposes only, suited for Sandboxes and POCs (mileage may vary)

# References
If unfamiliar with the Snowflake JDBC driver, please reference:
https://docs.snowflake.com/en/user-guide/jdbc-configure.html

Also please reference the page on Batch Updates:
https://docs.snowflake.com/en/user-guide/jdbc-using.html#batch-updates 

# Prerequisites
1. Working knowledge of JAVA, SQL, and Snowflake
2. JVM installed
3. IDE installed
4. Snowflake JDBC driver (ideally recommended version or higher)
      https://docs.snowflake.com/en/release-notes/requirements.html#recommended-versions
   Available for download here
      https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/
5. Javax JSON library.  Available here
      https://mvnrepository.com/artifact/org.glassfish/javax.json/1.1.4
6. Libraries above available on the host machine
7. Configure `config.properties.template` with Snowflake connection properties
      
# Steps to Use
1. Git the files to a local directory
2. Place the libraries listed in Prerequisites into an accessible folder (preferably in your existing `CLASSPATH`) 
3. Execute the following command to create a SnowflakeStageLargeVariant.class file.
 
       javac -cp <LIBRARY_PATH>/snowflake-jdbc-3.12.2.jar:<LIBRARY_PATH>/javax.json-1.1.4.jar:. -d . SnowflakeStageLargeVariant.java   
 
4. Edit the config.properties.template for your Snowflake account connection details, save it a convenient and safe directory
5. Execute the following command to initialize the connections, prepare the batch inserts, and execute the statements:

        java -cp <LIBRARY_PATH>snowflake-jdbc-3.12.2.jar:<LIBRARY_PATH>/javax.json-1.1.4.jar:. com.snowflake.client.SnowflakeStageLargeVariant <LIBRARY_PATH>/config.properties

6. Login to Snowflake account, query table `<SNFLK_DB>.<SNFLK_SCHEMA>.large_insert`


# Recommendations
1. For heavy data ingest volumes, use conventional `BULK COPY` statements instead of `executeBatch`
2. For continuous data loading, use Snowpipe with `AUTO_INGEST` set to `TRUE`
3. Size the batch and array binding to ensure the `CLIENT_STAGE_ARRAY_BINDING_THRESHOLD` is exceeded.  This ensures a temporary `STAGE` s created with inserts running against the temporary stage 
4. Tune the `CLIENT_STAGE_ARRAY_BINDING_THRESHOLD` as needed.  See https://docs.snowflake.com/en/user-guide/jdbc-using.html#batch-updates
5. Read carefully the notes for the following properties in the config.properties.template
        `NUM_RANDOM_OBJECTS`
        `BATCH_SIZE`
        `JSON_OBJ_SIZE`
        `USE_VARIANT`
        `DROP_OBJECTS`
        `LOG_RESULTS`
        `RESET_LOGS`