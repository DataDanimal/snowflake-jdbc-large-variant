/*
###################################################################
###################################################################
##        The MIT License - SPDX short identifier: MIT           ##
###################################################################
###################################################################
#
#Copyright 2020 Dan Sandler @ https://github.com/DataDanimal
#
#Permission is hereby granted, free of charge, to any person obtaining
#a copy of this software and associated documentation files (the "Software"),
#to deal in the Software without restriction, including without
#limitation the rights to use, copy, modify, merge, publish, distribute,
#sublicense, and/or sell copies of the Software, and to permit persons
#to whom the Software is furnished to do so, subject to the following
#conditions:
#
#The above copyright notice and this permission notice shall be
#included in all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
#EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
#MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
#IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
#CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
#TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
#SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# Please consider this script an example.
# Do not use this in any production scenario
*/

package com.snowflake.client;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.StringWriter;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.sql.*;
import java.util.Properties;
import java.util.Date;
import java.util.UUID;

public class SnowflakeStageLargeVariant {

    // Instantiate global variables
    static Properties  myprops =  new Properties();
    static String propFile = new String();

    public static void main(String[] args) throws Exception {
        // Get properties file from command line
        propFile = args[0];
        SnowflakeStageLargeVariant sv = new SnowflakeStageLargeVariant();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        System.out.println("Reading property file " + propFile + " at " + ts);
        myprops =  getSnflkProps () ;

        // get connection
        System.out.println("Create JDBC connection");
        Connection connection = getConnection();
        System.out.println("Done creating JDBC connection");

        // create statement
        System.out.println("Create JDBC statement");
        Statement statement = connection.createStatement();
        ts = new Timestamp(System.currentTimeMillis());
        System.out.println("Done creating JDBC statement at " + ts);

        System.out.println("Create/replace sequence");
        statement.executeUpdate("create or replace sequence  large_insert_seq");
        System.out.println("Complete sequence creation/replacement");

        System.out.println("Create/replace table");
        statement.executeUpdate("create or replace table  large_insert (rec_seq integer not null default large_insert_seq.nextval, json_msg variant not null, created_by varchar(100) not null default current_user, created_timestamp  timestamp_ltz not null default current_timestamp)");
        System.out.println("Complete table creation/replacement");
        statement.close();

        ts = new Timestamp(System.currentTimeMillis());
        System.out.println("Preparing statement at " + ts);
        PreparedStatement pstmt = connection.prepareStatement("insert into  large_insert (json_msg) select  column1::variant from values (?)");

        System.out.println("Adding batch inserts");
        int insertCnt = 0;
        int totalInserts = Integer.parseInt(myprops.getProperty("random_objects"));
        int batchSize = Integer.parseInt(myprops.getProperty("batch_size"));

        // Loop through the specified number of inserts, getting a random JSON object of ~16MB
        while (insertCnt <  totalInserts) {
            String parmRandomJson = sv.getRandomJson();
            pstmt.setString(1, parmRandomJson);
            pstmt.addBatch();
            insertCnt++;
            if (insertCnt  %  batchSize == 0) {
                int[] count_interval = pstmt.executeBatch(); // After execution, count[0]=1, count[1]=1
                connection.commit();
                pstmt.close();
                ts = new Timestamp(System.currentTimeMillis());
                System.out.println("Batch size limit of " + batchSize + " reached after insert " + insertCnt + " at " + ts);
                pstmt = connection.prepareStatement("insert into  large_insert (json_msg) select  column1::variant from values (?)");
            }
        }
        System.out.println("Added " + insertCnt + " inserts");

        ts = new Timestamp(System.currentTimeMillis());
        System.out.println("Executing batch inserts at " + ts);
        int[] count_final = pstmt.executeBatch(); // After execution, count[0]=1, count[1]=1
        connection.commit();

        ts = new Timestamp(System.currentTimeMillis());
        System.out.println("Completed " + insertCnt + " inserts at " + ts);
        pstmt.close();
    }

    // This method retrieves the properties from the properties file specified at run time
    private static Properties getSnflkProps () {
        Properties properties = new Properties();
        Properties snflkProps = new Properties();
        try (InputStream input = new FileInputStream(propFile)) {
            // build connection properties
            // load a properties file
            properties.load(input);
            String snflkUser = properties.getProperty("SNFLK_USER");
            System.out.println("Getting user " + snflkUser);
            snflkProps.put("user", snflkUser);
            String snflkPwd = properties.getProperty("SNFLK_PWD");
            System.out.println("Getting password ******** " );
            snflkProps.put("password", snflkPwd);
            String snflkWh = properties.getProperty("SNFLK_WH");
            System.out.println("Getting warehouse " + snflkWh);
            snflkProps.put("warehouse", snflkWh);
            String snflkDb = properties.getProperty("SNFLK_DB");
            System.out.println("Getting database " + snflkDb);
            snflkProps.put("db", snflkDb);
            String snflkSch = properties.getProperty("SNFLK_SCHEMA");
            System.out.println("Getting schema " + snflkSch);
            snflkProps.put("schema", snflkSch);
            String snflkRole = properties.getProperty("SNFLK_ROLE");
            System.out.println("Getting role " + snflkRole);
            snflkProps.put("role", snflkRole);
            String snflkAcct = properties.getProperty("SNFLK_ACCT") ;
            System.out.println("Getting Account " + snflkAcct);
            snflkProps.put("account", snflkAcct);
            String snflkAutoCommit = properties.getProperty("SNFLK_AUTO_COMMIT") ;
            System.out.println("Getting Auto commit " + snflkAutoCommit);
            snflkProps.put("auto_commit", snflkAutoCommit);
            String snflkTracing = properties.getProperty("SNFLK_TRACING") ;
            System.out.println("Getting Trace level " + snflkTracing);
            if(snflkTracing != null)  snflkProps.put ("tracing", snflkTracing);
            String debugMode = properties.getProperty("DEBUG_MODE") ;
            System.out.println("Getting Debug Mode " + debugMode);
            if(debugMode != null)  snflkProps.put("debug", debugMode);
            String snflkNumRandomObjects = properties.getProperty("NUM_RANDOM_OBJECTS") ;
            System.out.println("Getting Number of random objects that will be inserted " + snflkNumRandomObjects);
            snflkProps.put("random_objects", snflkNumRandomObjects);
            String snflkBatchSize = properties.getProperty("BATCH_SIZE") ;
            System.out.println("Batch size for batch inserts " + snflkBatchSize);
            snflkProps.put("batch_size", snflkBatchSize);
            String snflkJsonObjSize = properties.getProperty("JSON_OBJ_SIZE") ;
            System.out.println("JSON Object size for batch inserts " + snflkJsonObjSize);
            snflkProps.put("json_obj_size", snflkJsonObjSize);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return snflkProps;
    }

    // This method establishes the Snowflake connection based on the connection properties in config.properties
    private static Connection getConnection()
            throws SQLException {

        String connectStr = new String();
        try {
            Class.forName("com.snowflake.client.jdbc.SnowflakeDriver");
        } catch (ClassNotFoundException ex) {
            System.err.println("Driver not found");
        }
        connectStr = "jdbc:snowflake://" + myprops.getProperty("account") + ".snowflakecomputing.com";
        System.out.println("Getting Connection string " + connectStr);
        if (Boolean.parseBoolean(myprops.getProperty("debug"))) {
            System.out.println("Getting Connection properties " + myprops);
        }
        Connection myConn = DriverManager.getConnection(connectStr, myprops);
        myConn.setAutoCommit(Boolean.parseBoolean(myprops.getProperty("auto_commit")));
        return myConn;
    }

    // This method generates a random JSON string of the size specified in the config file
    private String getRandomJson() {
        int jsonObjectSize = Integer.parseInt(myprops.getProperty("json_obj_size"));
        StringBuilder stringB = new StringBuilder(jsonObjectSize);
        while (stringB.length() < jsonObjectSize) {
            String paddingString = UUID.randomUUID().toString();
            stringB.append(paddingString);
        }

        JsonObjectBuilder jsonBuilder = Json.createObjectBuilder();
        jsonBuilder.add("sproc_json", stringB.toString());
        JsonObject jsonObj = jsonBuilder.build();
        StringWriter strWtr = new StringWriter();
        JsonWriter jsonWtr = Json.createWriter(strWtr);
        jsonWtr.writeObject(jsonObj);
        jsonWtr.close();
        return strWtr.toString();
    }
}

