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

import javax.json.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SnowflakeStageLargeVariant {

    // Instantiate global variables
    static Properties myprops = new Properties();
    static String propFile;
    static Boolean useVariant = false;
    static String currentUser;
    static double payLoadSize;
    static JsonObjectBuilder jsonHeaderBuilder = Json.createObjectBuilder();
    static JsonArrayBuilder jsonLogArrayBuilder = Json.createArrayBuilder();
    static Timestamp startTs = new Timestamp(System.currentTimeMillis());

    public static void main(String[] args) throws Exception {
        // Get properties file from command line
        propFile = args[0];
        SnowflakeStageLargeVariant sv = new SnowflakeStageLargeVariant();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        System.out.println("Reading property file " + propFile + " at " + ts);
        myprops = getSnflkProps();

        // get connection
        System.out.println("Create JDBC connection");
        Connection connection = getConnection();
        System.out.println("Done creating JDBC connection");


        // create statement
        System.out.println("Create JDBC statement");
        Statement statement = connection.createStatement();
        ts = new Timestamp(System.currentTimeMillis());
        System.out.println("Done creating JDBC statement at " + ts);

        dropObjects(statement,
                Boolean.parseBoolean(myprops.getProperty("drop_objects")),
                Boolean.parseBoolean(myprops.getProperty("log_results")),
                Boolean.parseBoolean(myprops.getProperty("reset_logs")));

        logResults("begin", connection, 0);

        ts = new Timestamp(System.currentTimeMillis());
        System.out.println("Preparing statement at " + ts);
        String stmtStr = (useVariant) ?
                "insert into  large_insert (json_msg, created_by, created_ts) select  parse_json(column1) from values (?, ?, ?)" :
                "insert into  large_insert (json_msg, created_by, created_ts) values ( ?, ?, ?)";

        // get Nextval in statement, encourages binding for bulk insert
        String seqStmt = "select large_insert_seq.nextval ";
        PreparedStatement seqps = connection.prepareStatement(seqStmt);

        // get current user in statement, encourages binding for bulk insert
        String userStmt = "select current_user ";
        PreparedStatement userps = connection.prepareStatement(userStmt);
        ResultSet userrs = userps.executeQuery();
        userrs.next();
        currentUser = userrs.getString(1);

        PreparedStatement pstmt = connection.prepareStatement(stmtStr);
        System.out.println("Adding batch inserts");
        double insertCnt = 0;
        int totalInserts = Integer.parseInt(myprops.getProperty("total_inserts"));
        int batchSize = Integer.parseInt(myprops.getProperty("batch_size"));

        // Loop through the specified number of inserts, getting a random JSON object of ~16MB
        int lastExecCnt = 0;
        while (insertCnt < totalInserts) {
            ts = new Timestamp(System.currentTimeMillis());
            String parmRandomJson = sv.getRandomJson();
            pstmt.setString(1, parmRandomJson);
            payLoadSize += parmRandomJson.length();
            pstmt.setString(2, currentUser);
            pstmt.setTimestamp(3, ts);
            pstmt.addBatch();
            insertCnt++;
            lastExecCnt++;
            if (insertCnt % batchSize == 0) {
                pstmt.executeBatch(); // After execution, count[0]=1, count[1]=1
                connection.commit();
                pstmt.close();
                ts = new Timestamp(System.currentTimeMillis());
                System.out.println("Batch size limit of " + batchSize + " reached after insert " + insertCnt + " at " + ts);
                pstmt = connection.prepareStatement(stmtStr);
                lastExecCnt = 0;
                logResults("middle", connection, insertCnt);
            }
        }
        // last insert
        ts = new Timestamp(System.currentTimeMillis());
        if (lastExecCnt > 0) {
            System.out.println("Executing final inserts at " + ts);
            pstmt.executeBatch(); // After execution, count[0]=1, count[1]=1
            connection.commit();
            pstmt.close();
            logResults("middle", connection, insertCnt);
        }

        pstmt.close();
        statement.close();

        logResults("end", connection, insertCnt);
    }

    // This method retrieves the properties from the properties file specified at run time
    private static Properties getSnflkProps() {
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
            System.out.println("Getting password ******** ");
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
            String snflkAcct = properties.getProperty("SNFLK_ACCT");
            System.out.println("Getting Account " + snflkAcct);
            snflkProps.put("account", snflkAcct);
            String snflkAutoCommit = properties.getProperty("SNFLK_AUTO_COMMIT");
            System.out.println("Getting Auto commit " + snflkAutoCommit);
            snflkProps.put("auto_commit", snflkAutoCommit);
            String snflkTracing = properties.getProperty("SNFLK_TRACING");
            System.out.println("Getting Trace level " + snflkTracing);
            if (snflkTracing != null) snflkProps.put("tracing", snflkTracing);
            String debugMode = properties.getProperty("DEBUG_MODE");
            System.out.println("Getting Debug Mode " + debugMode);
            if (debugMode != null) snflkProps.put("debug", debugMode);
            String snflkTotalInserts = properties.getProperty("TOTAL_INSERTS");
            System.out.println("Getting Number of random objects that will be inserted " + snflkTotalInserts);
            snflkProps.put("total_inserts", snflkTotalInserts);
            String snflkBatchSize = properties.getProperty("BATCH_SIZE");
            System.out.println("Batch size for batch inserts " + snflkBatchSize);
            snflkProps.put("batch_size", snflkBatchSize);
            String snflkJsonObjSize = properties.getProperty("JSON_OBJ_SIZE");
            System.out.println("JSON Object size for batch inserts " + snflkJsonObjSize);
            snflkProps.put("json_obj_size", snflkJsonObjSize);
            String snflkUseVariant = properties.getProperty("USE_VARIANT");
            useVariant = Boolean.parseBoolean(myprops.getProperty("use_variant"));
            snflkProps.put("use_variant", snflkUseVariant);
            System.out.println("Use Variant, i.e. if false then takes advantage of bulk Array processing " + useVariant);
            String snflkDropObjects = properties.getProperty("DROP_OBJECTS");
            if (snflkDropObjects != null) snflkProps.put("drop_objects", snflkDropObjects);
            System.out.println("Drop objects prior to execution, i.e. if you want to start clean " + snflkDropObjects);
            String snflkLogResults = properties.getProperty("LOG_RESULTS");
            snflkProps.put("log_results", snflkLogResults);
            System.out.println("Log results post-execution, i.e. if you want to analyze cross tests " + snflkLogResults);
            String snflkResetLogs = properties.getProperty("RESET_LOGS");
            snflkProps.put("reset_logs", snflkResetLogs);
            System.out.println("Clear logs prior to execution " + snflkResetLogs);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return snflkProps;
    }

    // This method establishes the Snowflake connection based on the connection properties in config.properties
    private static Connection getConnection()
            throws SQLException {

        String connectStr;
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
        JsonObjectBuilder jsonObjBuilder = Json.createObjectBuilder();
        JsonArrayBuilder jsonArrBuilder = Json.createArrayBuilder();
        String jsonTag = "random_str";
        int lengthCnt = 0;
        while (lengthCnt < jsonObjectSize) {
            String randomString = UUID.randomUUID().toString();
            jsonArrBuilder.add(Json.createObjectBuilder().add(jsonTag, randomString));
            lengthCnt += (randomString.length() + jsonTag.length() + 6); // 6 = beg tag + end tag + beg quote + end quote + colon + comma
        }
        JsonArray jsonArr = jsonArrBuilder.build();
        jsonObjBuilder.add("random_payload", jsonArr);
        JsonObject jsonObj = jsonObjBuilder.build();
        JsonArrayBuilder jsonFinalArrBuilder = Json.createArrayBuilder();
        jsonFinalArrBuilder.add(jsonObj);
        JsonObjectBuilder jsonFinalObjBuilder = Json.createObjectBuilder();
        jsonFinalObjBuilder.add("payload", jsonFinalArrBuilder.build());
        JsonObject jsonFinalObj = jsonFinalObjBuilder.build();
        StringWriter strWtr = new StringWriter();
        JsonWriter jsonWtr = Json.createWriter(strWtr);
        jsonWtr.writeObject(jsonFinalObj);
        jsonWtr.close();
        return strWtr.toString();
    }

    // This drops objects if the DROP_OBJECT property is true
    private static void dropObjects(Statement currStmt, boolean dropObjects, boolean logResults, boolean resetLogs) throws SQLException {
        Timestamp cts = new Timestamp(System.currentTimeMillis());
        String dropSql;
        if (dropObjects) {
            System.out.println("Dropping tables at " + cts);
            dropSql = "drop table if exists large_insert";
            currStmt.executeUpdate(dropSql);
            dropSql = "drop sequence if exists large_insert_seq";
            currStmt.executeUpdate(dropSql);
            System.out.println("Create/replace sequence");
            currStmt.executeUpdate("create  sequence if not exists large_insert_seq");
            System.out.println("Complete sequence creation/replacement");
            System.out.println("Create/replace table");
            String columnType = (useVariant) ? " variant  " : " string ";
            String virtVarColumn = (useVariant) ? " " : " json_msg_var variant as parse_json(json_msg),  ";
            String createTblDdl = "create  table if not exists large_insert" +
                    " (rec_seq integer not null default  large_insert_seq.nextval, " +
                    "  json_msg " + columnType + " not null, " + virtVarColumn +
                    "  created_by varchar(100)  not null  , " +
                    "  created_ts  timestamp_ltz not null  )";
            currStmt.executeUpdate(createTblDdl);
            System.out.println("Complete table creation/replacement");
        }
        if (resetLogs) {
            dropSql = "drop table if exists large_insert_log";
            currStmt.executeUpdate(dropSql);
            dropSql = "drop sequence if exists large_insert_log_seq";
            currStmt.executeUpdate(dropSql);
        }
        if (logResults) {
            currStmt.executeUpdate("create  sequence if not exists large_insert_log_seq");
            String createTblDdl = "create  table if not exists  large_insert_log" +
                    " (log_seq integer not null default  large_insert_log_seq.nextval, " +
                    "  log_msg variant ," +
                    "  created_by varchar(100)  not null default current_user , " +
                    "  created_ts  timestamp_ltz not null default current_timestamp )";
            currStmt.executeUpdate(createTblDdl);
        }
        cts = new Timestamp(System.currentTimeMillis());
        System.out.println("Complete object dropping at " + cts);
    }

    // This logs the results during execution
    private static void logResults(String logPhase, Connection logConn, double insertCnt) throws SQLException {
        Timestamp lts = new Timestamp(System.currentTimeMillis());
        long diffInMillis = Math.abs(lts.getTime() - startTs.getTime());
        long diff = TimeUnit.SECONDS.convert(diffInMillis, TimeUnit.MILLISECONDS);
        DecimalFormat decimalFormat = new java.text.DecimalFormat("#,##0.0000000000");
        double rowsPerSec = insertCnt / diff;
        double totalSizeMB = payLoadSize / (1024 * 1024);
        double totalSizeGB = totalSizeMB / 1024;
        double rateBytesPerSec = payLoadSize / diff;
        double rateMBytesPerSec = rateBytesPerSec / (1024 * 1024);
        double rateGBytesPerSec = rateMBytesPerSec / 1024;
        if (logPhase.equalsIgnoreCase("begin")) {
            jsonHeaderBuilder.add("prop_user", myprops.getProperty("user"));
            jsonHeaderBuilder.add("prop_db", myprops.getProperty("db"));
            jsonHeaderBuilder.add("prop_warehouse", myprops.getProperty("warehouse"));
            jsonHeaderBuilder.add("prop_schema", myprops.getProperty("schema"));
            jsonHeaderBuilder.add("prop_warehouse", myprops.getProperty("warehouse"));
            jsonHeaderBuilder.add("prop_role", myprops.getProperty("role"));
            jsonHeaderBuilder.add("prop_account", myprops.getProperty("account"));
            jsonHeaderBuilder.add("prop_auto_commit", myprops.getProperty("auto_commit"));
            jsonHeaderBuilder.add("prop_total_inserts", myprops.getProperty("total_inserts"));
            jsonHeaderBuilder.add("prop_batch_size", myprops.getProperty("batch_size"));
            jsonHeaderBuilder.add("prop_json_obj_size", myprops.getProperty("json_obj_size"));
            jsonHeaderBuilder.add("prop_use_variant", myprops.getProperty("use_variant"));
            jsonHeaderBuilder.add("prop_drop_objects", myprops.getProperty("drop_objects"));
            jsonHeaderBuilder.add("prop_log_results", myprops.getProperty("log_results"));
            jsonHeaderBuilder.add("prop_reset_logs", myprops.getProperty("reset_logs"));
            jsonHeaderBuilder.add("start_ts", startTs.toString());
        } else if (logPhase.equalsIgnoreCase("middle")) {
            JsonObjectBuilder jsonLogBuilder = Json.createObjectBuilder();
            jsonLogBuilder.add("log_ts", lts.toString());
            jsonLogBuilder.add("log_row", insertCnt);
            jsonLogBuilder.add("log_interval", diff);
            jsonLogBuilder.add("log_rows_per_sec", rowsPerSec);
            jsonLogBuilder.add("log_total_size", payLoadSize);
            jsonLogBuilder.add("log_total_size_mb", totalSizeMB);
            jsonLogBuilder.add("log_total_size_gb", totalSizeGB);
            jsonLogBuilder.add("log_bytes_per_sec", rateBytesPerSec);
            jsonLogBuilder.add("log_mb_per_sec", rateMBytesPerSec);
            jsonLogBuilder.add("log_gb_per_sec", rateGBytesPerSec);
            JsonObject jsonLogObj = jsonLogBuilder.build();
            jsonLogArrayBuilder.add(jsonLogObj);

        } else if (logPhase.equalsIgnoreCase("end")) {
            // Final stats for logging
            System.out.println("Added " + insertCnt + " inserts");
            System.out.println("Completed " + insertCnt + " inserts at " + lts);
            System.out.println("Execution Time in Seconds: " + diff);
            System.out.println("Rows / Second: " + ((DecimalFormat) decimalFormat).format(rowsPerSec));
            System.out.println("Estimated Bytes / Second: " + ((DecimalFormat) decimalFormat).format(rateBytesPerSec));
            System.out.println("Estimated MB / Second: " + ((DecimalFormat) decimalFormat).format(rateMBytesPerSec));
            System.out.println("Estimated GB / Second: " + ((DecimalFormat) decimalFormat).format(rateGBytesPerSec));
            System.out.println("Estimated Bytes Ingested: " + ((DecimalFormat) decimalFormat).format(payLoadSize));
            System.out.println("Estimated MB Ingested: " + ((DecimalFormat) decimalFormat).format(totalSizeMB));
            System.out.println("Estimated GB Ingested: " + ((DecimalFormat) decimalFormat).format(totalSizeGB));

            jsonHeaderBuilder.add("total_inserts", insertCnt);
            jsonHeaderBuilder.add("exec_in_secs", diff);
            jsonHeaderBuilder.add("rows_per_sec", rowsPerSec);
            jsonHeaderBuilder.add("total_size", payLoadSize);
            jsonHeaderBuilder.add("total_size_mb", totalSizeMB);
            jsonHeaderBuilder.add("total_size_gb", totalSizeGB);
            jsonHeaderBuilder.add("bytes_per_sec", rateBytesPerSec);
            jsonHeaderBuilder.add("mb_per_sec", rateMBytesPerSec);
            jsonHeaderBuilder.add("gb_per_sec", rateGBytesPerSec);
            jsonHeaderBuilder.add("end_ts", lts.toString() );

            // nesting of JSON log
            // first nest logs as an array in the header object array
            jsonHeaderBuilder.add("log_result", jsonLogArrayBuilder.build());
            // next prepare the array of header plus results
            JsonArrayBuilder jsonFinalArrayBuilder = Json.createArrayBuilder();
            JsonObject jsonFinalHdr = jsonHeaderBuilder.build();
            // then nest header in the array
            jsonFinalArrayBuilder.add(jsonFinalHdr);
            JsonObjectBuilder jsonFinalObjBuilder = Json.createObjectBuilder();
            // final log array
            jsonFinalObjBuilder.add("log", jsonFinalArrayBuilder.build());
            JsonObject jsonFinalObj = jsonFinalObjBuilder.build();
            StringWriter strWtr = new StringWriter();
            JsonWriter jsonWtr = Json.createWriter(strWtr);
            jsonWtr.writeObject(jsonFinalObj);
            jsonWtr.close();
            String logJsonStr = strWtr.toString();
            if (Boolean.parseBoolean(myprops.getProperty("log_results"))) {
                String insertResults = "insert into large_insert_log (log_msg) select parse_json(column1) from values (?)";
                PreparedStatement lstmt = logConn.prepareStatement(insertResults);
                lstmt.setString(1, logJsonStr);
                lstmt.addBatch();
                lstmt.executeBatch(); // After execution, count[0]=1, count[1]=1
                logConn.commit();
                lstmt.close();
            }
        }


    }
}