# snowflake-pipe-api
Sample Java client to call the Snowpipe REST API

Also, this is for demo purposes only, suited for Sandboxes and POCs (mileage may vary)

# References
If unfamiliar with the Snowflake Pipe REST API, please reference:
https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest.html

Also please reference the Sample Java program:
https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-load.html#sample-program-for-the-java-sdk

# Prerequisites
1. Active Snowflake and AWS account
2. Key pair authentication enabled for Snowflake user.  For information on how to setup, see:
https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-gs.html#using-key-pair-authentication
3. Working knowledge of JAVA, Snowflake, SQL, S3, and Snowpipe
4. JVM installed
5. IDE installed
6. The following libraries are required to compile
    `json-simple-1.1.1.jar
    auth-2.11.12.jar
    snowflake-ingest-sdk-0.9.7.jar
    snowflake-jdbc-3.12.1.jar
    aws-core-2.11.12.jar
    utils-2.11.12.jar
    s3-2.11.12.jar
    regions-2.11.12.jar
    sdk-core-2.11.12.jar`
7. The following libraries are required to execute
    `json-simple-1.1.1.jar
    jackson-annotations-2.10.2.jar
    jackson-core-2.10.2.jar
    jackson-databind-2.10.2.jar
    aws-query-protocol-2.11.12.jar
    aws-json-protocol-2.11.12.jar
    protocol-core-2.11.12.jar
    commons-logging-1.2.jar
    httpclient-4.5.11.jar
    httpcore-4.4.12.jar
    apache-client-2.11.12.jar
    aws-http-client-apache-2.0.0-preview-1.jar
    reactive-streams-1.0.3.jar
    profiles-2.11.12.jar
    log4j-slf4j-impl-2.13.1.jar
    log4j-api-2.13.1.jar
    log4j-core-2.13.1.jar
    slf4j-api-1.7.30.jar
    aws-xml-protocol-2.11.12.jar
    auth-2.11.12.jar
    http-client-spi-2.11.12.jar
    snowflake-ingest-sdk-0.9.7.jar
    snowflake-jdbc-3.12.1.jar
    aws-core-2.11.12.jar
    utils-2.11.12.jar
    s3-2.11.12.jar
    regions-2.11.12.jar
    sdk-core-2.11.12.jar`
8. Libraries above available on the host machine
9. Configure `pipeconfig.properties.template` with Snowflake connection properties
      
# Steps to Use
1. Git the files to a local directory
2. Edit the SQL script, change `<SNFLK_DB>` to your Snowflake database
3. Place the libraries listed in Prerequisites into an accessible folder (preferably in your existing `CLASSPATH`) 
4. Execute the following command to create a SnowflakePipeWrapper.class file.
 
        javac -cp <LIBRARY_PATH>/json-simple-1.1.1.jar:<LIBRARY_PATH>/auth-2.11.12.jar:<LIBRARY_PATH>/snowflake-ingest-sdk-0.9.7.jar:<LIBRARY_PATH>/snowflake-jdbc-3.12.1.jar:<LIBRARY_PATH>/aws-core-2.11.12.jar:<LIBRARY_PATH>/utils-2.11.12.jar:<LIBRARY_PATH>/s3-2.11.12.jar:<LIBRARY_PATH>/regions-2.11.12.jar:<LIBRARY_PATH>/sdk-core-2.11.12.jar:. -d . SnowflakePipeWrapper.java  
 
6. Edit the pipeconfig.properties.template for your Snowflake account connection details, save it a convenient and safe directory
7. Execute the following command to initialize the connections, prepare the batch inserts, and execute the statements:

        java  -cp <LIBRARY_PATH>/json-simple-1.1.1.jar:<LIBRARY_PATH>/jackson-annotations-2.10.2.jar:<LIBRARY_PATH>/jackson-core-2.10.2.jar:<LIBRARY_PATH>/jackson-databind-2.10.2.jar:<LIBRARY_PATH>/aws-query-protocol-2.11.12.jar:<LIBRARY_PATH>/aws-json-protocol-2.11.12.jar:<LIBRARY_PATH>/protocol-core-2.11.12.jar:<LIBRARY_PATH>/commons-logging-1.2.jar:<LIBRARY_PATH>/httpclient-4.5.11.jar:<LIBRARY_PATH>/httpcore-4.4.12.jar:<LIBRARY_PATH>/apache-client-2.11.12.jar:<LIBRARY_PATH>/aws-http-client-apache-2.0.0-preview-1.jar:<LIBRARY_PATH>/reactive-streams-1.0.3.jar:<LIBRARY_PATH>/profiles-2.11.12.jar:<LIBRARY_PATH>/log4j-slf4j-impl-2.13.1.jar:<LIBRARY_PATH>/log4j-api-2.13.1.jar:<LIBRARY_PATH>/log4j-core-2.13.1.jar:<LIBRARY_PATH>/slf4j-api-1.7.30.jar:<LIBRARY_PATH>/aws-xml-protocol-2.11.12.jar:<LIBRARY_PATH>/auth-2.11.12.jar:<LIBRARY_PATH>/http-client-spi-2.11.12.jar:<LIBRARY_PATH>/snowflake-ingest-sdk-0.9.7.jar:<LIBRARY_PATH>/snowflake-jdbc-3.12.1.jar:<LIBRARY_PATH>/aws-core-2.11.12.jar:<LIBRARY_PATH>/utils-2.11.12.jar:<LIBRARY_PATH>/s3-2.11.12.jar:<LIBRARY_PATH>/regions-2.11.12.jar:<LIBRARY_PATH>/sdk-core-2.11.12.jar:. com.snowflake.pipe.SnowflakePipeWrapper <LIBRARY_PATH>/pipeconfig.properties
8. Login to Snowflake account, query table `<SNFLK_DB>.<SNFLK_SCHEMA>.large_insert`
