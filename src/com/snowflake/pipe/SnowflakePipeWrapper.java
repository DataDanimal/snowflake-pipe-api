package com.snowflake.pipe;


import net.snowflake.client.jdbc.internal.org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import net.snowflake.client.jdbc.internal.org.bouncycastle.jce.provider.BouncyCastleProvider;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.PEMParser;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import net.snowflake.client.jdbc.internal.org.bouncycastle.operator.InputDecryptorProvider;
import net.snowflake.client.jdbc.internal.org.bouncycastle.operator.OperatorCreationException;
import net.snowflake.client.jdbc.internal.org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import net.snowflake.client.jdbc.internal.org.bouncycastle.pkcs.PKCSException;
import net.snowflake.ingest.SimpleIngestManager;
import net.snowflake.ingest.connection.HistoryRangeResponse;
import net.snowflake.ingest.connection.HistoryResponse;
import net.snowflake.ingest.connection.IngestResponse;
import net.snowflake.ingest.utils.StagedFileWrapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import javax.crypto.spec.PBEKeySpec;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivateKey;
import java.security.Security;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class SnowflakePipeWrapper {
    static Properties myProps = new Properties();
    static String propFile;
    private static String PUBLIC_KEY_FILE;
    private static String PRIVATE_KEY_FILE;
    private static SimpleIngestManager manager;
    private static S3Client s3;

    private static HistoryResponse waitForFilesHistory(SimpleIngestManager manager,
                                                       Set<String> files,
                                                       long timeoutInMillis)
            throws Exception {

        String beginMark = null;
        Set<String> filesWatchList = files;
        HistoryResponse filesHistory = null;
        long start = System.currentTimeMillis();
        long end = start + timeoutInMillis;
        while (true) {
            Thread.sleep(10000);
            HistoryResponse response = manager.getHistory(null, null, beginMark);
            if (response.getNextBeginMark() != null) {
                beginMark = response.getNextBeginMark();
            }
            if (response != null && response.files != null) {
                for (HistoryResponse.FileEntry entry : response.files) {
                    //if we have a complete file that we've
                    // loaded with the same name..
                    String filename = entry.getPath();
                    if (entry.getPath() != null && entry.isComplete() &&
                            filesWatchList.contains(filename)) {
                        if (filesHistory == null) {
                            filesHistory = new HistoryResponse();
                            filesHistory.setPipe(response.getPipe());
                        }
                        filesHistory.files.add(entry);
                        filesWatchList.remove(filename);
                        //we can return true!
                        if (filesWatchList.isEmpty()) {
                            return filesHistory;
                        }
                    }
                }
            }
            if (System.currentTimeMillis() > end) {
                throw new TimeoutException("Timeout limit reached at " + new Timestamp(System.currentTimeMillis()).toString());
                //  break;
            }
        }
    }

    private static IngestResponse insertFile(String filename) throws Exception {
        StagedFileWrapper myFile = new StagedFileWrapper(filename, (Long) null);
        return manager.ingestFile(myFile, (UUID) null);
    }

    private static IngestResponse insertFiles(Set<String> files) throws Exception {
        SimpleIngestManager var10001 = manager;
        return manager.ingestFiles(SimpleIngestManager.wrapFilepaths(files), (UUID) null);
    }

    // This method establishes the Snowflake connection based on the connection properties in config.properties
    private static Connection getConnection(String accountName, PrivateKey privKey)
            throws SQLException {

        String connectStr;
        try {
            Class.forName("com.snowflake.client.jdbc.SnowflakeDriver");
        } catch (ClassNotFoundException ex) {
            System.err.println("Driver not found");
        }
        myProps.put("privateKey", privKey);
        connectStr = "jdbc:snowflake://" + accountName + ".snowflakecomputing.com";
        System.out.println("Getting Connection string " + connectStr);
        if (Boolean.parseBoolean(myProps.getProperty("debug"))) {
            System.out.println("Getting Connection properties " + myProps);
        }
        Connection myConn = DriverManager.getConnection(connectStr, myProps);
        // myConn.setAutoCommit(Boolean.parseBoolean(myProps.getProperty("auto_commit")));
        return myConn;
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
            String snflkWh = properties.getProperty("SNFLK_WH");
            System.out.println("Getting warehouse " + snflkWh);
            snflkProps.put("warehouse", snflkWh);
            String snflkDb = properties.getProperty("SNFLK_DB");
            System.out.println("Getting database " + snflkDb);
            snflkProps.put("db", snflkDb);
            String snflkSch = properties.getProperty("SNFLK_SCHEMA");
            System.out.println("Getting schema " + snflkSch);
            snflkProps.put("schema", snflkSch);
            String snflkStage = properties.getProperty("SNFLK_STAGE");
            System.out.println("Getting stage " + snflkStage);
            snflkProps.put("stage", snflkStage);
            String snflkPipe = properties.getProperty("SNFLK_PIPE");
            System.out.println("Getting pipe " + snflkPipe);
            snflkProps.put("pipe", snflkPipe);
            String snflkAutoCommit = properties.getProperty("SNFLK_AUTO_COMMIT");
            System.out.println("Getting Auto commit " + snflkAutoCommit);
            snflkProps.put("auto_commit", snflkAutoCommit);
            String snflkRole = properties.getProperty("SNFLK_ROLE");
            System.out.println("Getting role " + snflkRole);
            snflkProps.put("role", snflkRole);
            String snflkAcct = properties.getProperty("SNFLK_ACCT");
            System.out.println("Getting Account " + snflkAcct);
            snflkProps.put("account", snflkAcct);
            String debugMode = properties.getProperty("DEBUG_MODE");
            System.out.println("Getting Debug Mode " + debugMode);
            if (debugMode != null) snflkProps.put("debug", debugMode);
            String snflkDropObjects = properties.getProperty("DROP_OBJECTS");
            if (snflkDropObjects != null) snflkProps.put("drop_objects", snflkDropObjects);
            System.out.println("Drop objects prior to execution, i.e. if you want to start clean " + snflkDropObjects);
            String snflkLogResults = properties.getProperty("LOG_RESULTS");
            snflkProps.put("log_results", snflkLogResults);
            System.out.println("Log results post-execution, i.e. if you want to analyze cross tests " + snflkLogResults);
            String snflkResetLogs = properties.getProperty("RESET_LOGS");
            snflkProps.put("reset_logs", snflkResetLogs);
            System.out.println("Clear logs prior to execution " + snflkResetLogs);
            String snflkPrivateKey = properties.getProperty("PRIVATE_KEY_FILE");
            System.out.println("Getting private key from  " + snflkPrivateKey);
            snflkProps.put("private_key", snflkPrivateKey);
            String snflkPublicKey = properties.getProperty("PUBLIC_KEY_FILE");
            System.out.println("Getting public key from  " + snflkPublicKey);
            snflkProps.put("private_key", snflkPrivateKey);
            String snflkSourceFile = properties.getProperty("PIPE_SOURCE_FILES");
            System.out.println("Getting local files   " + snflkSourceFile);
            if (snflkSourceFile != null) snflkProps.put("source_files", snflkSourceFile);
            String snflkSourcePath = properties.getProperty("PIPE_SOURCE_PATH");
            System.out.println("Getting source files from path from  " + snflkSourcePath);
            snflkProps.put("source_path", snflkSourcePath);
            String snflkSourceFileExt = properties.getProperty("PIPE_FILE_EXT");
            System.out.println("Getting source file extension  " + snflkSourceFileExt);
            snflkProps.put("file_ext", snflkSourceFileExt);
            String snflkTargetPath = properties.getProperty("PIPE_TARGET_PATH");
            System.out.println("Getting pipe target path from  " + snflkTargetPath);
            snflkProps.put("target_path", snflkTargetPath);
            String pipeBasePath = properties.getProperty("PIPE_BASE_PATH");
            System.out.println("Getting Pipe Base path " + pipeBasePath);
            snflkProps.put("pipe_base", pipeBasePath);
            String snflkPassPhrase = properties.getProperty("PRIVATE_KEY_PASSPHRASE");
            System.out.println("Getting passphrase *******");
            snflkProps.put("passphrase", snflkPassPhrase);
            String snflkBatchWindow = properties.getProperty("BATCH_WINDOW");
            System.out.println("Getting batch window in milliseconds " + snflkBatchWindow);
            snflkProps.put("window", snflkBatchWindow);
            String awsAccessKey = properties.getProperty("AWS_ACCESS_KEY_ID");
            System.out.println("Getting AWS Access Key ******");
            snflkProps.put("aws_access_key", awsAccessKey);
            String awsSecretAccessKey = properties.getProperty("AWS_SECRET_ACCESS_KEY");
            System.out.println("Getting AWS Access Secret ******");
            snflkProps.put("aws_access_secret", awsSecretAccessKey);
            String awsRegion = properties.getProperty("AWS_REGION");
            System.out.println("Getting AWS Region " + awsRegion);
            snflkProps.put("aws_region", awsRegion);
            String awsS3Bucket = properties.getProperty("S3_BUCKET");
            System.out.println("Getting AWS S3 Bucket " + awsS3Bucket);
            snflkProps.put("aws_s3_bucket", awsS3Bucket);

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return snflkProps;
    }

    public static void main(String[] args) {
        try {
            // Get properties file from command line
            propFile = args[0];
            Timestamp ts = new Timestamp(System.currentTimeMillis());
            System.out.println("Reading property file " + propFile + " at " + ts);
            myProps = getSnflkProps();
            Set<String> files = new TreeSet<>();
            Set<String> filesToIngest = new TreeSet<>();

            String account = myProps.getProperty("account");
            String host = account + ".snowflakecomputing.com";
            String user = myProps.getProperty("user");
            String database = myProps.getProperty("db");
            String schema = myProps.getProperty("schema");
            String stage = myProps.getProperty("stage");
            String pipe = database + "." + schema + "." + myProps.getProperty("pipe");
            String source_path = myProps.getProperty("source_path");
            String source_files = myProps.getProperty("source_files");
            String target_path = myProps.getProperty("target_path");
            String pipe_base = myProps.getProperty("pipe_base");
            String fileExt = myProps.getProperty("file_ext").toLowerCase();
            Long window = Long.parseLong(myProps.getProperty("window"));

            PUBLIC_KEY_FILE = myProps.getProperty("public_key");
            PRIVATE_KEY_FILE = myProps.getProperty("private_key");

            //final long batchTimeInMillis =  window;
            String startTime = Instant
                    .ofEpochMilli(System.currentTimeMillis()).toString();
            //Timestamp endTimestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis() +  batchTimeInMillis));
            final PrivateKey privateKey = PrivateKeyReader.get(PRIVATE_KEY_FILE);

            manager = new SimpleIngestManager(account, user, pipe, privateKey, "https", host, 443);
            LocalDateTime today = LocalDateTime.now();
            String year = Integer.toString(today.getYear());
            int month = today.getMonthValue();
            int day = today.getDayOfMonth();
            int hour = today.getHour();
            int minute = today.getMinute();
            String datePath = year + "/" + String.format("%02d", month) + "/" + String.format("%02d", day) + "/" + String.format("%02d", hour) + "/" + String.format("%02d", minute);

            String pipeTargetPath = target_path + "/" + datePath;
            String fileReadyPath = source_path + "/ready";
            File sourceFiles = new File(fileReadyPath) ;
            String[] source_file_array = sourceFiles.list();
            for (String file : source_file_array) {
                if (file.toLowerCase().endsWith(fileExt) || fileExt.equalsIgnoreCase("*.*"))  files.add(file);
            }
            filesToIngest = putFiles(files, fileReadyPath, pipe_base, pipeTargetPath);
            UUID requestId = UUID.randomUUID();
            IngestResponse response = manager.ingestFiles(SimpleIngestManager.wrapFilepaths(filesToIngest), requestId);
            System.out.println("Received ingest response: " + response.toString() + " at " + new Timestamp(System.currentTimeMillis()));
            ;
            HistoryResponse history = waitForFilesHistory(manager, filesToIngest, window);
            String endTime = Instant
                    .ofEpochMilli(System.currentTimeMillis()).toString();

            HistoryRangeResponse historyRangeResponse =
                    manager.getHistoryRange(requestId,
                            startTime,
                            endTime);

            JSONObject jsonResults = verifyResults(historyRangeResponse, fileReadyPath, source_path);
            logResults(jsonResults, account, privateKey);
            Timestamp endTs = new Timestamp(System.currentTimeMillis());
            long diffInMillis = Math.abs(endTs.getTime() - ts.getTime());
            long diff = TimeUnit.SECONDS.convert(diffInMillis, TimeUnit.MILLISECONDS);
            System.out.println("Completed Pipe execution at " + endTs);
            System.out.println("Total execution time (in milliseconds): " + diffInMillis + " (" + diff + " seconds)");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static Set<String> putFiles(Set<String> files, String filesLocation, String pipeBasePath, String pipeTargetPath) throws Exception, S3Exception {
        String filesLocationFormatted = "" + filesLocation;
        String awsAccessKey = myProps.getProperty("aws_access_key");
        String awsAccessSecret = myProps.getProperty("aws_access_secret");
        String awsRegion = myProps.getProperty("aws_region");
        String awsS3Bucket = myProps.getProperty("aws_s3_bucket"); //+ "/" + myProps.getProperty("target_path");
        Region region = Region.of(awsRegion) ;
        System.setProperty("aws.accessKeyId", awsAccessKey);
        System.setProperty("aws.secretAccessKey", awsAccessSecret);
        SystemPropertyCredentialsProvider sp = SystemPropertyCredentialsProvider.create();
        s3 = S3Client.builder().region(region).credentialsProvider(sp).build();
        String bucket = awsS3Bucket + System.currentTimeMillis();
        String key = awsAccessKey;
        String s3TargetPath = pipeBasePath + "/" + pipeTargetPath;
        Set<String> filesStaged = new TreeSet<>();
        System.out.println("Start putting files to S3 " + awsS3Bucket + " at " + new Timestamp(System.currentTimeMillis()));
        files.forEach((file) -> {
            File f = Paths.get(filesLocationFormatted + "/" + file).getFileName().toFile();
            String fileToAdd = pipeTargetPath + "/" + file.toString();
            String fileToStaged = s3TargetPath + "/" + file.toString();
            filesStaged.add(fileToAdd);
            s3.putObject(PutObjectRequest.builder().bucket(awsS3Bucket).key(fileToStaged)
                            .build(),
                    RequestBody.fromFile(Paths.get(filesLocationFormatted + "/" + file)));
        });
        System.out.println("Complete putting files to S3 " + awsS3Bucket + " at " + new Timestamp(System.currentTimeMillis()));
        return filesStaged;
    }

    private static JSONObject verifyResults(HistoryRangeResponse historyRangeResponse, String fileReadyPath, String source_path)
            throws ParseException, IOException {
        String historyResponseStr = historyRangeResponse.toString();
        System.out.println("Received history range response (as String): " +
                historyResponseStr);
        String historyResponseFormatted = new String("{" + historyResponseStr.replace("History Range Result:", "")
                .replace("Complete result", "completeResult")
                //.replaceAll(": '", "\\: '")
                .replaceAll("(: )([^'])", "\"$1\"$2")
                .replaceAll("(-------------)", "")
                .replaceAll("(?m)^\\s", "")  // empty lines
                .replaceAll("(^)", "\"")
                .replaceAll("(\\n)", "\"$1,\"")
                .replaceAll("\"(\\{|}|},)\"", "$1")
                .replaceAll(",(},)", "$1")
                .replaceAll("(\\{)\\n,", "$1")
                .replaceAll("(Path):", "$1\":\"")
                .replaceFirst("(,)(\\{\"Path)", "$1\"result\": [$2")
                .replaceAll("(},)", "}")
                .replaceAll("(,})", "}")
                + "]}").replaceAll("(,)(\")(])", "$3")
                ;
        System.out.println("Received history range response (as JSON): " +
                historyResponseFormatted);
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObj = (JSONObject) jsonParser.parse(historyResponseFormatted);



        JSONArray results = (JSONArray) jsonObj.get("result");
        @SuppressWarnings("unchecked")
        Iterator<JSONObject> it = results.iterator();
        while (it.hasNext()) {
            JSONObject jsObj = it.next();
            String filePath = (String) jsObj.get("Path");
            String baseFile = filePath.substring(filePath.lastIndexOf("/") + 1);
            String fileStatus = (String) jsObj.get("Status");
            Integer fileErrors = Integer.parseInt((String) jsObj.get("ErrorsSeen"));
            //           System.out.println("******* File Path in JSON Object = " + filePath + " at " + new Timestamp(System.currentTimeMillis()));
            //           System.out.println("******* Base File in JSON Object = " + baseFile + " at " + new Timestamp(System.currentTimeMillis()));
            //           System.out.println("******* Status in JSON Object = " + fileStatus + " at " + new Timestamp(System.currentTimeMillis()));
            //           System.out.println("******* Errors in JSON Object = " + fileErrors + " at " + new Timestamp(System.currentTimeMillis()));

            String sourceFile = fileReadyPath + "/" + baseFile;
            String targetFile = source_path;
            if (fileStatus.equalsIgnoreCase("LOADED") && fileErrors == 0) {
                targetFile = targetFile + "/complete/" + baseFile;
            } else {
                targetFile = targetFile + "/error/" + baseFile;
            }
            System.out.println("******* Moving file " + sourceFile + " to " + targetFile);
            Path temp = Files.move(Paths.get(sourceFile), Paths.get(targetFile));
        }
        return jsonObj;
    }

    // This logs the results post-execution
    private static void logResults(JSONObject jsonResults, String account, PrivateKey privateKey) throws SQLException {
        Connection logConn = getConnection(account, privateKey);
        String logJsonStr = jsonResults.toString();

        Statement statement = logConn.createStatement();
        Boolean dropObjects = Boolean.parseBoolean(myProps.getProperty("drop_objects"));
        Boolean logResults = Boolean.parseBoolean(myProps.getProperty("log_results"));
        Boolean resetResults = Boolean.parseBoolean(myProps.getProperty("reset_logs"));

        Timestamp cts = new Timestamp(System.currentTimeMillis());
        String sqlStr;
        if (dropObjects) {
            System.out.println("Dropping tables at " + cts);
            sqlStr = "drop table if exists pipe_api_log";
            statement.executeUpdate(sqlStr);
            sqlStr = "drop sequence if exists pipe_api_log_seq";
            statement.executeUpdate(sqlStr);
            System.out.println("Creating tables to log pipe results at " + cts);
            statement.executeUpdate("create  sequence if not exists pipe_api_log_seq");
            sqlStr = "create  table if not exists  pipe_api_log" +
                    " (pipe_log_seq integer not null default  pipe_api_log_seq.nextval, " +
                    "  pipe_log_msg variant ," +
                    "  created_by varchar(100)  not null default current_user , " +
                    "  created_ts  timestamp_ltz not null default current_timestamp )";
            statement.executeUpdate(sqlStr);
        }
        cts = new Timestamp(System.currentTimeMillis());
        if (resetResults) {
            System.out.println("Truncating existing pipe results " + cts);
            sqlStr = "truncate table  pipe_api_log";
            statement.executeUpdate(sqlStr);
        }
        if (logResults) {
            String insertResults = "insert into pipe_api_log (pipe_log_msg) select parse_json(column1) from values (?)";
            PreparedStatement lstmt = logConn.prepareStatement(insertResults);
            lstmt.setString(1, logJsonStr);
            lstmt.addBatch();
            lstmt.executeBatch(); // After execution, count[0]=1, count[1]=1
            logConn.commit();
            lstmt.close();
        }
    }

    public static class PrivateKeyReader {

        private static PrivateKey get(String keyFilePath)

                throws IOException, OperatorCreationException, PKCSException, Exception {

            String password = myProps.getProperty("passphrase");
            PEMParser pemParser = new PEMParser(new FileReader(Paths.get(keyFilePath).toFile()));
            PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo = (PKCS8EncryptedPrivateKeyInfo) pemParser.readObject();
            pemParser.close();
            Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME);
            Security.insertProviderAt(new BouncyCastleProvider(), 1);
            InputDecryptorProvider pkcs8Prov = new JceOpenSSLPKCS8DecryptorProviderBuilder().build(password.toCharArray());
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME);
            PBEKeySpec keySpec = new PBEKeySpec(password.toCharArray());
            PrivateKeyInfo decryptedPrivateKeyInfo = encryptedPrivateKeyInfo.decryptPrivateKeyInfo(pkcs8Prov);
            PrivateKey privateKey = converter.getPrivateKey(decryptedPrivateKeyInfo);
            return privateKey;

        }
    }


}