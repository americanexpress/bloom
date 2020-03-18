/**
 *
 *   Copyright 2020 American Express Travel Related Services Company, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software distributed under the License
 *   is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 *   or implied. See the License for the specific language governing permissions and limitations under
 *   the License.
 */

package com.americanexpress.bloom.component;

import com.americanexpress.bloom.beans.JobDetails;
import com.americanexpress.bloom.config.AppConfig;
import com.americanexpress.bloom.constants.ErrorCodes;
import com.americanexpress.bloom.exceptions.BloomException;
import com.americanexpress.bloom.util.DateConversionUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.jasypt.util.text.BasicTextEncryptor;
import org.joda.time.DateTime;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This is a driver to launch the bulk load batch job. It mainly passes command
 * line arguments to LoadConfig to create application wise configurations,
 * creates the job instance and creates spark context to be used in spark job
 */
public class BulkLoadDriver {
    private static final Logger LOGGER = LogManager.getLogger(BulkLoadDriver.class);

    private static FileSystem fs;

    static {
        try {
            fs = FileSystem.get(new Configuration());
        } catch (IOException e) {
            LOGGER.error("failed to initialized file System", e);
        }
    }

    /**
     * Driver Method which creates the spark session and runs the job
     *
     * @param commandLineArgs
     * @throws BloomException - when couldn't complete the spark job successfully
     */
    public static void main(String[] commandLineArgs) throws BloomException {
        LOGGER.info("BLooM started with input args:{}", Arrays.toString(commandLineArgs));
        Job job = null;
        DateTime startTime = DateTime.now();
        boolean jobCompletion;
        AppConfig appConfig = new AppConfig(commandLineArgs);
        ;
        try {
            job = new Job(appConfig);
            //Read the table details from tableDetailsConfig.yml file and set it in config so that it can be read from anywhere
            appConfig.setMemSQLTableConfig();

            //Get the decrypted password and set it in the application config so that it can be access from anywhere
            setMemSQLPswrdInConfig(appConfig);

            //set the file name for saving the job details in the environment properties
            System.setProperty("propFile", appConfig.getCommandLineConf().getAppFilePath());

            SparkSession sparkSession = buildSparkSession(appConfig);
            LOGGER.info("Spark session created. with application ID: {}", sparkSession.sparkContext().applicationId());
            job.setSparkSession(sparkSession);
            job.getJobDetails().setStartTime(startTime.toLocalDateTime().toString());
            LOGGER.info("Job start time is -{}", job.getJobDetails().getStartTime());
            jobCompletion = job.run();

            if (jobCompletion) {
                job.getJobDetails().setStatus("Passed");
            }

        } catch (Exception e) {
            job.getJobDetails().setStatus("Failed");
            job.getJobDetails().setErrorMessage(stackTraceToString(e));
            throw new BloomException("BulkLoadMemSQL Failed to run", ErrorCodes.MBL_JOB_RUNNER_FAILED, e);
        } finally {
            DateTime endTime = DateTime.now();
            job.getJobDetails().setEndTime(endTime.toLocalDateTime().toString());
            LOGGER.info("Job end time - {}", job.getJobDetails().getEndTime());
            long diff = (endTime.getMillis() - startTime.getMillis()) / 1000;
            job.getJobDetails().setTotalTimeTaken(Long.toString(diff) + " secs");
            LOGGER.info("Total time taken:{} secs", diff);
            job.saveJobDetails();
            persistJobDetailsInFile(appConfig.getCommandLineConf().getAppFilePath(), job.getJobDetails());
            LOGGER.debug("Spark job has completed time taken:{} secs", diff);
        }

    }

    private static void setMemSQLPswrdInConfig(AppConfig appConfig) {
        BasicTextEncryptor crypto = new BasicTextEncryptor();
        {
            String key = System.getenv("symmetric_cipher_key");
            crypto.setPassword(key);
        }
        String memSQLPassword = crypto.decrypt(appConfig.getMemSQLEncryptedPassword());
        appConfig.setMemSQLPassword(memSQLPassword);
    }

    private static void persistJobDetailsInFile(String file, JobDetails jd) {
        try {
            writeToFile(file, objToJson(jd));
        } catch (Exception e) {
            LOGGER.error("Failed to persist job details to file", e);
        }
    }

    public static void writeToFile(String path, String line) throws IOException {
        checkArgument(!(path == null || path.isEmpty()), "Write To file path can't be null");
        try (FSDataOutputStream fileOutputStream = fs.create(new Path(path))) {
            fileOutputStream.writeBytes(line);
            fileOutputStream.flush();
        }

    }

    public static String objToJson(Object obj) {
        ObjectWriter ow = new ObjectMapper().writer();
        try {
            return ow.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            return "Failed to create JSON obj:" + obj.toString();
        }

    }


    /**
     * Prints the stacktrace
     *
     * @param throwable
     * @return
     */
    public static String stackTraceToString(Throwable throwable) {
        StringWriter str = new StringWriter();
        PrintWriter writer = new PrintWriter(str);
        throwable.printStackTrace(writer);
        return str.getBuffer().toString();
    }

    private static SparkSession buildSparkSession(AppConfig appConfig) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.memsql.host", appConfig.getMemSQLHost())
                .set("spark.memsql.port", appConfig.getMemSQLPort())
                .set("spark.memsql.defaultDatabase", appConfig.getMemSQLDefaultDatabase())
                .set("spark.memsql.user", appConfig.getMemSQLUsername())
                .set("spark.memsql.password", appConfig.getMemSQLPassword());

        SparkSession sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                .appName("BULK-LOADER-MEMSQL")
                .getOrCreate();
        //registering convert date udf function in the spark session
        sparkSession.udf().register("convertDate", columnValue -> {
            return DateConversionUtil.ConvertDate((String) columnValue);
        }, DataTypes.StringType);
        return sparkSession;
    }

}
