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
import com.americanexpress.bloom.constants.Constants;
import com.americanexpress.bloom.constants.ErrorCodes;
import com.americanexpress.bloom.dao.JavaMemSQLDao;
import com.americanexpress.bloom.dao.SparkMemSQLDao;
import com.americanexpress.bloom.exceptions.BloomException;
import com.americanexpress.bloom.processor.Processor;
import com.americanexpress.bloom.processor.impl.BulkLoadProcessor;
import com.americanexpress.bloom.processor.impl.DeltaLoadProcessor;
import com.americanexpress.bloom.processor.impl.RecordCountCheckProcessor;
import com.americanexpress.bloom.reader.Reader;
import com.americanexpress.bloom.reader.ReaderFactory;
import com.americanexpress.bloom.schema.EntitySchema;
import com.americanexpress.bloom.schema.SchemaGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.upper;

/***
 * This class represents the main bloom Job which uses spark session
 * to read data from input applies various transformations and finally
 * saves it to MemSQL
 */

public class Job {
    private static final Logger LOGGER = LogManager.getLogger(Job.class);
    private SparkSession sparkSession;
    private AppConfig appConfig;
    private JobDetails jobDetails;
    private SparkMemSQLDao sparkMemSQLDao;
    private JavaMemSQLDao javaMemSQLDao;

    public Job(AppConfig appConfig) {
        this.appConfig = appConfig;
        this.jobDetails = new JobDetails();
        this.jobDetails.setTableName(appConfig.getCommandLineConf().getTableName());
        this.jobDetails.setSourceType(appConfig.getCommandLineConf().getInputType());
        this.jobDetails.setInputFilePath(appConfig.getCommandLineConf().getInputLocation());
        this.jobDetails.setRefreshType(appConfig.getCommandLineConf().getRefreshType());
        this.jobDetails.setStatus("Failed");
        this.javaMemSQLDao = new JavaMemSQLDao();
    }

    /**
     * validate if the configured hive columns are valid hive columns
     *
     * @param configuredHiveColumnNames
     * @param hiveColNames
     * @throws BloomException
     */
    private static void validateHiveColumns(List<String> configuredHiveColumnNames, List<String> hiveColNames) throws BloomException {
        if (!hiveColNames.containsAll(configuredHiveColumnNames)) {
            LOGGER.warn("Not all hive columns configured are present in the hive table.. !");
            throw new BloomException("Not all hive columns configured are present in the hive table.. ! ", ErrorCodes.MBL_HIVE_TABLE_VALIDATION_FAILED);
        }
    }

    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        this.jobDetails.setApplicationId(sparkSession.sparkContext().applicationId());
        this.sparkMemSQLDao = new SparkMemSQLDao(sparkSession);
    }

    public JobDetails getJobDetails() {
        return jobDetails;
    }

    public boolean run() throws BloomException, FileNotFoundException, SQLException {
        boolean completed;
        Processor processor;
        DateTime startTime = DateTime.now();
        this.jobDetails.setStartTime(startTime.toLocalDateTime().toString());
        LOGGER.info("Job start time is -{}", this.jobDetails.getStartTime());

        //local variable to be used within the method
        String inputType = this.appConfig.getCommandLineConf().getInputType();
        String refreshType = this.appConfig.getCommandLineConf().getRefreshType();
        String tableType = this.appConfig.getCommandLineConf().getMemSQLTableType();

        //fail the job if load_append refresh type is requested for rowstore type table
        if (refreshType.equalsIgnoreCase(Constants.LOAD_APPEND_MODE) && tableType.equalsIgnoreCase(Constants.TABLE_TYPE_ROWSTORE))
            throw new BloomException("RefreshType LOAD-APPEND not supported for rowstore tables.. ! ");

        //get the reader for csv/hive
        Reader reader = ReaderFactory.getReader(inputType);
        reader.init(this.sparkSession, this.appConfig, this.sparkMemSQLDao, this.jobDetails);

        //Reads the first row for csv to get the input field names and for hive get those from the tableConfigDetails yml file
        String[] inputHeaderFieldNames;
        Dataset<Row> columnDataSet = reader.readFirstRow();
        if (inputType.equalsIgnoreCase("csv")) {
            if (!columnDataSet.toJavaRDD().isEmpty())
                inputHeaderFieldNames = columnDataSet.first().schema().fieldNames();
            else {
                throw new BloomException("Data in input file is not valid: Header/Data absent!!!");
            }
        } else {

            List<String> inputMemSQLTableColumnNames = this.appConfig.getMemSQLTableConf().getInputMemSQLTableColumnNames();
            inputHeaderFieldNames = inputMemSQLTableColumnNames.toArray(new String[inputMemSQLTableColumnNames.size()]);
            List<String> columNames = new ArrayList<>();
            for (Row row : columnDataSet.collectAsList()) {
                String colName = row.getString(0) == null ? "" : row.getString(0);
                columNames.add(colName);
            }
            //validate if the configured hive columns are valid hive columns
            validateHiveColumns(this.appConfig.getMemSQLTableConf().getHiveTableColumnNames(), columNames);
        }

        //validate MemSQL columns configured are correct or not
        this.javaMemSQLDao.init(appConfig);
        validateMemSQLCols(inputHeaderFieldNames, javaMemSQLDao.getMemSQLTableColNames());

        EntitySchema entitySchema = null;
        try {
            entitySchema = SchemaGenerator
                    .generateEntitySchema(this.appConfig, inputHeaderFieldNames);
        } catch (Exception e) {
            throw new BloomException("Failed while generating Schema", ErrorCodes.MBL_JOB_RUNNER_FAILED, e);
        }
        //validates if the mandatory columns(primary keys and last modified timestamp column) are present for FULL_REFRESH and UPSERT mode
        if (!this.appConfig.getCommandLineConf().getRefreshType().equalsIgnoreCase(Constants.LOAD_APPEND_MODE))
            validateMandatoryColumns(inputHeaderFieldNames, entitySchema);


        //reads the input data
        Dataset<Row> inputDataSet = reader.read(entitySchema).cache();
        long recordCount = inputDataSet.count();
        this.jobDetails.setInputRecordsCount(Long.toString(recordCount));


        // Apply checks and balances
        if (inputType.equalsIgnoreCase("csv")) {
            RecordCountCheckProcessor recordCountCheckProcessor = new RecordCountCheckProcessor(appConfig);
            if (recordCountCheckProcessor.isCtlFilePresent())
                recordCountCheckProcessor.validateRecordCount(inputDataSet);
        }

        LOGGER.info("Total input records:{}", recordCount);

        //Stop the job if the input has 0 record to be processed
        if (recordCount == 0L) {
            LOGGER.warn("Input record count is zero, aborting JOB !");
            throw new BloomException("No Input record found ", ErrorCodes.MBL_CSV_READER_FAILED);

        }
        //converts the incoming timestamp column to standard cornerstone timestamp format
        inputDataSet = standardizeTimestampFormat(inputDataSet, entitySchema);

        processor = getProcessorForRefreshType(refreshType, tableType);
        // processes the data using the processor based on the refresh type
        Dataset<Row> processedDataset = processor.getProcessedDataSet(inputDataSet, entitySchema, this.appConfig).cache();
        //DO NOT REMOVE: this count triggers actions to execute map functions on all records, before invoking save
        Long proccessedRecordCount = processedDataset.count();
        LOGGER.info("record count after processing is " + proccessedRecordCount);


        //save the processed data
        String tableName = entitySchema.getTableName();
        if (tableType.equalsIgnoreCase(Constants.TABLE_TYPE_COLUMNSTORE) && refreshType.equalsIgnoreCase(Constants.UPSERT_MODE)) {
            tableName = tableName.concat("_stage");
        }
        //save the data in the staging table whenever the table is of type columnstore
        String saveMode;
        if (tableType.equalsIgnoreCase(Constants.TABLE_TYPE_COLUMNSTORE)) {
            saveMode = "ignore";
            if (refreshType.equalsIgnoreCase(Constants.UPSERT_MODE)) {
                this.javaMemSQLDao.deleteStagingTable();
            } else if (refreshType.equalsIgnoreCase(Constants.FULL_REFRESH_MODE)) {
                this.javaMemSQLDao.deleteOriginalTable();
            }
        } else {
            saveMode = "overwrite";
        }
        LOGGER.info("table type is " + tableType + " and saveMode is " + saveMode);
        sparkMemSQLDao.save(processedDataset, tableName, saveMode);

        if (tableType.equalsIgnoreCase(Constants.TABLE_TYPE_COLUMNSTORE) && refreshType.equalsIgnoreCase(Constants.UPSERT_MODE)) {
            javaMemSQLDao.save();
        }

        //set the details so that it can saved in the bloom_job_history table
        this.jobDetails.setInputRecordsCount(Long.toString(recordCount));
        this.jobDetails.setProcessedRecordsCount(Long.toString(proccessedRecordCount));

        completed = true;
        return completed;
    }

    private void validateMemSQLCols(String[] inputHeaderFieldNames, List<String> memSQLTableColNames) throws BloomException {
        List<String> configuredMemSQLCols = Arrays.asList(inputHeaderFieldNames);
        if (!memSQLTableColNames.containsAll(configuredMemSQLCols)) {
            LOGGER.warn("There is a mismatch in the configured columns and the ones in MemSQL.. !");
            LOGGER.info("MemSQL columns: " + memSQLTableColNames);
            LOGGER.info("Configured columns: " + configuredMemSQLCols);
            throw new BloomException("There is a mismatch in the configured columns and the ones in MemSQL.... ! ", ErrorCodes.MBL_INPUT_VALIDATION_FAILED);
        }
    }

    /**
     * Validates that the timestamp column and primary key columns are not missing in the data
     *
     * @param csvHeaderFieldNames
     * @param entitySchema
     * @throws BloomException
     */
    private void validateMandatoryColumns(String[] csvHeaderFieldNames, EntitySchema entitySchema) throws BloomException {
        List<String> primaryKeyColumns = entitySchema.getPrimaryKeyColumnNames();
        String timestampColumnName = entitySchema.getTimestampColumnName();

        if (!Arrays.asList(csvHeaderFieldNames).contains(timestampColumnName)) {
            LOGGER.warn("Timestamp Column absent !");
            throw new BloomException("Timestamp Column absent in the data file!", ErrorCodes.MBL_MANDATORY_COLUMNS_VALIDATION_FAILED);
        }
        if (!Arrays.asList(csvHeaderFieldNames).containsAll(primaryKeyColumns)) {
            LOGGER.warn("Primary Key Columns Missing  !");
            throw new BloomException("Primary Key Columns absent  in the data file!", ErrorCodes.MBL_MANDATORY_COLUMNS_VALIDATION_FAILED);
        }

    }

    /**
     * Transforms the incoming timestamp column to the standard cornerstone formar
     *
     * @param inputDataSet - incoming dataset
     * @param entitySchema - incoming schema information file
     * @return transformed dataset
     */
    private Dataset<Row> standardizeTimestampFormat(Dataset<Row> inputDataSet, EntitySchema entitySchema) {
        inputDataSet = inputDataSet.withColumn(entitySchema.getTimestampColumnName(), callUDF("convertDate", inputDataSet.col(entitySchema.getTimestampColumnName()).cast(DataTypes.StringType)));
        return inputDataSet;
    }

    /**
     * Standardises primary key column by coverting them to upper case
     *
     * @param inputDataSet
     * @param entitySchema
     * @return
     */
    private Dataset<Row> standardizePrimaryKey(Dataset<Row> inputDataSet, EntitySchema entitySchema) {
        for (String primaryKey : entitySchema.getPrimaryKeyColumnNames()) {
            inputDataSet = inputDataSet.withColumn(primaryKey, upper(inputDataSet.apply(primaryKey)));

        }
        return inputDataSet;
    }

    /**
     * Get the proper processor -FULL_REFRESH, Load Append and UPSERT
     *
     * @param refreshType
     * @return
     */
    private Processor getProcessorForRefreshType(String refreshType, String tableType) throws BloomException {
        if (!refreshType.equalsIgnoreCase(Constants.FULL_REFRESH_MODE) && !refreshType.equalsIgnoreCase(Constants.LOAD_APPEND_MODE) && !refreshType.equalsIgnoreCase(Constants.UPSERT_MODE))
            throw new BloomException("No valid processor present for this refreshType");
        if (refreshType.equalsIgnoreCase(Constants.FULL_REFRESH_MODE) || refreshType.equalsIgnoreCase(Constants.LOAD_APPEND_MODE) || tableType.equalsIgnoreCase("columnstore")) {
            return new BulkLoadProcessor(this.sparkSession);
        } else if (refreshType.equalsIgnoreCase(Constants.UPSERT_MODE)) {
            return new DeltaLoadProcessor(this.sparkSession);
        } else {
            throw new BloomException("No valid processor present for this refreshType");
        }
    }

    /***
     * Method is used to save the job history in the bloom_job_history table
     * @throws BloomException
     */
    public void saveJobDetails() throws BloomException {
        LOGGER.info("Saving Job Details in bloom_job_history table");
        Encoder<JobDetails> jobDetailsEncoder = Encoders.bean(JobDetails.class);
        if (this.sparkSession != null) {
            Dataset<JobDetails> jobDetailsDataset = this.sparkSession.createDataset(Collections.singletonList(jobDetails), jobDetailsEncoder);
            this.sparkMemSQLDao.save(jobDetailsDataset, "bloom_job_history", "overwrite");
        }
    }
}
