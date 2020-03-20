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
package com.americanexpress.bloom.schema;

import com.americanexpress.bloom.config.AppConfig;
import com.americanexpress.bloom.config.MemSQLTableConfig;
import com.americanexpress.bloom.constants.ErrorCodes;
import com.americanexpress.bloom.exceptions.BloomException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * Class to generate schema by reading the configured yaml
 */

public class SchemaGenerator {
    private static final Logger LOGGER = LogManager.getLogger(SchemaGenerator.class);


    public static EntitySchema generateEntitySchema(AppConfig appConfig, String[] inpuFieldNames) throws BloomException {
        List<String> dtoFieldsList = new ArrayList<String>();
        List<StructField> inputFields = new ArrayList();
        EntitySchema entitySchema = new EntitySchema();
        List<String> primaryKeyColumns = appConfig.getMemSQLTableConf().getMemSQLTablePrimaryKeyColumnNames();
        if (primaryKeyColumns == null || primaryKeyColumns.isEmpty()) {
            throw new BloomException("primaryKeyColumns not configured in the yaml file. ! ");
        }
        List<String> autogeneratedColumnNames = appConfig.getMemSQLTableConf().getMemSQLTableAutogeneratedColumnNames();

        for (String columnName : appConfig.getMemSQLTableConf().getMemSQLTableColumnNames()) {
            if (autogeneratedColumnNames == null || !autogeneratedColumnNames.contains(columnName)) {
                if (primaryKeyColumns.contains(columnName))
                    inputFields.add(DataTypes.createStructField(columnName, DataTypes.StringType, false));
                else
                    inputFields.add(DataTypes.createStructField(columnName, DataTypes.StringType, true));
                dtoFieldsList.add(columnName);

            }
        }
        entitySchema.setTableName(appConfig.getMemSQLTableConf().getMemSQLTableName());
        entitySchema.setPrimaryKeyColumnNames(primaryKeyColumns);
        if (appConfig.getMemSQLTableConf().getMandatoryUpdateColumnNames() != null && appConfig.getCommandLineConf().getRefreshType().equalsIgnoreCase("load_append"))
            entitySchema.setMandatoryUpdateColumnNames(appConfig.getMemSQLTableConf().getMandatoryUpdateColumnNames());
        String timestampColumnName = appConfig.getMemSQLTableConf().getMemSQLTableTimestampColumnName();
        if (timestampColumnName != null && !timestampColumnName.isEmpty()) {
            entitySchema.setTimestampColumnName(appConfig.getMemSQLTableConf().getMemSQLTableTimestampColumnName());
        } else {
            throw new BloomException("Timestamp column not configured in the yaml file. ! ");
        }

        if (appConfig.getCommandLineConf().getInputType().equalsIgnoreCase("csv")) {
            validateInputHeaderFields(dtoFieldsList, inpuFieldNames);
        } else {
            validateHiveColumns(appConfig.getMemSQLTableConf().getInputMemSQLTableColumnNames(), inpuFieldNames);
        }

        StructType schema = DataTypes.createStructType(inputFields);
        entitySchema.setDtoFieldsSchema(schema);
        entitySchema.setInputFieldsSchema(generateInputFieldsSchema(inpuFieldNames, appConfig.getMemSQLTableConf()));
        return entitySchema;
    }


    private static void validateInputHeaderFields(List<String> dtoFieldsList, String[] inpuFieldNames) throws BloomException {
        if (!dtoFieldsList.containsAll(Arrays.asList(inpuFieldNames))) {
            LOGGER.warn("input has headers different than the table columns.. !");
            throw new BloomException("input has headers different than the table columns.. ! ", ErrorCodes.MBL_INPUT_VALIDATION_FAILED);
        }
    }

    private static void validateHiveColumns(List<String> hiveColumnNames, String[] inpuFieldNames) throws BloomException {
        if (!hiveColumnNames.containsAll(Arrays.asList(inpuFieldNames))) {
            LOGGER.warn("Not all mapped Memsql columns are present in the configured MemSQL columns table.. !");
            throw new BloomException("Not all mapped Memsql columns are present in the configured MemSQL columns table.. !", ErrorCodes.MBL_INPUT_VALIDATION_FAILED);
        }
    }

    /***
     * This method is used to create the schema for the input data fields. This method also validates that the input data all columns present in the table and not
     * any other column
     * @param inputFieldNames - the input data fields array
     * @return
     * @throws BloomException
     */
    private static StructType generateInputFieldsSchema(String[] inputFieldNames, MemSQLTableConfig memSQLTableConfig) throws BloomException {

        List<StructField> csvInputFields = new ArrayList();
        for (String inpuFieldName : inputFieldNames) {
            if (memSQLTableConfig.getMemSQLTablePrimaryKeyColumnNames().contains(String.valueOf(inpuFieldName))) {
                csvInputFields.add(DataTypes.createStructField(inpuFieldName, DataTypes.StringType, false));
            } else {
                csvInputFields.add(DataTypes.createStructField(inpuFieldName, DataTypes.StringType, true));
            }
        }
        return DataTypes.createStructType(csvInputFields);

    }


}
