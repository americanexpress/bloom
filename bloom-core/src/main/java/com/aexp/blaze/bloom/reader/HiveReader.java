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

package com.aexp.blaze.bloom.reader;

import com.aexp.blaze.bloom.exceptions.BloomException;
import com.aexp.blaze.bloom.schema.EntitySchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to read data from hive tables
 */
public class HiveReader extends Reader {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveReader.class);
    private static final String HIVE_SELECT_FULL_DATA_SQL = "SELECT %s FROM %s";

    /***
     * This method is used to read the data from csv with proper delimiter and it
     * fails for any malformed record found
     * @return
     */
    @Override
    public Dataset<Row> read(EntitySchema entitySchema) throws BloomException {

        String hiveTableName = this.appConfig.getCommandLineConf().getInputLocation();
        String[] columnName = this.appConfig.getMemSQLTableConf().getHiveTableColumnNames().toArray(new String[this.appConfig.getMemSQLTableConf().getHiveTableColumnNames().size()]);

        String sql = String.format(HIVE_SELECT_FULL_DATA_SQL, String.join(",", columnName), hiveTableName);
        LOGGER.info("Executing FULL_REFRESH/UPSERT/LOAD-APPEND SQL : {}", sql);
        Dataset<Row> dataDF = sparkSession.sql(sql).cache();
        for (int fieldIndex = 0; fieldIndex < this.appConfig.getMemSQLTableConf().getInputMemSQLTableColumnNames().size(); fieldIndex++) {

            dataDF = dataDF.withColumnRenamed(this.appConfig.getMemSQLTableConf().getHiveTableColumnNames().get(fieldIndex), this.appConfig.getMemSQLTableConf().getInputMemSQLTableColumnNames().get(fieldIndex));
        }
        return dataDF;
    }

    /***
     * This method is used to get the hive column names for the configured hive table
     * @return
     * @throws BloomException
     */
    @Override
    public Dataset<Row> readFirstRow() {
        Dataset<Row> columnDataSet = sparkSession.sql(String.format("SHOW COLUMNS IN %s", this.appConfig.getCommandLineConf().getInputLocation()));
        columnDataSet.show();
        return columnDataSet;
    }


}
