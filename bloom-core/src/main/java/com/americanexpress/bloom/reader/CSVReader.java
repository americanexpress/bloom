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

package com.americanexpress.bloom.reader;

import com.americanexpress.bloom.exceptions.BloomException;
import com.americanexpress.bloom.schema.EntitySchema;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to read data from csv
 */
public class CSVReader extends Reader {
    private static final Logger LOGGER = LoggerFactory.getLogger(CSVReader.class);

    /***
     * This method is used to read the data from csv with proper delimiter and it
     * fails for any malformed record found
     * @param entitySchema
     * @return
     */
    @Override
    public Dataset<Row> read(EntitySchema entitySchema) {
        LOGGER.info("reading data from csv file ");
        final DataFrameReader dataFrameReader = sparkSession.read();
        if (this.appConfig.getCSVDelimiter().equalsIgnoreCase(",")) {
            dataFrameReader
                    .option("header", "true")
                    .option("mode", "FAILFAST")
                    .option("inferSchema", "false")
                    .option("quote", "\"")
                    .option("delimiter", this.appConfig.getCSVDelimiter())
                    .option("escape", "\"")
                    .schema(entitySchema.getInputFieldsSchema());

        } else {
            dataFrameReader
                    .option("header", "true")
                    .option("mode", "FAILFAST")
                    .option("quote", "\u0003")
                    .option("inferSchema", "false")
                    .option("delimiter", this.appConfig.getCSVDelimiter());
        }
        return dataFrameReader.csv(this.appConfig.getCommandLineConf().getInputLocation());
    }

    /***
     * This method is used to read the first row from the csv, which can later be used to validate if the headers are proper and if they contain the mandatory columns or not
     * @return
     * @throws BloomException
     */
    @Override
    public Dataset<Row> readFirstRow() throws BloomException {
        LOGGER.info("reading first row from the csv file at path {}", this.appConfig.getCommandLineConf().getInputLocation());
        final DataFrameReader dataFrameReader = sparkSession.read();
        if (this.appConfig.getCSVDelimiter().equalsIgnoreCase(",")) {
            dataFrameReader.option("header", "true").option("inferSchema", "false").option("escape", "\"").option("quote", "\"").option("delimiter", this.appConfig.getCSVDelimiter());

        } else {
            dataFrameReader.option("header", "true").option("inferSchema", "false").option("quote", "").option("delimiter", this.appConfig.getCSVDelimiter());
        }
        Dataset<Row> csvDataFrame = dataFrameReader.csv(this.appConfig.getCommandLineConf().getInputLocation());
        if (!csvDataFrame.toJavaRDD().isEmpty())
            return csvDataFrame;
        else {
            throw new BloomException("Data is not valid: Header/Data absent!!!");
        }
    }
}
