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

package com.aexp.blaze.bloom.processor.impl;

import com.aexp.blaze.bloom.config.AppConfig;
import com.aexp.blaze.bloom.processor.Processor;
import com.aexp.blaze.bloom.schema.EntitySchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/***
 * This class is used to return the processed data for refresh type -FULL-REFRESH
 */
public class BulkLoadProcessor extends Processor {
    public BulkLoadProcessor(SparkSession sparkSession) {
        super(sparkSession);
    }

    @Override
    public Dataset<Row> getProcessedDataSet(Dataset<Row> inputDataSet, EntitySchema entitySchema, AppConfig appConfig) {
        return inputDataSet;
    }
}
