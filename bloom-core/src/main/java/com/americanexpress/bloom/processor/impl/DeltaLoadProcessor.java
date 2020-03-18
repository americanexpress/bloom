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

package com.americanexpress.bloom.processor.impl;

import com.americanexpress.bloom.config.AppConfig;
import com.americanexpress.bloom.exceptions.BloomException;
import com.americanexpress.bloom.functions.ValidateTimestampFunction;
import com.americanexpress.bloom.processor.Processor;
import com.americanexpress.bloom.schema.EntitySchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;

/***
 * This class is used for refresh type -UPSERT. It validates the timestamp of
 * the incoming record and acordingly create the final datase which can be later saved
 */
public class DeltaLoadProcessor extends Processor {

    private static final Logger LOGGER = LogManager.getLogger(DeltaLoadProcessor.class);

    public DeltaLoadProcessor(SparkSession sparkSession) {
        super(sparkSession);
    }

    @Override
    public Dataset<Row> getProcessedDataSet(Dataset<Row> inputDataset, EntitySchema entitySchema, AppConfig appConfig) throws BloomException {
        ValidateTimestampFunction validationTimestampFunction = new ValidateTimestampFunction(entitySchema, appConfig);
        return inputDataset.mapPartitions(validationTimestampFunction, RowEncoder.apply(entitySchema.getDtoFieldsSchema()));
    }


}

