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

package com.americanexpress.bloom.dao;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/***
 * This class represents the spark MemSQL Dao used to read and write data MemSQL using spark
 * MemSQLConnector
 */
public class SparkMemSQLDao {

    private SparkSession sparkSession;

    public SparkMemSQLDao(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public void save(Dataset dataset, String tableName, String saveMode) {
        dataset.write().format("com.memsql.spark.connector").mode(saveMode).save(tableName);
    }
}
