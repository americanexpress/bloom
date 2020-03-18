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
import com.americanexpress.bloom.constants.Constants;
import com.americanexpress.bloom.exceptions.BloomException;
import org.junit.Assert;
import org.junit.Test;

public class SchemaGeneratorTest {

    @Test
    public void testTableName() throws BloomException {
        String[] commandLineArgs = {"-tn", "contact", "-ip", "/app/bloom/DEMO/fact_columnstore_ts_5MM.csv", "-rt", "FULL-REFRESH", "-it", "csv", "-tt", "columnstore"};
        AppConfig appConfig = new AppConfig(commandLineArgs);
        // CommandLineConfig commandLineConfig = new CommandLineConfig(commandLineArgs);
        System.setProperty(Constants.APP_HOME, "\\bloom-core\\src\\main");

        // MemSQLTableConfig memSQLTableConfig = new MemSQLTableConfig(commandLineConfig);
        EntitySchema entitySchema = SchemaGenerator
                .generateEntitySchema(appConfig,
                        new String[]{"bu_in", "id", "lastmodifieddate", "accountid", "lastname", "firstname"});
        Assert.assertEquals("Table name dose not match with schema POJO.",
                entitySchema.getTableName(), "contact");
    }

    @Test
    public void testTimestampColumnName() throws BloomException {
        String[] commandLineArgs = {"-tn", "contact", "-ip", "/app/bloom/DEMO/fact_columnstore_ts_5MM.csv", "-rt", "FULL-REFRESH", "-it", "csv", "-tt", "columnstore"};
        AppConfig appConfig = new AppConfig(commandLineArgs);
        //CommandLineConfig commandLineConfig = new CommandLineConfig(commandLineArgs);
        System.setProperty(Constants.APP_HOME, "\\bloom-core\\src\\main");

        //MemSQLTableConfig memSQLTableConfig = new MemSQLTableConfig(commandLineConfig);
        EntitySchema entitySchema = SchemaGenerator
                .generateEntitySchema(appConfig,
                        new String[]{"bu_in", "id", "lastmodifieddate", "accountid", "lastname", "firstname"});
        Assert.assertEquals("Timestamp column does not matches.",
                entitySchema.getTimestampColumnName(), "lastmodifieddate");
    }

    @Test
    public void testPrimaryKeyColumnNames() throws BloomException {
        String[] commandLineArgs = {"-tn", "contact", "-ip", "/app/bloom/DEMO/fact_columnstore_ts_5MM.csv", "-rt", "FULL-REFRESH", "-it", "csv", "-tt", "columnstore"};
        AppConfig appConfig = new AppConfig(commandLineArgs);
        //CommandLineConfig commandLineConfig = new CommandLineConfig(commandLineArgs);
        System.setProperty(Constants.APP_HOME, "\\bloom-core\\src\\main");

        //MemSQLTableConfig memSQLTableConfig = new MemSQLTableConfig(commandLineConfig);
        EntitySchema entitySchema = SchemaGenerator
                .generateEntitySchema(appConfig,
                        new String[]{"bu_in", "id", "lastmodifieddate", "accountid", "lastname", "firstname"});
        Assert.assertTrue(entitySchema.getPrimaryKeyColumnNames().contains("bu_in"));
        Assert.assertTrue(entitySchema.getPrimaryKeyColumnNames().contains("id"));
    }

    @Test
    public void testInputFieldSchema() throws BloomException {
        String[] commandLineArgs = {"-tn", "contact", "-ip", "/app/bloom/DEMO/fact_columnstore_ts_5MM.csv", "-rt", "FULL-REFRESH", "-it", "csv", "-tt", "columnstore"};
        AppConfig appConfig = new AppConfig(commandLineArgs);
        // CommandLineConfig commandLineConfig = new CommandLineConfig(commandLineArgs);
        System.setProperty(Constants.APP_HOME, "\\bloom-core\\src\\main");

        // MemSQLTableConfig memSQLTableConfig = new MemSQLTableConfig(commandLineConfig);
        EntitySchema entitySchema = SchemaGenerator
                .generateEntitySchema(appConfig,
                        new String[]{"bu_in", "id", "lastmodifieddate", "accountid", "lastname", "firstname"});
        Assert.assertEquals("InputFieldsSchema does not all the input fields.", entitySchema.getInputFieldsSchema().fields().length, 6);
    }


}
