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

package com.americanexpress.bloom.config;

import com.americanexpress.bloom.constants.Constants;
import com.americanexpress.bloom.exceptions.BloomException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MemSQLTableConfigTest {

    private MemSQLTableConfig memSQLTableConfig;

    @Before
    public void setUp() throws BloomException {
        System.setProperty(Constants.APP_HOME, this.getClass().getClassLoader().getResource("").getPath());
        String[] commandLineArgs = {"-tn", "date_dim", "-ip", "/app/bloom/DEMO/date_dim_ts_5MM.csv", "-rt", "FULL-REFRESH", "-it", "csv", "-tt", "columnstore"};
        CommandLineConfig commandLineConfig = new CommandLineConfig(commandLineArgs);
        this.memSQLTableConfig = new MemSQLTableConfig(commandLineConfig);
    }

    @Test
    public void testTableName() throws BloomException {
        Assert.assertEquals("date_dim", memSQLTableConfig.getMemSQLTableName());
    }

    @Test
    public void testMandatoryUpdateColumnNames() throws BloomException {
        this.memSQLTableConfig.getMemSQLTableColumnNames();
        List<String> expected = Arrays.asList("dayofweek");
        Assert.assertEquals("dayofweek", this.memSQLTableConfig.getMandatoryUpdateColumnNames().get(0));
    }
}
