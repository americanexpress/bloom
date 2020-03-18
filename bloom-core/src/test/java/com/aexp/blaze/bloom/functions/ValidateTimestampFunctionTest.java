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

package com.aexp.blaze.bloom.functions;

import com.aexp.blaze.bloom.config.AppConfig;
import com.aexp.blaze.bloom.config.CommandLineConfig;
import com.aexp.blaze.bloom.config.MemSQLTableConfig;
import com.aexp.blaze.bloom.exceptions.BloomException;
import com.aexp.blaze.bloom.schema.EntitySchema;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.mockito.Mockito.*;

public class ValidateTimestampFunctionTest {
    CommandLineConfig commandLineConfig = mock(CommandLineConfig.class);
    MemSQLTableConfig memSQLTableConfig = mock(MemSQLTableConfig.class);
    AppConfig appConfig = mock(AppConfig.class);
    EntitySchema entitySchema = mock(EntitySchema.class);
    Connection connection = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    ResultSet rs = mock(ResultSet.class);
    Row row = mock(Row.class);

    @Test
    public void testValidateTimestampFunctionNullResultSet() throws SQLException, BloomException, ParseException {
        when(appConfig.getCommandLineConf()).thenReturn(commandLineConfig);


        when(entitySchema.getTableName()).thenReturn("sample_table");
        List<String> primaryCols = new ArrayList<>();
        primaryCols.add("ID");


        when(connection.createStatement()).thenReturn(stmt);

        when(stmt.executeQuery("SELECT * from sample_table as t1 where t1.ID = \"ABC_123\" and cast(t1.lastmodifieddate as datetime(6))  > cast('2' as datetime(6))")).thenReturn(null);

        when(row.getAs("ID")).thenReturn("ABC_123");
        when(row.getAs("lastmodifieddate")).thenReturn(2);
        when(appConfig.getMemSQLTableConf()).thenReturn(memSQLTableConfig);
        when(commandLineConfig.getTableName()).thenReturn("sample_table");
        when(memSQLTableConfig.getMemSQLTablePrimaryKeyColumnNames()).thenReturn(primaryCols);
        when(memSQLTableConfig.getMemSQLTableTimestampColumnName()).thenReturn("lastmodifieddate");
        //Invoking actual test method
        ValidateTimestampFunction validateTimestamp = new ValidateTimestampFunction(entitySchema, appConfig);
        validateTimestamp.setConnection(connection);

        List<Row> rows = new ArrayList<>();
        rows.add(row);
        Iterator<Row> rowIterator = validateTimestamp.call(rows.iterator());
        Assert.assertFalse(rowIterator.hasNext());

        verify(stmt, times(1)).executeQuery("SELECT * from sample_table as t1 where t1.ID = \"ABC_123\" and cast(t1.lastmodifieddate as datetime(6))  > cast('2' as datetime(6))");
        verify(connection, times(1)).close();
    }

    @Test
    public void testValidateTimestampFunctionInsert() throws SQLException, BloomException, ParseException {
        List<String> primaryCols = new ArrayList<>();
        primaryCols.add("ID");

        when(connection.createStatement()).thenReturn(stmt);

        List<StructField> inputFields = new ArrayList();
        inputFields.add(DataTypes.createStructField("ID", DataTypes.StringType, true));
        inputFields.add(DataTypes.createStructField("lastmodifieddate", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(inputFields);
        when(entitySchema.getDtoFieldsSchema()).thenReturn(schema);

        when(row.getAs("ID")).thenReturn("ABC_123");
        when(row.getAs("lastmodifieddate")).thenReturn(2);
        when(stmt.executeQuery("SELECT * from sample_table as t1 where t1.ID = \"ABC_123\" and cast(t1.lastmodifieddate as datetime(6))  > cast('2' as datetime(6))")).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        when(appConfig.getMemSQLTableConf()).thenReturn(memSQLTableConfig);
        when(appConfig.getCommandLineConf()).thenReturn(commandLineConfig);
        when(commandLineConfig.getTableName()).thenReturn("sample_table");
        when(memSQLTableConfig.getMemSQLTablePrimaryKeyColumnNames()).thenReturn(primaryCols);
        when(memSQLTableConfig.getMemSQLTableTimestampColumnName()).thenReturn("lastmodifieddate");
        List<String> manCols = new ArrayList<>();
        manCols.add("test");
        when(memSQLTableConfig.getMandatoryUpdateColumnNames()).thenReturn(manCols);
        //Invoking actual test method
        ValidateTimestampFunction validateTimestamp = new ValidateTimestampFunction(entitySchema, appConfig);
        validateTimestamp.setConnection(connection);

        List<Row> rows = new ArrayList<>();
        rows.add(row);
        when(row.schema()).thenReturn(schema);
        Iterator<Row> rowIterator = validateTimestamp.call(rows.iterator());
        Assert.assertTrue(rowIterator.hasNext());

        verify(stmt, times(1)).executeQuery("SELECT * from sample_table as t1 where t1.ID = \"ABC_123\" and cast(t1.lastmodifieddate as datetime(6))  > cast('2' as datetime(6))");
        verify(connection, times(1)).close();

    }

}
