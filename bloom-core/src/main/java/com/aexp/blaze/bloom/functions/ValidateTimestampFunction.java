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
import com.aexp.blaze.bloom.constants.ErrorCodes;
import com.aexp.blaze.bloom.exceptions.BloomException;
import com.aexp.blaze.bloom.schema.EntitySchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;

import java.sql.*;
import java.text.ParseException;
import java.util.*;

/**
 * This map partition function is used for UPSERT refresh type and is used to
 * validate the timestamp of the incoming record and accordingly create the
 * final dataset which can be later saved
 */
public class ValidateTimestampFunction implements MapPartitionsFunction<Row, Row> {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger(ValidateTimestampFunction.class);
    private EntitySchema entitySchema;
    private AppConfig appConfig;
    private boolean recordAlreadyPresent = false;
    private Connection connection;
    private String deleteQuery;
    private String tableName;
    private List<String> primaryKeyColumnNames;
    private String timestampColumnName;

    public ValidateTimestampFunction(EntitySchema entitySchema, AppConfig appConfig) throws BloomException {
        this.entitySchema = entitySchema;
        this.appConfig = appConfig;
        this.tableName = this.appConfig.getCommandLineConf().getTableName();
        this.primaryKeyColumnNames = this.appConfig.getMemSQLTableConf().getMemSQLTablePrimaryKeyColumnNames();
        this.timestampColumnName = this.appConfig.getMemSQLTableConf().getMemSQLTableTimestampColumnName();
    }

    @Override
    public Iterator<Row> call(Iterator<Row> iterator) throws BloomException, ParseException {

        Statement stmt;
        Statement deleteStmt;

        ResultSet rs = null;
        recordAlreadyPresent = false;

        try {
            connection = connection == null ? getMemSQLConnection() : connection;
            stmt = connection.createStatement();
            deleteStmt = connection.createStatement();
        } catch (Exception e) {
            throw new BloomException("Failed in MemSQL ", ErrorCodes.MBL_MEMSQL_JDBC_FAILED, e);
        }

        ArrayList<Row> outRowList = new ArrayList<>();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            String selectQuery = constructSelectQuery(row);
            deleteQuery = constructDeleteQuery(row);
            if (stmt != null) {
                try {
                    rs = stmt.executeQuery(selectQuery);
                } catch (SQLException e) {
                    throw new BloomException("Failed to execute the statements ", ErrorCodes.MBL_MEMSQL_JDBC_FAILED, e);

                }
            }

            if (rs != null) {
                try {
                    Row updatedRow = constructUpdatedRow(rs, row, entitySchema, deleteStmt);
                    outRowList.add(updatedRow);
                } catch (SQLException e) {
                    throw new BloomException("Failed to iterate the result set ", ErrorCodes.MBL_MEMSQL_JDBC_FAILED, e);
                }
            }

        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {

            }
        }
        return outRowList.iterator();

    }

    /***
     * This method is used to construct the delete query which can be used delete a record  if the same record is already present in MemSQL
     * @param row - row from the input dataset
     * @return
     */
    private String constructDeleteQuery(Row row) {

        String deleteQuery = "DELETE from " + this.tableName + " as t1 where ";
        int count = 0;
        for (String primaryKey : this.primaryKeyColumnNames) {
            count = count + 1;
            if (count != this.primaryKeyColumnNames.size())
                deleteQuery = deleteQuery
                        .concat("t1." + primaryKey + " = " + "\"" + row.getAs(primaryKey) + "\"" + " and ");
            else {
                deleteQuery = deleteQuery.concat("t1." + primaryKey + " = " + "\"" + row.getAs(primaryKey) + "\"");
            }
        }
        return deleteQuery;
    }

    /***
     * This method is used to construct the select query which can be used check if the same record is already present in MemSQL
     * @param row - row from the input dataset
     * @return
     */
    private String constructSelectQuery(Row row) throws BloomException {

        String selectQuery = "SELECT * from " + this.tableName + " as t1 where ";
        int count = 0;
        for (String primaryKey : this.primaryKeyColumnNames) {
            count = count + 1;
            if (count != this.primaryKeyColumnNames.size())
                selectQuery = selectQuery
                        .concat("t1." + primaryKey + " = " + "\"" + row.getAs(primaryKey) + "\"" + " and ");
            else {
                selectQuery = selectQuery.concat("t1." + primaryKey + " = " + "\"" + row.getAs(primaryKey) + "\""
                        + " and cast(t1." + this.timestampColumnName + " as datetime(6))  > cast('"
                        + row.getAs(this.timestampColumnName) + "' as datetime(6))");
            }
        }
        LOGGER.debug("select query is " + selectQuery);
        return selectQuery;
    }

    /***
     * This method is used to create Memsql connection
     * @return
     * @throws BloomException
     */
    private Connection getMemSQLConnection() throws BloomException {
        Connection connection = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new BloomException("Failed to load jdbc Driver", ErrorCodes.MBL_MEMSQL_DRIVER_LOAD_FAILED, e);
        }

        try {
            connection = DriverManager.getConnection(
                    "jdbc:mysql://" + this.appConfig.getMemSQLHost() + ":" + this.appConfig.getMemSQLPort() + "/"
                            + this.appConfig.getMemSQLDefaultDatabase(),
                    this.appConfig.getMemSQLUsername(), this.appConfig.getMemSQLPassword());
        } catch (SQLException e) {
            throw new BloomException("Failed in get the MemSQL Conection", ErrorCodes.MBL_MEMSQL_CONNECTION_FAILED, e);
        }
        return connection;
    }

    /***
     * This method uses the resultset to create a database hashmap, uses incoming row to create a file hashmap and then finally overwrites the csv hashmap over database hashmap
     * and forms the final hashmap
     * @param rs
     * @param row
     * @param entitySchema
     * @param deleteStmt
     * @return
     * @throws SQLException
     * @throws ParseException
     */
    private Row constructUpdatedRow(ResultSet rs, Row row, EntitySchema entitySchema, Statement deleteStmt) throws SQLException, ParseException, BloomException {
        //form a hashmap from the existing record
        LinkedHashMap<String, String> databaseRecordHashMap = databaseRecordToHashMap(rs, entitySchema, deleteStmt);

        //form a hashmap from the incoming record
        LinkedHashMap<String, String> fileHashMap = new LinkedHashMap();
        for (StructField field : row.schema().fields()) {
            if (row.get(row.fieldIndex(field.name())) == null || String.valueOf(row.get(row.fieldIndex(field.name()))).isEmpty())
                fileHashMap.put(field.name(), null);
            else
                fileHashMap.put(field.name(), String.valueOf(row.get(row.fieldIndex(field.name()))));

        }
        LinkedHashMap<String, String> finalHashMap = formFinalHashMap(databaseRecordHashMap, fileHashMap);

        Object[] outColumns = new Object[finalHashMap.size()];
        Collection c = finalHashMap.values();

        Iterator itr = c.iterator();
        int i = 0;
        while (itr.hasNext()) {
            outColumns[i] = itr.next();
            i++;
        }
        return RowFactory.create(outColumns);
    }

    /***
     * This method is used to create the final hashmap by overwriting the incoming csv values on the database record values.The final hashmap is then used to create the
     * outgoing row
     * @param databaseRecordHashMap
     * @param fileHashMap
     * @return
     */
    private LinkedHashMap<String, String> formFinalHashMap(LinkedHashMap<String, String> databaseRecordHashMap,
                                                           LinkedHashMap<String, String> fileHashMap
    ) throws BloomException {
        if (!recordAlreadyPresent) {
            for (Map.Entry<String, String> entry : fileHashMap.entrySet()) {
                databaseRecordHashMap.put(entry.getKey(), entry.getValue());
            }
        }
        if (this.appConfig.getMemSQLTableConf().getMandatoryUpdateColumnNames() != null) {
            List<String> mandatoryUpdateColumnNames = this.appConfig.getMemSQLTableConf().getMandatoryUpdateColumnNames();
            for (String colName : mandatoryUpdateColumnNames) {
                databaseRecordHashMap.put(colName, fileHashMap.get(colName));
            }
        }
        return databaseRecordHashMap;

    }

    /***
     * This method is used to compare convert the read resultset into a hashmap.
     * @param rs
     * @param entitySchema
     * @param deleteStmt
     * @return
     * @throws SQLException
     */
    private LinkedHashMap<String, String> databaseRecordToHashMap(ResultSet rs, EntitySchema entitySchema, Statement deleteStmt) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columns = entitySchema.getDtoFieldsSchema().fieldNames().length;
        LinkedHashMap<String, String> databaseRecordHashMap = new LinkedHashMap();
        if (rs.next() == false) {
            recordAlreadyPresent = false;
            for (int fieldsIndex = 0; fieldsIndex < columns; fieldsIndex++) {
                databaseRecordHashMap.put(entitySchema.getDtoFieldsSchema().fieldNames()[fieldsIndex], null);
            }

        } else {
            recordAlreadyPresent = true;
            if (this.appConfig.getCommandLineConf().getMemSQLTableType().equalsIgnoreCase("columnstore")) {
                LOGGER.info("deleteQuery query is" + deleteQuery);
                deleteStmt.executeUpdate(deleteQuery);
            }
            do {
                for (int fieldsIndex = 0; fieldsIndex < columns; fieldsIndex++) {
                    databaseRecordHashMap.put(entitySchema.getDtoFieldsSchema().fieldNames()[fieldsIndex], rs.getString(entitySchema.getDtoFieldsSchema().fieldNames()[fieldsIndex]));
                }
            } while (rs.next());
        }
        return databaseRecordHashMap;
    }

    /**
     * This method is only used during unt test.
     * During application execution, connection object is created using getMemSQLConnection()
     *
     * @param connection - setting connection object
     */
    public void setConnection(Connection connection) {
        this.connection = connection;
    }


}
