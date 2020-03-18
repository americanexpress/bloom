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

package com.aexp.blaze.bloom.dao;

import com.aexp.blaze.bloom.config.AppConfig;
import com.aexp.blaze.bloom.constants.ErrorCodes;
import com.aexp.blaze.bloom.exceptions.BloomException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Java based DAO layer to talk to MemSQL
 */
public class JavaMemSQLDao {

    private static final Logger LOGGER = LogManager.getLogger(JavaMemSQLDao.class);
    private AppConfig appConfig;
    private Connection connection;

    public void init(AppConfig appConfig) {
        this.appConfig = appConfig;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            this.connection = DriverManager.getConnection(
                    "jdbc:mysql://" + appConfig.getMemSQLHost() + ":" + appConfig.getMemSQLPort() + "/"
                            + appConfig.getMemSQLDefaultDatabase(), appConfig.getMemSQLUsername(),
                    appConfig.getMemSQLPassword());
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void save() throws BloomException, SQLException {
        try {
            executeUpsertStatements();
        } catch (SQLException e) {
            LOGGER.error("Transaction is getting rolled back !!!");
            this.connection.rollback();
            throw new BloomException("Failed in completing the columnstore load ", ErrorCodes.MBL_COLUMNSTORE_LOAD_FAILED, e);

        } finally {
            try {
                this.connection.setAutoCommit(true);
                deleteStagingTable();
            } catch (SQLException excep) {
                throw new BloomException("Failed in deleting the staging table for the columnstore load ", ErrorCodes.MBL_COLUMNSTORE_LOAD_FAILED, excep);
            }
        }
    }

    private void dropNetInsertTable() throws SQLException {

        String dropTableSQL = "DROP TABLE IF EXISTS NET_INSERT_TABLE";
        LOGGER.debug("SQL to drop the net insert table: " + dropTableSQL);
        Statement deleteOriginalTableStatement = this.connection.createStatement();
        deleteOriginalTableStatement.execute(dropTableSQL);
    }

    private void deleteAllFromOriginalTable() throws SQLException {
        //truncate the original table
        String deletedAllContentFromOriginalTableSQL = "DELETE FROM " + appConfig.getCommandLineConf().getTableName();
        LOGGER.debug("SQL to delete the original table: " + deletedAllContentFromOriginalTableSQL);
        Statement deleteOriginalTableStatement = this.connection.createStatement();
        deleteOriginalTableStatement.execute(deletedAllContentFromOriginalTableSQL);
    }

    private void loadALLFromStagingTable() throws SQLException {
        //load all the content of the staging table into the original table
        String insertIntoOriginalFromStagingSQL = "INSERT INTO " + appConfig.getCommandLineConf().getTableName() + " SELECT * FROM " + appConfig.getCommandLineConf().getTableName() + "_stage";
        LOGGER.debug("SQL to insert into the original table from staging table: " + insertIntoOriginalFromStagingSQL);
        Statement insertIntoOrginalTableStatement = this.connection.createStatement();
        insertIntoOrginalTableStatement.executeUpdate(insertIntoOriginalFromStagingSQL);
    }


    private void executeUpsertStatements() throws SQLException, BloomException {

        //create a temporary table(NET_INSERT_TABLE- rowstore table) which will be used to hold the data which has to be finally overwritten in the original table
        createNetInsertTable();
        //insert those records in the NET_INSERT_TABLE which are new and not present in the original table
        insertNewRecordsFromStagingTableInToNetInsertTable();
        //insert those records in the NET_INSERT_TABLE which are already present but has the last_modified_ts value greater or equal to the existing record
        insertLatestRecordsFromStagingTableToNetInsertTable();
        //insert those records in the NET_INSERT_TABLE which are only present in the original table
        insertRecordsFromOriginalInNetInsertTable();
        //insert the required records in Net_Insert_table by selecting the mandatory attributes from Delta and existing attributes from original table to form a complete record
        insertMandotoryUpdateRecordsFromStagingTableInToNetInsertTable();
        this.connection.setAutoCommit(false);
        //delete the records from the original table which has to be updated
        deleteAllFromOriginalTable();
        insertFromNetInsertTabeIntoOriginalTable();

    }

    private void createNetInsertTable() throws SQLException, BloomException {
        List<String> memSQLColumns = appConfig.getMemSQLTableConf().getMemSQLTableColumnNames();
        String memSQLColumnsCommaDelimeted = String.join(",", memSQLColumns);
        String createTempTableForUpsertSQL = "CREATE TEMPORARY TABLE NET_INSERT_TABLE SELECT * FROM " + appConfig.getCommandLineConf().getTableName() + " LIMIT 0";
        // String createTempTableForUpsertSQL = "CREATE TABLE NET_INSERT_TABLE LIKE "+appConfig.getCommandLineConf().getTableName();
        LOGGER.debug("SQL to create the temporary NET_INSERT_TABLE: " + createTempTableForUpsertSQL);
        Statement createTempTableStatement = null;

        createTempTableStatement = this.connection.createStatement();
        createTempTableStatement.execute(createTempTableForUpsertSQL);

    }

    private void insertNewRecordsFromStagingTableInToNetInsertTable() throws SQLException, BloomException {
        String selectNewRowsSQL = "SELECT a.* FROM " + appConfig.getCommandLineConf().getTableName() + "_stage a LEFT JOIN " + appConfig.getCommandLineConf().getTableName() + " b ON ";
        List<String> primaryKeyList = this.appConfig.getMemSQLTableConf().getMemSQLTablePrimaryKeyColumnNames();
        int count = 1;
        for (String primaryKey : primaryKeyList) {
            if (count != primaryKeyList.size()) {
                selectNewRowsSQL = selectNewRowsSQL
                        .concat("a." + primaryKey + " = b." + primaryKey + " and ");
            } else {
                selectNewRowsSQL = selectNewRowsSQL
                        .concat("a." + primaryKey + " = b." + primaryKey);
            }

        }
        selectNewRowsSQL = selectNewRowsSQL.concat(" where ");
        count = 1;
        for (String primaryKey : primaryKeyList) {
            if (count != primaryKeyList.size()) {
                selectNewRowsSQL = selectNewRowsSQL
                        .concat("b." + primaryKey + " IS NULL and ");
                count++;
            } else {
                selectNewRowsSQL = selectNewRowsSQL
                        .concat("b." + primaryKey + " IS NULL");
            }

        }

        //load the content of the staging table into the NET_INSERT_TABLE
        String insertIntoNetInsertTableFromStagingSQL = "INSERT INTO NET_INSERT_TABLE " + selectNewRowsSQL;
        LOGGER.debug("SQL to insert new rows into the NET_INSERT_TABLE table from staging table: " + insertIntoNetInsertTableFromStagingSQL);
        Statement insertIntoNetInsertTableStatement = this.connection.createStatement();
        insertIntoNetInsertTableStatement.executeUpdate(insertIntoNetInsertTableFromStagingSQL);

    }

    private void insertMandotoryUpdateRecordsFromStagingTableInToNetInsertTable() throws SQLException, BloomException {
        List<String> memSQLColumns = appConfig.getMemSQLTableConf().getMemSQLTableColumnNames();
        List<String> mandatoryUpdateColumns = appConfig.getMemSQLTableConf().getMandatoryUpdateColumnNames();
        String selectQuery;
        if (mandatoryUpdateColumns != null && mandatoryUpdateColumns.size() > 0) {
            memSQLColumns.removeAll(mandatoryUpdateColumns);
            List<String> memSQLColumnsWithAliasPrefix = new ArrayList<String>();
            List<String> mandatoryUpdateColumnsWithAliasPrefix = new ArrayList<String>();
            for (String col : memSQLColumns) {
                memSQLColumnsWithAliasPrefix.add("b." + col);
            }
            for (String col : mandatoryUpdateColumns) {
                mandatoryUpdateColumnsWithAliasPrefix.add("a." + col);
            }
            String mandatoryColumns = String.join(",", mandatoryUpdateColumnsWithAliasPrefix);
            String notUpdatingColumns = String.join(",", memSQLColumnsWithAliasPrefix);
            selectQuery = "SELECT " + mandatoryColumns + "," + notUpdatingColumns + " FROM " + appConfig.getCommandLineConf().getTableName() + "_stage a," + appConfig.getCommandLineConf().getTableName() + " b where ";
            for (String primaryKey : this.appConfig.getMemSQLTableConf().getMemSQLTablePrimaryKeyColumnNames()) {
                selectQuery = selectQuery
                        .concat("a." + primaryKey + " = b." + primaryKey + " and ");
            }
            selectQuery = selectQuery
                    .concat("cast(a." + this.appConfig.getMemSQLTableConf().getMemSQLTableTimestampColumnName() + " as datetime(6)) < cast(b." + this.appConfig.getMemSQLTableConf().getMemSQLTableTimestampColumnName() + " as datetime(6))");

        } else {
            List<String> memSQLColumnsWithAliasPrefix = new ArrayList<String>();
            for (String col : memSQLColumns) {
                memSQLColumnsWithAliasPrefix.add("b." + col);
            }
            selectQuery = "SELECT " + String.join(",", memSQLColumnsWithAliasPrefix) + " FROM " + appConfig.getCommandLineConf().getTableName() + "_stage a," + appConfig.getCommandLineConf().getTableName() + " b where ";
            for (String primaryKey : this.appConfig.getMemSQLTableConf().getMemSQLTablePrimaryKeyColumnNames()) {
                selectQuery = selectQuery
                        .concat("a." + primaryKey + " = b." + primaryKey + " and ");
            }
            selectQuery = selectQuery
                    .concat("cast(a." + this.appConfig.getMemSQLTableConf().getMemSQLTableTimestampColumnName() + " as datetime(6)) < cast(b." + this.appConfig.getMemSQLTableConf().getMemSQLTableTimestampColumnName() + " as datetime(6))");

        }
        LOGGER.debug("SQL to select the mandatory data: " + selectQuery);
        Statement insertIntoNetInsertTableStatement = this.connection.createStatement();
        String insertQuery;
        if (mandatoryUpdateColumns != null)
            insertQuery = "Insert into NET_INSERT_TABLE (" +
                    String.join(",", mandatoryUpdateColumns) + "," + String.join(",", memSQLColumns) + ") " + selectQuery;
        else
            insertQuery = "Insert into NET_INSERT_TABLE (" + String.join(",", memSQLColumns) + ") " + selectQuery;
        LOGGER.debug("insertQuery: " + insertQuery);
        insertIntoNetInsertTableStatement.executeUpdate(insertQuery);

    }

    private void insertLatestRecordsFromStagingTableToNetInsertTable() throws SQLException, BloomException {
        //form the select query used to fetch the delta load data, by comparing the staging table and orginal table based on last modified timestamp
        String selectUpsertRowsSQL = "SELECT distinct a.* FROM " + appConfig.getCommandLineConf().getTableName() + "_stage a," + appConfig.getCommandLineConf().getTableName() + " b where ";
        for (String primaryKey : this.appConfig.getMemSQLTableConf().getMemSQLTablePrimaryKeyColumnNames()) {
            selectUpsertRowsSQL = selectUpsertRowsSQL
                    .concat("a." + primaryKey + " = b." + primaryKey + " and ");
        }
        selectUpsertRowsSQL = selectUpsertRowsSQL
                .concat("cast(a." + this.appConfig.getMemSQLTableConf().getMemSQLTableTimestampColumnName() + " as datetime(6)) >= cast(b." + this.appConfig.getMemSQLTableConf().getMemSQLTableTimestampColumnName() + " as datetime(6))");
        LOGGER.debug("SQL to select the latest data from staging table: " + selectUpsertRowsSQL);


        //use the created select query to load latest data into the NET_INSERT_TABLE
        Statement insertUpsertIntoNetInsertTableStatement = this.connection.createStatement();
        insertUpsertIntoNetInsertTableStatement.executeUpdate("Insert into NET_INSERT_TABLE " + selectUpsertRowsSQL);
    }


    private void insertRecordsFromOriginalInNetInsertTable() throws SQLException, BloomException {
        String selectNewRowsSQL = "SELECT a.* FROM " + appConfig.getCommandLineConf().getTableName() + " a LEFT JOIN " + appConfig.getCommandLineConf().getTableName() + "_stage b ON ";
        List<String> primaryKeyList = this.appConfig.getMemSQLTableConf().getMemSQLTablePrimaryKeyColumnNames();
        int count = 1;
        for (String primaryKey : primaryKeyList) {
            if (count != primaryKeyList.size()) {
                selectNewRowsSQL = selectNewRowsSQL
                        .concat("a." + primaryKey + " = b." + primaryKey + " and ");
            } else {
                selectNewRowsSQL = selectNewRowsSQL
                        .concat("a." + primaryKey + " = b." + primaryKey);
            }

        }
        selectNewRowsSQL = selectNewRowsSQL.concat(" where ");
        count = 1;
        for (String primaryKey : primaryKeyList) {
            if (count != primaryKeyList.size()) {
                selectNewRowsSQL = selectNewRowsSQL
                        .concat("b." + primaryKey + " IS NULL and ");
                count++;
            } else {
                selectNewRowsSQL = selectNewRowsSQL
                        .concat("b." + primaryKey + " IS NULL");
            }

        }

        //load the content of the staging table into the NET_INSERT_TABLE
        String insertIntoNetInsertTableFromStagingSQL = "INSERT INTO NET_INSERT_TABLE " + selectNewRowsSQL;
        LOGGER.debug("SQL to insert new rows into the OLDER_TABLE table from original table: " + insertIntoNetInsertTableFromStagingSQL);
        Statement insertIntoNetInsertTableStatement = this.connection.createStatement();
        insertIntoNetInsertTableStatement.executeUpdate(insertIntoNetInsertTableFromStagingSQL);

    }

    private void insertFromNetInsertTabeIntoOriginalTable() throws SQLException {
        Statement insertFromNetInsertIntoOrginalTableStatement = this.connection.createStatement();
        insertFromNetInsertIntoOrginalTableStatement.executeUpdate("INSERT INTO " + appConfig.getCommandLineConf().getTableName() + " SELECT * FROM NET_INSERT_TABLE");
    }

    public void deleteStagingTable() {
        try {
            Statement deleteStagingTableStatement = this.connection.createStatement();
            String deleteQuery = "DELETE FROM " + appConfig.getCommandLineConf().getTableName() + "_stage";
            LOGGER.debug("SQL to delete the staging table: " + deleteQuery);
            deleteStagingTableStatement.execute(deleteQuery);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void deleteOriginalTable() {
        try {
            Statement deleteOriginalTableStatement = this.connection.createStatement();
            String deleteQuery = "DELETE FROM " + appConfig.getCommandLineConf().getTableName();
            LOGGER.debug("SQL to delete the original table: " + deleteQuery);
            deleteOriginalTableStatement.execute(deleteQuery);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<String> getMemSQLTableColNames() {
        try {
            Statement selectColsTableStatement = this.connection.createStatement();
            String selectQuery = "SELECT * FROM " + appConfig.getCommandLineConf().getTableName() + " LIMIT 0";
            ResultSet rs = selectColsTableStatement.executeQuery(selectQuery);
            ResultSetMetaData rsmd = rs.getMetaData();
            List<String> memSQLColList = new ArrayList<String>();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                memSQLColList.add(rsmd.getColumnName(i));
            }
            return memSQLColList;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
