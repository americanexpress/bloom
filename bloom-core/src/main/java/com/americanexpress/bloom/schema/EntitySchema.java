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

import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.List;

/***
 * This class represents the MemSQL table which the user is trying to load the data into.
 */
public class EntitySchema implements Serializable {


    private String tableName;
    private List<String> primaryKeyColumnNames;
    private StructType dtoFieldsSchema;
    private StructType inputFieldsSchema;
    private String timestampColumnName;
    private List<String> mandatoryUpdateColumnNames;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getPrimaryKeyColumnNames() {
        return primaryKeyColumnNames;
    }

    public void setPrimaryKeyColumnNames(List<String> primaryKeyColumnNames) {
        this.primaryKeyColumnNames = primaryKeyColumnNames;
    }

    public String getTimestampColumnName() {
        return timestampColumnName;
    }

    public void setTimestampColumnName(String timestampColumnName) {
        this.timestampColumnName = timestampColumnName;
    }

    public StructType getDtoFieldsSchema() {
        return dtoFieldsSchema;
    }

    public void setDtoFieldsSchema(StructType dtoFieldsSchema) {
        this.dtoFieldsSchema = dtoFieldsSchema;
    }

    public StructType getInputFieldsSchema() {
        return inputFieldsSchema;
    }

    public void setInputFieldsSchema(StructType inputFieldsSchema) {
        this.inputFieldsSchema = inputFieldsSchema;
    }

    public List<String> getMandatoryUpdateColumnNames() {
        return mandatoryUpdateColumnNames;
    }

    public void setMandatoryUpdateColumnNames(List<String> mandatoryUpdateColumnNames) {
        this.mandatoryUpdateColumnNames = mandatoryUpdateColumnNames;
    }


}