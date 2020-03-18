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

package com.americanexpress.bloom.constants;

/**
 * Created by Dean.Jain
 * Error Code Enum constants
 */
public enum ErrorCodes {

    MBL_DEFAULT("MBL DEFAULT ", "MBL_1000"),
    MBL_JOB_RUNNER_FAILED("JOB RUNNER FAILED", "MBL_1005"),
    MBL_LOAD_PROPERTIES_FAILED("MBL LOAD PROPERTIES FAILD", "MBL_1006"),
    MBL_MEMSQL_DRIVER_LOAD_FAILED("MBL MEMSQL DRIVER LOAD FAILED", "MBL_1007"),
    MBL_MEMSQL_CONNECTION_FAILED("MBL MEMSQL CONNECTION FAILED", "MBL_1008"),
    MBL_MEMSQL_JDBC_FAILED("MBL MEMSQL JDBC FAILED", "MBL_1009"),
    MBL_PARSING_CONFIGURATION_FAILED("MBL FAILED TO PARSE CONFIGURATIONS", "MBL_1013"),
    MBL_CSV_READER_FAILED("MBL FAILED TO PARSE THE CSV", "MBL_1014"),
    MBL_MANDATORY_COLUMNS_VALIDATION_FAILED("MANDATORY COLUMNS VALIDATION FAILED", "MBL_1015"),
    MBL_INPUT_VALIDATION_FAILED("INPUT COLUMNS VALIDATION FAILED", "MBL_1017"),
    MBL_TABLE_CONFIG_PARSING_FAILED("TABLE CONFIG YML PARSING FAILED", "MBL_1018"),
    MBL_COLUMNSTORE_LOAD_FAILED("MBL MEMSQL COLUMNSTORE TABLE LOAD FAILED", "MBL_1018"),
    MBL_HIVE_TABLE_VALIDATION_FAILED("CONFIGURED HIVE COLUMNS NOT PRESENT IN THE CONFIGURED HIVE TABLE", "MBL_1019"),
    MBL_CHECKS_AND_BALANCES_FAILED("CHECKS AND BALANCING FAILED", "MBL_1019"),
    MBL_MEMSQL_SAVE_FAILED("MBL FAILED TO SAVE INTO MEMSQL", "MBL_1005");


    private String message;
    private String code;

    ErrorCodes(String message, String code) {
        this.message = message;
        this.code = code;
    }

    @Override
    public String toString() {
        return code + ": " + message;
    }

}
