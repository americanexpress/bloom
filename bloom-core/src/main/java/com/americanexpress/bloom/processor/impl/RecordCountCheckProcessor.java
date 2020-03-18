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
import com.americanexpress.bloom.constants.ErrorCodes;
import com.americanexpress.bloom.exceptions.BloomException;
import com.americanexpress.bloom.processor.AuditProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.FileNotFoundException;

/**
 * For Auditing, validates and checks if the record count is
 * same in control file vs data file
 */
public class RecordCountCheckProcessor extends AuditProcessor {
    private static final Logger LOGGER = LogManager.getLogger(RecordCountCheckProcessor.class);


    public RecordCountCheckProcessor(AppConfig appConfig) throws FileNotFoundException {
        super(appConfig);
    }

    public void validateRecordCount(Dataset<Row> inputDataSet) throws BloomException {
        if (ctlFileMap.get("recordCount") != null) {
            Long recordCountInCtlFile = Long.parseLong(ctlFileMap.get("recordCount"));
            Long recordCountInCSV = inputDataSet.count();
            if (!recordCountInCtlFile.equals(recordCountInCSV)) {
                LOGGER.warn("Record count in audit file(" + recordCountInCtlFile + ") does not match with the record count in data file(" + recordCountInCSV + ").. !");
                throw new BloomException("Record count in audit file(" + recordCountInCtlFile + ") does not match with the record count in data file(" + recordCountInCSV + ").. !", ErrorCodes.MBL_CHECKS_AND_BALANCES_FAILED);
            } else {
                LOGGER.info("Record count in audit file matches with the record count in data file.. !");
            }
        } else {
            throw new BloomException("audit file provided for data file is not proper.. !", ErrorCodes.MBL_CHECKS_AND_BALANCES_FAILED);
        }
    }
}
