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

package com.aexp.blaze.bloom.processor;

import com.aexp.blaze.bloom.config.AppConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

/**
 * For Audit, loads config
 */
public class AuditProcessor {
    private static final Logger LOGGER = LogManager.getLogger(AuditProcessor.class);
    public HashMap<String, String> ctlFileMap = new HashMap<>();
    public boolean ctlFilePresent;

    public AuditProcessor(AppConfig appConfig) throws FileNotFoundException {
        int dotPos = appConfig.getCommandLineConf().getInputLocation().lastIndexOf(".");
        String controlFileName = (appConfig.getCommandLineConf().getInputLocation()).substring(0, dotPos);
        String controlFileNameWithExtn = controlFileName.concat(".ctl");
        File ctlFile = new File(controlFileNameWithExtn);
        if (ctlFile.exists()) {
            setCtlFilePresent(true);
            Scanner scanner = new Scanner(ctlFile);
            //Set the delimiter used in file
            scanner.useDelimiter(appConfig.getCSVDelimiter());
            int count = 0;
            //Get all tokens and store them in some data structure
            while (scanner.hasNext()) {
                count++;
                String value = scanner.next().trim();
                if (count == 1)
                    ctlFileMap.put("date", value);
                else if (count == 2) {
                    ctlFileMap.put("table", value);
                } else if (count == 3) {
                    ctlFileMap.put("recordCount", value);
                    break;
                }

            }
            scanner.close();
        } else {
            setCtlFilePresent(false);
        }

    }

    public boolean isCtlFilePresent() {
        return ctlFilePresent;
    }

    public void setCtlFilePresent(boolean ctlFilePresent) {
        this.ctlFilePresent = ctlFilePresent;
    }

}
