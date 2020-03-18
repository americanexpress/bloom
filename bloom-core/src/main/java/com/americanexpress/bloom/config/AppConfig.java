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
import com.americanexpress.bloom.constants.ErrorCodes;
import com.americanexpress.bloom.exceptions.BloomException;
import com.americanexpress.bloom.exceptions.BloomRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.Properties;

/***
 * This class represents the Application Level configurations DTO.
 * It holds the application properties configured in the properties file,
 * the ones it gets from command line
 * and also the table details which it gets from the yml file
 */
public class AppConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LogManager.getLogger(AppConfig.class);
    private CommandLineConfig commandLineConf;
    private Properties appProperties;
    private MemSQLTableConfig memSQLTableConfig;
    private String memSQLPassword;

    public AppConfig(String[] commandLineArgs) throws BloomException {
        this.commandLineConf = new CommandLineConfig(commandLineArgs);
        this.appProperties = new Properties();
        String srcPath = String.format("%s/conf/", System.getProperty(Constants.APP_HOME));
        String sourceFileName = String.format("bloom.properties", System.getProperty(Constants.ENV));
        File propFiles = new File(srcPath, sourceFileName);
        try (InputStream input = new FileInputStream(propFiles)) {
            appProperties.load(input);

        } catch (IOException ex) {
            throw new BloomRuntimeException("Failed to load properties file", ErrorCodes.MBL_LOAD_PROPERTIES_FAILED, ex);
        }
    }

    public CommandLineConfig getCommandLineConf() {
        return commandLineConf;
    }

    public MemSQLTableConfig getMemSQLTableConf() {
        return memSQLTableConfig;
    }

    public void setMemSQLTableConfig() throws BloomException {
        this.memSQLTableConfig = new MemSQLTableConfig(this.commandLineConf);
    }

    public String getMemSQLHost() {
        return this.appProperties.getProperty("blaze.bloom.memsql.host");
    }

    public String getMemSQLPort() {
        return this.appProperties.getProperty("blaze.bloom.memsql.port");
    }

    public String getMemSQLUsername() {
        return this.appProperties.getProperty("blaze.bloom.memsql.user");
    }

    public String getMemSQLEncryptedPassword() {
        return this.appProperties.getProperty("blaze.bloom.memsql.password");
    }

    public String getMemSQLPassword() {
        return this.memSQLPassword;
    }

    public void setMemSQLPassword(String pwd) {
        this.memSQLPassword = pwd;
    }

    public String getMemSQLDefaultDatabase() {
        return this.appProperties.getProperty("blaze.bloom.memsql.defaultDatabase");
    }

    public String getCSVDelimiter() {
        return this.appProperties.getProperty("blaze.bloom.csv.delimter");
    }

}
