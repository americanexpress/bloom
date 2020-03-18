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


import com.americanexpress.bloom.constants.ErrorCodes;
import com.americanexpress.bloom.exceptions.BloomException;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;

/***
 * This class represents the command line arguments DTO and also helps the user by printing the help and usage information
 */
public class CommandLineConfig implements Serializable {


    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger(CommandLineConfig.class);
    protected CommandLine cmd;


    public CommandLineConfig(String[] commandLineArgs) throws BloomException {
        Options options = getOptions();
        CommandLineParser parser = new PosixParser();
        LOGGER.debug("parsing command line arguments");
        try {
            this.cmd = parser.parse(options, commandLineArgs);

            if (cmd.hasOption("h")) {
                help();
            }

        } catch (ParseException e) {
            printUsage();
            throw new BloomException("Parsing the command line arguments failed", ErrorCodes.MBL_PARSING_CONFIGURATION_FAILED, e);
        }
    }

    public String getTableName() {
        return this.cmd.getOptionValue("tn");
    }

    public String getInputLocation() {
        return this.cmd.getOptionValue("ip");
    }

    public String getRefreshType() {
        return this.cmd.getOptionValue("rt");
    }

    public String getInputType() {
        return this.cmd.getOptionValue("it");
    }

    public String getMemSQLTableType() {
        return this.cmd.getOptionValue("tt");
    }

    public String getAppFilePath() {
        return this.cmd.getOptionValue("fp");
    }


    public Options getOptions() {
        Options options = new Options();
        options.addOption(new Option("h", "help", false, "Show Help."));

        Option ec = new Option("tn", "tableName", true, "It should be the name of the table where the data has to be loaded");
        ec.setRequired(true);
        options.addOption(ec);
        Option ip = new Option("ip", "input", true, "Directory location where data file from ESODL will be dropped");
        ip.setRequired(true);
        options.addOption(ip);
        Option rt = new Option("rt", "refreshType", true, "This indicates the type of load-FULL_REFRESH,UPSERT");
        rt.setRequired(true);
        options.addOption(rt);
        Option it = new Option("it", "inputType", true, "This indicates the type of file like csv,hive table etc..");
        it.setRequired(true);
        options.addOption(it);
        Option tt = new Option("tt", "tableType", true, "This indicates the type of table-rowstore/columnstore");
        tt.setRequired(true);
        options.addOption(tt);
        Option ar = new Option("ar", "archival", true, "This indicates if archival of csv files is needed or not");
        options.addOption(ar);
        Option fp = new Option("fp", "filePath", true, "This indicates the path where the final file is to be written");
        options.addOption(fp);

        return options;
    }

    /***
     * Prints the usage and exits 
     */
    private void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("bloom", getOptions());
    }

    private void help() {
        // This prints out some help
        HelpFormatter formater = new HelpFormatter();
        formater.printHelp("bloom", getOptions());
        System.exit(0);
    }

}
