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

package com.aexp.blaze.bloom.util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Provides basics File Utilities for HDFS
 */
public class FileUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);

    private static FileSystem fs;

    static {
        try {
            fs = FileSystem.get(new Configuration());
        } catch (IOException e) {
            LOGGER.error("failed to initialized file System", e);
        }
    }

    private FileUtils() {
    }

    /**
     * Read the file contents from path and return as string.
     *
     * @param path location of file
     * @return file contents
     */
    public static String readFileContent(String path) throws IOException {
        Path pathObj = new Path(path);
        BufferedReader input = new BufferedReader(new InputStreamReader(openInputStream(pathObj)));
        String conent = readFileContent(input);
        return conent;
    }

    /**
     * Opens a java.io.FileInputStream for the specified file, providing better error messages than
     * simply calling new FileInputStream(file).
     * An exception is thrown if the file exists but cannot be read.
     */
    public static FSDataInputStream openInputStream(Path path) throws IOException {
        if (!fs.exists(path)) {
            throw new FileNotFoundException("File '" + path + "' does not exist");
        }
        if (fs.exists(path) && fs.isDirectory(path)) {
            throw new IOException("File '" + path + "' exists but is a directory");
        }

        return fs.open(path);
    }


    public static String readFileContent(BufferedReader input) throws IOException {
        StringWriter output = new StringWriter();
        String line = null;
        while (null != (line = input.readLine())) {
            output.write(line);
        }
        return output.toString();
    }


}
