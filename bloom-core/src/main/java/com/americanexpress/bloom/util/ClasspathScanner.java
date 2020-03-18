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

package com.americanexpress.bloom.util;

import com.americanexpress.bloom.exceptions.BloomException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * loads class for the entities needed
 */
public class ClasspathScanner {
    private static Logger logger = LogManager.getLogger(ClasspathScanner.class);

    public List<URL> findEntityClassName(String entityClassName) throws BloomException {
        logger.debug("Finding entity class name in classpath");
        if (entityClassName == null) {

            throw new BloomException("Entity class name should not be null");
        }
        String eClassName = entityClassName.replaceAll("\\.", "/") + ".class";

        List<URL> allVersions = findAllResourceVersions(eClassName);

        logger.debug("All versions available for entity class is : {}", eClassName);
        for (URL version : allVersions) {
            logger.debug("Entity Class found : {}", version);
        }
        return allVersions;
    }

    private List<URL> findAllResourceVersions(String eClassName) throws BloomException {
        ClassLoader cl = getClass().getClassLoader();
        List<URL> results = new ArrayList<>();

        try {
            Enumeration<URL> urls = cl.getResources(eClassName);
            while (urls.hasMoreElements()) {
                results.add(urls.nextElement());
            }
        } catch (IOException exception) {
            throw new BloomException("Could not find the versions of classpath resource: " + eClassName, exception);
        }
        return results;
    }

}
