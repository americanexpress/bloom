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

package com.aexp.blaze.bloom.reader;

import com.aexp.blaze.bloom.exceptions.BloomException;

/**
 * Currently supporting "csv" and hive
 */
public class ReaderFactory {

    private ReaderFactory() {
        // Utility classes, which are collections of static members, are not meant to be instantiated
    }

    public static Reader getReader(String type) throws BloomException {
        String inputType = type.toUpperCase();

        if (inputType.equalsIgnoreCase("csv")) {
            return new CSVReader();
        } else if (inputType.equalsIgnoreCase("hive")) {
            return new HiveReader();
        } else {
            throw new BloomException("No valid reader present for this type of input");
        }
    }
}
