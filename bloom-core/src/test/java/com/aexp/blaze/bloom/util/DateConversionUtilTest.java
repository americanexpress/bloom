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

import org.junit.Assert;
import org.junit.Test;

import static com.aexp.blaze.bloom.util.DateConversionUtil.ConvertDate;

public class DateConversionUtilTest {

    @Test
    public void testConvertDate() {

        try {
            Assert.assertEquals("2016-08-17 07:10:32.123", ConvertDate("2016-08-17 07:10:32.123000"));

            Assert.assertEquals("2016-08-17 07:10:32.001", ConvertDate("2016-08-17 07:10:32.001000"));

            Assert.assertEquals("2016-08-17 07:10:32.001", ConvertDate("2016-08-17 07:10:32.001"));

            Assert.assertEquals("2016-08-17 07:10:32.999", ConvertDate("2016-08-17 07:10:32.999000000"));


            Assert.assertEquals("2016-08-17 07:10:32.999", ConvertDate("2016-08-17 07:10:32.999000"));
            Assert.assertEquals("2016-08-17 07:10:32.000", ConvertDate("2016-08-17 07:10:32.000000"));
            Assert.assertEquals("2016-08-17 00:00:00.000", ConvertDate("20160817"));
            Assert.assertEquals("2016-08-17 07:10:00.000", ConvertDate("17.08.2016 07:10"));
        } catch (Exception e) {
            System.out.println(e);
        }
    }

}