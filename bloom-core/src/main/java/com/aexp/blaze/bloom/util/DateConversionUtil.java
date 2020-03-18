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


import com.aexp.blaze.bloom.exceptions.BloomException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Date util method which can convert any of the configured
 * input date formats into a standard output date format
 * additional parsers can be added to array to support more
 * input date formats
 */
public class DateConversionUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(DateConversionUtil.class);

    public static String ConvertDate(String inputDate) throws BloomException {


        DateTimeParser customParser = new DateTimeFormatterBuilder()
                .appendYear(4, 4)        //4 digit year (YYYY)
                .appendLiteral("-")
                .appendMonthOfYear(2)    //2 digit month (MM)
                .appendLiteral("-")
                .appendDayOfMonth(2)    //2 digit day (DD)
                .appendLiteral(" ")
                .appendHourOfDay(2)        //2 digit hour (hh)
                .appendLiteral(":")
                .appendMinuteOfHour(2)    //2 digit minute (mm)
                .appendLiteral(":")
                .appendSecondOfMinute(2)//2 digit second (ss)
                //optional 9 digit nanosec
                .appendOptional(new DateTimeFormatterBuilder()
                        .appendLiteral(".")
                        .appendPattern("SSSSSSSSS")
                        .toParser())
                .toParser();

        // creating an array of all possible parsers we want to support in our date conversion util
        DateTimeParser[] parsers = {
                DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss").getParser(),
                DateTimeFormat.forPattern("yyyyMMdd").getParser(),
                DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").getParser(),
                customParser
        };

        DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(null, parsers).toFormatter();
        DateTime dateOutput = null;

        try {
            if (inputDate != null && !inputDate.equals("") && !inputDate.equals(" ") && !inputDate.isEmpty())
                dateOutput = formatter.parseDateTime(inputDate);
            else
                throw new BloomException("Timestamp value cannot be null/empty. It should be either of these formats:(yyyy/MM/dd HH:mm:ss,yyyyMMdd,yyyy-MM-dd HH:mm:ss.SSSSSS)");
        } catch (IllegalArgumentException ie) {
            throw new BloomException("Timestamp value not supported. It should be either of these formats:(yyyy/MM/dd HH:mm:ss,yyyyMMdd,yyyy-MM-dd HH:mm:ss.SSSSSS) Failure reason is: " + ie.getMessage());

        }
        return dateOutput.toString();

    }
}
