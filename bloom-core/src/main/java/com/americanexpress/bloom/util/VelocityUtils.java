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

import com.americanexpress.bloom.beans.JobDetails;
import com.americanexpress.bloom.constants.Constants;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Date;

/**
 * stand along program to send email using velocity template engine
 *
 * @since 1.0
 */
public class VelocityUtils<T extends JobDetails> {

    public static final String XFORMATIONEMAIL_VM = "xformationemail.vm";
    private static final String[] TYPES = new String[]{"STR", "OBJ"};
    private static final Logger LOGGER = LoggerFactory.getLogger(VelocityUtils.class);
    private final VelocityEngine ve;

    public VelocityUtils(String emailTemplate) {
        ve = new VelocityEngine();
        ve.setProperty(RuntimeConstants.RESOURCE_LOADER, "file");
        ve.setProperty(RuntimeConstants.FILE_RESOURCE_LOADER_CACHE, "true");
        ve.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS, "org.apache.velocity.runtime.log.NullLogSystem");
        ve.setProperty(RuntimeConstants.FILE_RESOURCE_LOADER_PATH, emailTemplate);
        ve.init();
    }

    public static void main(String[] args) throws OperationNotSupportedException {
        LOGGER.info("Job Args:{}", Arrays.toString(args));
        if (args.length < 2) {
            LOGGER.error("Usage: <TYPE<STR|OBJ>> [CLass Name only supported for OBJ TYPE and must be of JobDetails type] <string>");
            System.exit(1);
        }
        try {
            String message = null;
            JobDetails jd = null;
            VelocityUtils vu = null;
            if (TYPES[0].equals(args[0])) {
                vu = new VelocityUtils<JobDetails>(args[1]);
                message = args[1];
            } else if (TYPES[1].equals(args[0])) {
                Class<? extends JobDetails> clazz = (Class<? extends JobDetails>) Class.forName(args[1].trim());
                String json = FileUtils.readFileContent(args[2]);
                jd = jsonToObj(json, clazz);
                vu = new VelocityUtils<JobDetails>((System.getProperty(Constants.APP_HOME) + "/conf"));
            } else {
                throw new OperationNotSupportedException("Only supports TYPE and STR");
            }
            System.out.println(vu.html(jd, message));
        } catch (IOException | ClassNotFoundException | UtilsException e) {
            LOGGER.error("Failed to create html for arguments:" + Arrays.toString(args), e);
            System.exit(1);
        }

    }

    public static <T> T jsonToObj(String str, Class<T> t) {
        ObjectMapper or = new ObjectMapper();
        try {
            return or.readValue(str, t);
        } catch (IOException e) {
            LOGGER.error("Failed deserilize json", e);
            return null;
        }

    }

    /**
     * Returns html file for job details
     */
    public String html(T jd, String message) throws UtilsException {
        if (ve == null) {
            throw new IllegalStateException("Initialize velocity engine");
        }
        VelocityContext context = new VelocityContext();
        String srcPath = String.format("%s/conf/", System.getProperty(Constants.APP_HOME));
        String templateFileName = "xformationemail.vm";
        Template t = ve.getTemplate("xformationemail.vm");
        if (jd != null) {
            context.put("jobDetails", jd);
        }
        if (message != null) {
            context.put("message", message);
        }

        context.put("date", new Date());
        StringWriter writer = new StringWriter();
        t.merge(context, writer);
        String html = writer.toString();
        //LOGGER.info("Email content:{}", html);
        try {
            writer.close();
        } catch (IOException e) {
            LOGGER.error("Failed to close writer", e);
        }
        return html;
    }
}
