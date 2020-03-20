CREATE TABLE `bloom_job_history` (
                                   `job_id`                 bigint(20) NOT NULL AUTO_INCREMENT,
                                   `sourceType`             varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
                                   `inputFilePath`          varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
                                   `tableName`              varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
                                   `inputRecordsCount`      varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
                                   `processedRecordsCount`  varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
                                   `startTime`              DATETIME(6),
                                   `endTime`                DATETIME(6),
                                   `loadAppendTimestampCol` bigint(20),
                                   `refreshType`            varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
                                   `className`              varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
                                   `applicationId`          varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
                                   `totalTimeTaken`         varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
                                   `status`                 varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
                                   `errorMessage`           longtext CHARACTER SET utf8 COLLATE utf8_bin,
                                   PRIMARY KEY (`job_id`)
);