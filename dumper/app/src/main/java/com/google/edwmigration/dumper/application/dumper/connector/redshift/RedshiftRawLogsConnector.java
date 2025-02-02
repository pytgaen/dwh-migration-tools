/*
 * Copyright 2022 Google LLC
 * Copyright 2013-2021 CompilerWorks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.edwmigration.dumper.application.dumper.connector.redshift;

import com.google.auto.service.AutoService;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentAssessment;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogDays;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogEnd;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogStart;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterable;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.ParallelTaskGroup;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RedshiftMetadataDumpFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RedshiftRawLogsDumpFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates these csv.
 * 1. userid => username mapping ( duplicated from schema )
 * 2. SVL_DDLTEXT for DDLs
 * 3 SVL_QUERY_TEXT for non-DDLS
 * 4. SQL_QUERY_METRICS for metrics
 */
@AutoService({Connector.class, LogsConnector.class})
@Description("Dumps logs from Amazon Redshift.")
@RespectsInput(order = ConnectorArguments.OPT_PORT_ORDER,
        arg = ConnectorArguments.OPT_PORT,
        description = "The port of the server.",
        required = ConnectorArguments.OPT_REQUIRED_IF_NOT_URL,
        defaultValue = "" + RedshiftMetadataConnector.OPT_PORT_DEFAULT)
@RespectsArgumentAssessment
@RespectsArgumentQueryLogDays
@RespectsArgumentQueryLogStart
@RespectsArgumentQueryLogEnd
public class RedshiftRawLogsConnector extends AbstractRedshiftConnector implements LogsConnector, RedshiftRawLogsDumpFormat {

    private static final Logger LOG = LoggerFactory.getLogger(RedshiftRawLogsConnector.class);

    public RedshiftRawLogsConnector() {
        super("redshift-raw-logs");
    }

    @Override
    public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments) throws MetadataDumperUsageException {

        ParallelTaskGroup parallelTask = new ParallelTaskGroup(this.getName());
        out.add(parallelTask);

        out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
        out.add(new FormatTask(FORMAT_NAME));

        //  is also be there in the metadata , no harm is making zip self-sufficient
        parallelTask.addTask(new JdbcSelectTask(RedshiftMetadataDumpFormat.PgUser.ZIP_ENTRY_NAME, "select * from pg_user"));

        ZonedIntervalIterable intervals = ZonedIntervalIterable.forConnectorArguments(arguments);

        // DDL TEXT is simple ...
        // min() as there is no ANY() or SOME()
        String queryTemplateDDL = "SELECT userid, xid, pid, trim(label) as label, starttime, endtime, sequence, text FROM STL_DDLTEXT WHERE ##";

        if (arguments.isAssessment()) {
            queryTemplateDDL += " ORDER BY starttime, xid, pid, sequence";
        }

        makeTasks(arguments, intervals, RedshiftRawLogsDumpFormat.DdlHistory.ZIP_ENTRY_PREFIX, queryTemplateDDL, "starttime", parallelTask);

        // Query Text has bit of playing around
        // 1. STL_QUERY has starttime, queryid, but text is 4000 char wich is useless
        // 2. STL_QUERY_TEXT has xid+squence+text which reconstructs query, but no starttime.
        // STL_QUERY is 1 row per query ; SQL_QUERY_TEXT is multi rows per query, using sequence and xid
        String queryTemplateQuery
                = "SELECT userid, xid, pid, query, trim(label) as label, starttime, endtime, sequence, text"
                + " FROM STL_QUERY join STL_QUERYTEXT using (userid, xid, pid, query) WHERE ##";

        if (arguments.isAssessment()) {
            queryTemplateQuery += " ORDER BY starttime, query, sequence";
        }

        makeTasks(arguments, intervals, RedshiftRawLogsDumpFormat.QueryHistory.ZIP_ENTRY_PREFIX, queryTemplateQuery, "starttime", parallelTask);

        if (arguments.isAssessment()) {
            String queryMetricsTemplateQuery
                = "SELECT userid, service_class, query, segment, step_type, starttime, slices, "
                + "  max_rows, rows, max_cpu_time, cpu_time, max_blocks_read, blocks_read, "
                + "  max_run_time, run_time, max_blocks_to_disk, blocks_to_disk, step, "
                + "  max_query_scan_size, query_scan_size, query_priority, query_queue_time, "
                + "  service_class_name "
                + "FROM STL_QUERY_METRICS WHERE ##";
            makeTasks(arguments, intervals,
                RedshiftRawLogsDumpFormat.QueryMetricsHistory.ZIP_ENTRY_PREFIX,
                queryMetricsTemplateQuery, "starttime", parallelTask);
        }
    }

    // ##  in the template to be replaced by the complete WHERE clause.
    private void makeTasks(ConnectorArguments arguments,
            ZonedIntervalIterable intervals,
            String filePrefix,
            String queryTemplate,
            String startField,
            ParallelTaskGroup out) throws MetadataDumperUsageException {

        List<String> whereClauses = new ArrayList<>();

        if (!StringUtils.isBlank(arguments.getQueryLogEarliestTimestamp()))
            whereClauses.add(String.format("%s >= CAST( '%s' as TIMESTAMP)", startField, arguments.getQueryLogEarliestTimestamp()));

        // LOG.info("Exporting query log for " + intervals);
        for (ZonedInterval interval : intervals) {
            String query = queryTemplate.replace("##",
                    newWhereClause(whereClauses,
                            String.format("%s >= TIMESTAMP '%s'", startField, SQL_FORMAT.format(interval.getStart())),
                            String.format("%s < TIMESTAMP '%s'", startField, SQL_FORMAT.format(interval.getEndExclusive()))));
            String file = filePrefix + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(interval.getStartUTC()) + RedshiftRawLogsDumpFormat.ZIP_ENTRY_SUFFIX;
            out.addTask(new JdbcSelectTask(file, query));
        }
    }
}
