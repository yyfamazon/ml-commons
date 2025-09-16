/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.jobs;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.opensearch.jobscheduler.spi.schedule.CronSchedule;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class DeepresearchAgentCronJob {

    private static final String DEFAULT_JOB_NAME_PREFIX = "deepresearch-agent-job";
    private static final Long DEFAULT_LOCK_DURATION_SECONDS = 300L; // 5 minutes
    private static final Double DEFAULT_JITTER = 0.1; // 10% jitter

    public static MLJobParameter createDailyCronJob(String jobName, int hour, int minute, boolean enabled) {
        if (hour < 0 || hour > 23) {
            throw new IllegalArgumentException("Hour must be between 0 and 23");
        }
        if (minute < 0 || minute > 59) {
            throw new IllegalArgumentException("Minute must be between 0 and 59");
        }

        String cronExpression = String.format("0 %d %d * * ?", minute, hour);

        CronSchedule schedule = new CronSchedule(cronExpression, java.time.ZoneId.systemDefault());

        log
            .info(
                "Creating daily cron job '{}' with schedule: {} ({}:{})",
                jobName,
                cronExpression,
                String.format("%02d", hour),
                String.format("%02d", minute)
            );

        return new MLJobParameter(
            jobName,
            schedule,
            DEFAULT_LOCK_DURATION_SECONDS,
            DEFAULT_JITTER,
            MLJobType.DEEPRESEARCH_AGENT_CRON,
            enabled
        );
    }

    public static MLJobParameter createCustomCronJob(String jobName, String cronExpression, boolean enabled) {
        CronSchedule schedule = new CronSchedule(cronExpression, java.time.ZoneId.systemDefault());

        log.info("Creating custom cron job '{}' with expression: {}", jobName, cronExpression);

        return new MLJobParameter(
            jobName,
            schedule,
            DEFAULT_LOCK_DURATION_SECONDS,
            DEFAULT_JITTER,
            MLJobType.DEEPRESEARCH_AGENT_CRON,
            enabled
        );
    }

    public static MLJobParameter createIntervalJob(String jobName, int interval, ChronoUnit unit, boolean enabled) {
        IntervalSchedule schedule = new IntervalSchedule(Instant.now(), interval, unit);

        log.info("Creating interval job '{}' with interval: {} {}", jobName, interval, unit);

        return new MLJobParameter(
            jobName,
            schedule,
            DEFAULT_LOCK_DURATION_SECONDS,
            DEFAULT_JITTER,
            MLJobType.DEEPRESEARCH_AGENT_CRON,
            enabled
        );
    }

    /**
     * Creates a default daily job that runs at 3 AM
     * 
     * @param enabled Whether the job is enabled
     * @return MLJobParameter configured for daily execution at 3 AM
     */
    public static MLJobParameter createDefaultDailyJob(boolean enabled) {
        String jobName = DEFAULT_JOB_NAME_PREFIX + "-daily-3am-" + System.currentTimeMillis();
        // return createDailyCronJob(jobName, 3, 0, enabled);
        return createDailyCronJob(jobName, 3, 0, enabled);
    }

    /**
     * Creates a test job that runs every minute (for testing purposes)
     * 
     * @param enabled Whether the job is enabled
     * @return MLJobParameter configured for every minute execution
     */
    public static MLJobParameter createTestJob(boolean enabled) {
        String jobName = DEFAULT_JOB_NAME_PREFIX + "-test-" + System.currentTimeMillis();
        return createIntervalJob(jobName, 1, ChronoUnit.MINUTES, enabled);
    }
}
