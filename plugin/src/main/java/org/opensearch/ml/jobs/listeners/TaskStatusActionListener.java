package org.opensearch.ml.jobs.listeners;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.ml.common.MLTask;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.ml.common.transport.task.MLTaskGetResponse;
import org.opensearch.threadpool.ThreadPool;


public class TaskStatusActionListener implements ActionListener<MLTaskGetResponse> {
    private static final Logger log = LogManager.getLogger(TaskStatusActionListener.class);

    private final String taskId;
    private final ThreadPool threadPool;
    private final ActionListener<String> completionListener;
    private final Runnable retryAction;
    private final long retryDelayMs;


    public TaskStatusActionListener(
            String taskId,
            ThreadPool threadPool,
            ActionListener<String> completionListener,
            Runnable retryAction,
            long retryDelayMs
    ) {
        this.taskId = taskId;
        this.threadPool = threadPool;
        this.completionListener = completionListener;
        this.retryAction = retryAction;
        this.retryDelayMs = retryDelayMs;
    }

    @Override
    public void onResponse(MLTaskGetResponse taskResponse) {
        try {
            log.debug("Received task status response for task ID: {}", taskId);
            if (taskResponse != null && taskResponse.getMlTask() != null) {
                MLTask task = taskResponse.getMlTask();
                log.info("11111111111111111111111\r\n");
                log.info("111 11 Task state: {}, function name: {}", task.getState(), task.getFunctionName());
                log.info(taskResponse.toString());
                log.info("222222222222222222222222\r\n");
                if (isTaskComplete(task)) {
                    log.info("1111 taskResponse is : {}", taskResponse);
                    String responseText = extractResponseText(task);
                    if (responseText != null) {
                        log.info("11 completed response text length: {}", responseText.length());
                    } else {
                        log.warn("11 completed but response text not found for task {}", taskId);
                    }
                    log.info("1111  Task {} completed successfully, notifying completion listener", taskId);
                    completionListener.onResponse(responseText);
                } else {
                    log.info("Task {} not yet completed (state: {}), scheduling retry in {} ms",
                            taskId, task.getState(), retryDelayMs);
                    scheduleRetry();
                }
            } else {
                log.warn("Received empty task response for task ID: {}, scheduling retry", taskId);
                scheduleRetry();
            }
        } catch (Exception e) {
            log.error("Error processing task status response for task ID: {}, error: {}", taskId, e.getMessage(), e);
            completionListener.onFailure(e);
        }
    }

    @Override
    public void onFailure(Exception e) {
        log.error("Failed to fetch task status for task ID: {}, error: {}", taskId, e.getMessage(), e);
        scheduleRetry();
    }

    private boolean isTaskComplete(MLTask task) {
        return task.getState() == MLTaskState.COMPLETED;
    }

    private String extractResponseText(MLTask task) {
        Map<String, Object> response = task.getResponse();
        if (response == null) {
            return null;
        }
        try {
            Object infResultsObj = response.get("inference_results");
            if (infResultsObj instanceof List) {
                List<?> infResults = (List<?>) infResultsObj;
                if (!infResults.isEmpty() && infResults.get(0) instanceof Map) {
                    Map<?, ?> first = (Map<?, ?>) infResults.get(0);
                    Object outputObj = first.get("output");
                    if (outputObj instanceof List) {
                        List<?> outputs = (List<?>) outputObj;
                        for (Object o : outputs) {
                            if (o instanceof Map) {
                                Map<?, ?> outMap = (Map<?, ?>) o;
                                Object dataAsMapObj = outMap.get("dataAsMap");
                                if (dataAsMapObj instanceof Map) {
                                    Map<?, ?> dataMap = (Map<?, ?>) dataAsMapObj;
                                    Object responseTextObj = dataMap.get("response");
                                    if (responseTextObj != null) {
                                        return String.valueOf(responseTextObj);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to parse response text for task {}: {}", task.getTaskId(), e.getMessage());
        }
        return null;
    }

    private void scheduleRetry() {
        if (retryAction != null && threadPool != null) {
            threadPool.schedule(
                    () -> {
                        log.debug("Executing scheduled retry for task ID: {}", taskId);
                        retryAction.run();
                    },
                    TimeValue.timeValueMillis(retryDelayMs),
                    ThreadPool.Names.GENERIC
            );
        }
    }
}