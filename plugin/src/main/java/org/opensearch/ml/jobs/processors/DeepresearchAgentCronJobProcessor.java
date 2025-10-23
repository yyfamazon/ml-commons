
package org.opensearch.ml.jobs.processors;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.transport.search.MLSearchActionRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

import org.opensearch.ml.common.MLAgentType;
import org.opensearch.ml.common.MLTask;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.execute.agent.AgentMLInput;
import org.opensearch.ml.common.transport.agent.MLRegisterAgentAction;
import org.opensearch.ml.common.transport.agent.MLRegisterAgentRequest;
import org.opensearch.ml.common.transport.agent.MLRegisterAgentResponse;
import org.opensearch.ml.common.transport.agent.MLSearchAgentAction;
import org.opensearch.ml.common.agent.MLAgent;
import org.opensearch.ml.common.agent.MLToolSpec;
import org.opensearch.ml.common.transport.execute.MLExecuteTaskAction;
import org.opensearch.ml.common.transport.execute.MLExecuteTaskRequest;
import org.opensearch.ml.common.transport.execute.MLExecuteTaskResponse;
import org.opensearch.ml.common.transport.task.MLTaskGetAction;
import org.opensearch.ml.common.transport.task.MLTaskGetRequest;
import org.opensearch.ml.common.transport.task.MLTaskGetResponse;
import org.opensearch.ml.jobs.listeners.TaskStatusActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

public class DeepresearchAgentCronJobProcessor extends MLJobProcessor {

    private static final Logger log = LogManager.getLogger(DeepresearchAgentCronJobProcessor.class);

    private static DeepresearchAgentCronJobProcessor instance;

    public static DeepresearchAgentCronJobProcessor getInstance(
            ClusterService clusterService,
            Client client,
            ThreadPool threadPool
    ) {
        if (instance != null) {
            return instance;
        }
        synchronized (DeepresearchAgentCronJobProcessor.class) {
            if (instance != null) {
                return instance;
            }
            instance = new DeepresearchAgentCronJobProcessor(clusterService, client, threadPool);
            return instance;
        }
    }

    public static synchronized void reset() {
        instance = null;
    }

    public DeepresearchAgentCronJobProcessor(
            ClusterService clusterService,
            Client client,
            ThreadPool threadPool
    ) {
        super(clusterService, client, threadPool);
    }

    @Override
    public void run() {
        log.info("DeepresearchAgentCronJob started at: {}", Instant.now());

        try {
            List<String> roles = new ArrayList<>();
//             roles.add("fake-role");
            roles.add("all_access");

            InjectorContextElement contextElement = new InjectorContextElement(
                    "id",
                    client.settings(),
                    client.threadPool().getThreadContext(),
                    roles
            );

            String agentName = "Test_Agent_For_RAG";
            String agentDescription = "this is a test agent";

            Map<String, Object> parameters = new HashMap<>();
            parameters.put("question", "Are there any errors today in openTelemetry log? " +
                    "Please return an object of array, array contains errors, and for each error, " +
                    "return error.title, error.description, error.severity, error.occurrenceTime, " +
                    "error.indexTimeFieldName, error.indexName. For error.severity, please evaluate " +
                    "the error and assign a value from (Low, Medium, High) to the error.severity. " +
                    "For error.occurrenceTime, if there are multiple occurrences, pick any of them.");
//            parameters.put("question", "how are you?");
            parameters.put("trigger_time", Instant.now().toString());
            parameters.put("verbose", true);
            parameters.put("async", true);

            //    registerAgent(agentName, agentDescription, parameters);

            executeAgent("uCC1z5gBMwWN6j8myLfH", parameters);

            //used for local testing
//            getAgentId(new ActionListener<>() {
//                @Override
//                public void onResponse(String agentId) {
//                    if (agentId == null || agentId.isEmpty()) {
//                        log.warn("Agent ID not configured, skipping deepresearch agent execution");
//                        return;
//                    }
//
//                    log.info("Executing deepresearch agent with ID: {}", agentId);
//                    executeAgent(agentId, parameters);
//                    log.info("DeepresearchAgentCronJob completed successfully at: {}", Instant.now());
//                }
//
//                @Override
//                public void onFailure(Exception e) {
//                    log.error("DeepresearchAgentCronJob failed with error: {}", e.getMessage(), e);
//                }
//            });
        } catch (Exception e) {
            log.error("DeepresearchAgentCronJob failed with error: {}", e.getMessage(), e);
        }
    }

    private void getAgentId(ActionListener<String> listener) {
        try {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            QueryBuilder queryBuilder = new TermQueryBuilder("name.keyword", "Claude 4 ppl-mapping-list");
            sourceBuilder.query(queryBuilder);
            SearchRequest searchRequest = new SearchRequest().source(sourceBuilder).indices("ml-agent");
            MLSearchActionRequest mlSearchRequest = MLSearchActionRequest.builder()
                    .searchRequest(searchRequest)
                    .tenantId(null)
                    .build();

            client.execute(MLSearchAgentAction.INSTANCE, mlSearchRequest, new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    if (searchResponse.getHits().getTotalHits().value() > 0) {
                        SearchHit[] hits = searchResponse.getHits().getHits();
                        if (hits.length > 0) {
                            listener.onResponse(hits[0].getId());
                        } else {
                            listener.onResponse(null);
                        }
                    } else {
                        listener.onResponse(null);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    log.error("Failed to search agent ID: {}", e.getMessage(), e);
                    listener.onResponse(null);
                }
            });
        } catch (Exception e) {
            log.error("Failed to search agent ID: {}", e.getMessage(), e);
            listener.onResponse(null);
        }
    }

    private void executeAgent(String agentId, Map<String, Object> parameters) {
        try {
            // 转换参数类型为 Map<String, String>
            Map<String, String> stringParameters = new HashMap<>();
            for (Map.Entry<String, Object> entry : parameters.entrySet()) {
                stringParameters.put(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null);
            }

            // 创建 RemoteInferenceInputDataSet
            RemoteInferenceInputDataSet inputDataSet = RemoteInferenceInputDataSet.builder()
                    .parameters(stringParameters)
                    .build();

            // 创建 AgentMLInput
            AgentMLInput agentMLInput = new AgentMLInput(agentId, null, FunctionName.AGENT, inputDataSet);
            agentMLInput.setIsAsync(true);


            MLExecuteTaskRequest executeRequest = new MLExecuteTaskRequest(FunctionName.AGENT, agentMLInput);


            client.execute(MLExecuteTaskAction.INSTANCE, executeRequest, new ActionListener<MLExecuteTaskResponse>() {
                @Override
                public void onResponse(MLExecuteTaskResponse response) {
                    log.info("11Successfully executed deepresearch agent {}", agentId);
                    ObjectMapper mapper = new ObjectMapper();
//                    mapper.registerModule(new JavaTimeModule());
                    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
                    try {
                        String outputStr = mapper.writeValueAsString(response.getOutput());
                        log.info("11Agent execution response writeValueAsString: {}", outputStr);

                        JsonNode node = mapper.readTree(outputStr);
                        String status = node.path("status").asText(null);
                        String taskId = node.path("taskId").asText(null);
                        log.info("111  11 Execute response status: {} , taskId: {}", status, taskId);

//                        List<ErrorObject> demoErrors = createDemoErrorObjects();
//                        log.info("No error objects found, creating demo data with {} entries", demoErrors.size());
//                        indexErrorObjects("ml_error_logs", demoErrors, taskId);

                        if ("COMPLETED".equalsIgnoreCase(status)) {
                            log.info("11Completed Output: {}", outputStr);
                        } else if (taskId != null && (status == null ||
                                "RUNNING".equalsIgnoreCase(status) ||
                                "IN_PROGRESS".equalsIgnoreCase(status))) {
                            log.info("11 Running{}", taskId);
                            pollTaskStatus(taskId);
                        } else if ("FAILED".equalsIgnoreCase(status)) {
                            log.warn("11 FAILED: {}", outputStr);
                        } else {
                            log.warn("11 else {}", outputStr);
                        }
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }

                private void pollTaskStatus(String taskId) {
                    MLTaskGetRequest taskGetRequest = new MLTaskGetRequest(taskId, null);
                    long retryDelayMs = 1000 * 60 * 5L;

                    ActionListener<String> completionListener = new ActionListener<>() {
                        @Override
                        public void onResponse(String responseText) {
                            if (responseText != null) {
                                log.info("Task {} completed with final response:\n{}", taskId, responseText);

                                // Extract and process JSON error array
                                try {
                                    String jsonArrayStr = extractJsonArrayFromMarkdown(responseText);
                                    log.info("jsonArrayStr is {}", jsonArrayStr);
                                    if (jsonArrayStr != null) {
                                        List<ErrorObject> errorObjects = processErrorArray(jsonArrayStr);
                                        String indexName = "ml_error_logs";
                                        createErrorIndex(indexName);
                                        log.info("errorObjects size is {}", errorObjects.size());

                                        if (!errorObjects.isEmpty()) {
                                            log.info("writing to index: ml_error_logs");
                                            log.info("errorObjects size is {}", errorObjects.size());
                                            log.info("errorObjects 1 is {}", errorObjects.getFirst().title);
                                            indexErrorObjects(indexName, errorObjects, taskId);
                                        } else {
//                                            List<ErrorObject> demoErrors = createDemoErrorObjects();
//                                            log.info("No error objects found, creating demo data with {} entries", demoErrors.size());
//                                            indexErrorObjects(indexName, demoErrors, taskId);
                                            log.info("not1 writing to index: ml_error_logs");
                                        }
                                    } else {
//                                        List<ErrorObject> demoErrors = createDemoErrorObjects();
//                                        log.info("111No error objects found, creating demo data with {} entries", demoErrors.size());
//                                        indexErrorObjects("ml_error_logs", demoErrors, taskId);
//                                        log.warn("No JSON array found in the response for task {}", taskId);
                                        log.info("not2 writing to index: ml_error_logs");
                                    }
                                } catch (Exception e) {
                                    log.error("Failed to process JSON error array for task {}: {}", taskId, e.getMessage(), e);
                                }

                            } else {
                                log.warn("Task {} completed but no response text extracted.", taskId);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            log.error("1111 11 Completion listener failed for task {}: {}", taskId, e.getMessage(), e);
                        }
                    };

                    TaskStatusActionListener statusListener = new TaskStatusActionListener(
                            taskId,
                            threadPool,
                            completionListener,
                            () -> pollTaskStatus(taskId),
                            retryDelayMs
                    );

                    client.execute(MLTaskGetAction.INSTANCE, taskGetRequest, statusListener);
                }

                @Override
                public void onFailure(Exception e) {
                    log.error("11Failed to execute deepresearch agent {}: {}", agentId, e.getMessage(), e);
                }
            });

        } catch (Exception e) {
            log.error("11Error creating agent execution request for {}: {}", agentId, e.getMessage(), e);
        }
    }

    // private void fetchTaskDetails(String taskId) {
    //     MLTaskGetRequest taskGetRequest = new MLTaskGetRequest(taskId, null);
    //     Runnable retryAction = () -> fetchTaskDetails(taskId);
    //     long retryDelayMs = 1000 * 60 * 5L;

    //     ActionListener<String> completionListener = new ActionListener<>() {
    //         @Override
    //         public void onResponse(String responseText) {
    //             if (responseText != null) {
    //                 log.info("Task {} completed with final response:\n{}", taskId, responseText);
    //             } else {
    //                 log.warn("Task {} completed but no response text extracted.", taskId);
    //             }
    //         }

    //         @Override
    //         public void onFailure(Exception e) {
    //             log.error("Completion listener failed for task {}: {}", taskId, e.getMessage(), e);
    //         }
    //     };

    //     TaskStatusActionListener statusListener = new TaskStatusActionListener(
    //         taskId,
    //         threadPool,
    //         completionListener,
    //         retryAction,
    //         retryDelayMs
    //     );

    //     client.execute(MLTaskGetAction.INSTANCE, taskGetRequest, statusListener);
    // }



    private String extractJsonArrayFromMarkdown(String markdownText) {
        if (markdownText == null) return null;

        int start = -1;
        boolean inString = false;
        boolean escaping = false;

        // Find the first '[' that starts an array (outside of a quoted string)
        for (int i = 0; i < markdownText.length(); i++) {
            char c = markdownText.charAt(i);

            if (inString) {
                if (escaping) {
                    escaping = false; // skip escaped char
                } else if (c == '\\') {
                    escaping = true;
                } else if (c == '"') {
                    inString = false;
                }
                continue;
            } else {
                if (c == '"') {
                    inString = true;
                    continue;
                }
                if (c == '[') {
                    start = i;
                    break;
                }
            }
        }

        if (start == -1) return null; // no array start found

        // From the first '[', walk forward and find the matching closing ']'
        int depth = 0;
        inString = false;
        escaping = false;

        for (int i = start; i < markdownText.length(); i++) {
            char c = markdownText.charAt(i);

            if (inString) {
                if (escaping) {
                    escaping = false;
                } else if (c == '\\') {
                    escaping = true;
                } else if (c == '"') {
                    inString = false;
                }
                continue;
            } else {
                if (c == '"') {
                    inString = true;
                    continue;
                }
                if (c == '[') {
                    depth++;
                } else if (c == ']') {
                    depth--;
                    if (depth == 0) {
                        // Found the full array
                        return markdownText.substring(start, i + 1);
                    }
                }
            }
        }

        // Unbalanced brackets
        return null;
    }

    /**
     * Represents an error object from the JSON array
     */
    private static class ErrorObject {
        private String title;
        private String description;
        private String severity;
        private String occurrenceTime;
        private String indexTimeFieldName;
        private String indexName;

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public String getSeverity() {
            return severity;
        }

        public void setSeverity(String severity) {
            this.severity = severity;
        }

        public String getOccurrenceTime() {
            return occurrenceTime;
        }

        public void setOccurrenceTime(String occurrenceTime) {
            this.occurrenceTime = occurrenceTime;
        }

        public String getIndexTimeFieldName() {
            return indexTimeFieldName;
        }

        public void setIndexTimeFieldName(String indexTimeFieldName) {
            this.indexTimeFieldName = indexTimeFieldName;
        }

        public String getIndexName() {
            return indexName;
        }

        public void setIndexName(String indexName) {
            this.indexName = indexName;
        }


        @Override
        public String toString() {
            return "Error{title='" + title + "', description='" + description + "', severity='" + severity + "'}";
        }
    }


    private List<ErrorObject> processErrorArray(String jsonArrayStr) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode arrayNode = mapper.readTree(jsonArrayStr);

            if (!arrayNode.isArray()) {
                log.error("JSON string is not an array");
                return new ArrayList<>();
            }

            log.info("Successfully parsed error array with {} entries", arrayNode.size());

            List<ErrorObject> errorObjects = new ArrayList<>();

            for (JsonNode elementNode : arrayNode) {
                ErrorObject error = new ErrorObject();

                // Check if this element has a nested "error" object
                if (elementNode.has("error") && elementNode.get("error").isObject()) {
                    // Handle nested error object case
                    JsonNode errorNode = elementNode.get("error");
                    error.setTitle(getStringValue(errorNode, "title"));
                    error.setDescription(getStringValue(errorNode, "description"));
                    error.setSeverity(getStringValue(errorNode, "severity"));
                    error.setIndexName(getStringValue(errorNode, "indexName"));
                    error.setOccurrenceTime(getStringValue(errorNode, "occurrenceTime"));
                    error.setIndexTimeFieldName(getStringValue(errorNode, "indexTimeFieldName"));
                } else {
                    // Handle flat structure case (original format)
                    error.setTitle(getStringValue(elementNode, "error.title"));
                    error.setDescription(getStringValue(elementNode, "error.description"));
                    error.setSeverity(getStringValue(elementNode, "error.severity"));
                    error.setIndexName(getStringValue(elementNode, "error.indexName"));
                    error.setOccurrenceTime(getStringValue(elementNode, "error.occurrenceTime"));
                    error.setIndexTimeFieldName(getStringValue(elementNode, "error.indexTimeFieldName"));
                }

                errorObjects.add(error);
            }

            for (int i = 0; i < errorObjects.size(); i++) {
                ErrorObject error = errorObjects.get(i);
                log.info("Error {}: {}", i+1, error);
            }
            return errorObjects;
        } catch (Exception e) {
            log.error("Failed to parse JSON error array: {}", e.getMessage(), e);
            log.info("Raw JSON array string: {}", jsonArrayStr);
            return new ArrayList<>();
        }
    }

    /**
     * Helper method to safely extract string values from JsonNode
     */
    private String getStringValue(JsonNode node, String fieldName) {
        JsonNode fieldNode = node.get(fieldName);
        return fieldNode != null && !fieldNode.isNull() ? fieldNode.asText() : null;
    }


    private void createErrorIndex(String indexName) {
        try {

            IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest(indexName);
            boolean indexExists = client.admin().indices().exists(indicesExistsRequest).actionGet().isExists();

            if (!indexExists) {

                XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("title")
                        .field("type", "text")
                        .field("analyzer", "standard")
                        .endObject()
                        .startObject("description")
                        .field("type", "text")
                        .field("analyzer", "standard")
                        .endObject()
                        .startObject("severity")
                        .field("type", "keyword")
                        .endObject()
                        .startObject("occurrenceTime")
                        .field("type", "keyword")
                        .endObject()
                        .startObject("indexTimeFieldName")
                        .field("type", "keyword")
                        .endObject()
                        .startObject("indexName")
                        .field("type", "keyword")
                        .endObject()
                        .startObject("taskId")
                        .field("type", "keyword")
                        .endObject()
                        .startObject("timestamp")
                        .field("type", "date")
                        .endObject()
                        .endObject()
                        .endObject();


                CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
                createIndexRequest.mapping(mappingBuilder);


                Settings indexSettings = Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                        .build();
                createIndexRequest.settings(indexSettings);

                // 执行创建索引请求
                CreateIndexResponse createIndexResponse = client.admin().indices().create(createIndexRequest).actionGet();

                if (createIndexResponse.isAcknowledged()) {
                    log.info("Successfully created index: {}", indexName);
                } else {
                    log.error("Failed to create index: {}", indexName);
                }
            } else {
                log.info("Index {} already exists", indexName);
            }
        } catch (Exception e) {
            log.error("Error creating index {}: {}", indexName, e.getMessage(), e);
        }
    }

    /**
     * 将错误对象写入索引
     * @param indexName 索引名称
     * @param errorObjects 错误对象列表
     * @param taskId 任务ID
     */
    private void indexErrorObjects(String indexName, List<ErrorObject> errorObjects, String taskId) {
        try {
            BulkRequest bulkRequest = new BulkRequest();
            ObjectMapper mapper = new ObjectMapper();

            for (ErrorObject error : errorObjects) {
                Map<String, Object> jsonMap = new HashMap<>();
                jsonMap.put("title", error.getTitle());
                jsonMap.put("description", error.getDescription());
                jsonMap.put("occurrenceTime", error.getOccurrenceTime());
                jsonMap.put("indexTimeFieldName", error.getIndexTimeFieldName());
                jsonMap.put("indexName", error.getIndexName());
                jsonMap.put("severity", error.getSeverity());
                jsonMap.put("taskId", taskId);
                jsonMap.put("timestamp", new Date());

                String documentId = UUID.randomUUID().toString();
                IndexRequest indexRequest = new IndexRequest(indexName)
                        .id(documentId)
                        .source(jsonMap);

                bulkRequest.add(indexRequest);
            }

            if (bulkRequest.numberOfActions() > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest).actionGet();

                if (bulkResponse.hasFailures()) {
                    log.error("Failed to index some error objects: {}", bulkResponse.buildFailureMessage());
                } else {
                    log.info("Successfully indexed {} error objects to index {}", errorObjects.size(), indexName);
                }
            }
        } catch (Exception e) {
            log.error("Error indexing error objects to {}: {}", indexName, e.getMessage(), e);
        }
    }


    private List<ErrorObject> createDemoErrorObjects() {
        List<ErrorObject> demoErrors = new ArrayList<>();


        ErrorObject error1 = new ErrorObject();
        error1.setTitle("Consume error: {0}");
        error1.setDescription("ConsumeException: Subscribed topic not available: orders: Broker: Unknown topic or partition. This error occurred in the accounting service while attempting to consume messages from a Kafka topic 'orders' that is not available or accessible.");
        error1.setSeverity("High");
        error1.setOccurrenceTime("2025-10-16T07:01:04.429380200Z");
        error1.setIndexTimeFieldName("timestamp");
        error1.setIndexName("ml_error_logs");
        demoErrors.add(error1);

//        ErrorObject error2 = new ErrorObject();
//        error2.setTitle("error demo 2");
//        error2.setDescription("error demo 2");
//        error2.setSeverity("Medium");
//        error2.setOccurrenceTime("time");
//        error2.setTimeFieldName("timeName");
//        error2.setIndexName("name");
//        demoErrors.add(error2);
//
//
//        ErrorObject error3 = new ErrorObject();
//        error3.setTitle("error demo 3");
//        error3.setDescription("error demo 3");
//        error3.setSeverity("Low");
//        error3.setOccurrenceTime("time");
//        error3.setTimeFieldName("timeName");
//        error3.setIndexName("name");
//        demoErrors.add(error3);

        return demoErrors;
    }
}
