
package org.opensearch.ml.jobs.processors;

import java.time.Instant;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.FunctionName;

import org.opensearch.ml.common.MLAgentType;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.execute.agent.AgentMLInput;
import org.opensearch.ml.common.transport.agent.MLRegisterAgentAction;
import org.opensearch.ml.common.transport.agent.MLRegisterAgentRequest;
import org.opensearch.ml.common.transport.agent.MLRegisterAgentResponse;
import org.opensearch.ml.common.agent.MLAgent;
import org.opensearch.ml.common.agent.MLToolSpec;
import org.opensearch.ml.common.transport.execute.MLExecuteTaskAction;
import org.opensearch.ml.common.transport.execute.MLExecuteTaskRequest;
import org.opensearch.ml.common.transport.execute.MLExecuteTaskResponse;
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
            parameters.put("question", "hi");
            parameters.put("trigger_time", Instant.now().toString());
            parameters.put("verbose", true);

//            registerAgent(agentName, agentDescription, parameters);

            String agentId = getAgentId();

            if (agentId == null || agentId.isEmpty()) {
                log.warn("Agent ID not configured, skipping deepresearch agent execution");
                return;
            }

            log.info("Executing deepresearch agent with ID: {}", agentId);

            executeAgent(agentId, parameters);

            log.info("DeepresearchAgentCronJob completed successfully at: {}", Instant.now());
        } catch (Exception e) {
            log.error("DeepresearchAgentCronJob failed with error: {}", e.getMessage(), e);
        }
    }

    private String getAgentId() {
        // TODO: 从配置文件或环境变量中获取 agent ID
        // 可以通过 OpenSearch 设置或插件配置来管理
//        return System.getProperty("deepresearch.agent.id",
//                System.getenv("DEEPRESEARCH_AGENT_ID"));
        return "uCC1z5gBMwWN6j8myLfH";
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


            MLExecuteTaskRequest executeRequest = new MLExecuteTaskRequest(FunctionName.AGENT, agentMLInput);


            client.execute(MLExecuteTaskAction.INSTANCE, executeRequest, new ActionListener<MLExecuteTaskResponse>() {
                @Override
                public void onResponse(MLExecuteTaskResponse response) {
                    log.info("11Successfully executed deepresearch agent {}", agentId);
                    log.debug("11Agent execution response: {}", response.getOutput());
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

    private void registerAgent(String agentName, String agentDescription, Map<String, Object> parameters) {
        try {
            log.info("Starting agent registration with name: {}", agentName);


            MLToolSpec toolSpec = MLToolSpec.builder()
                    .name("list_index_tool")
                    .type("ListIndexTool")
                    .build();


            MLAgent mlAgent = MLAgent.builder()
                    .name(agentName)
                    .type(MLAgentType.FLOW.name())
                    .description(agentDescription)
                    .tools(Arrays.asList(toolSpec))
                    .build();


            MLRegisterAgentRequest registerRequest = MLRegisterAgentRequest.builder()
                    .mlAgent(mlAgent)
                    .build();

            log.info("Sending agent registration request for agent: {}", agentName);

//            var threadContext = client.threadPool().getThreadContext();
//            log.info("threadContext is: {}", threadContext);
//

            String userStr = client.threadPool().getThreadContext().getTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
            log.info("userStr is: {}", userStr);

            String roleStr = client.threadPool().getThreadContext().getTransient(ConfigConstants.OPENSEARCH_SECURITY_INJECTED_ROLES);
            log.info("roleStr is: {}", roleStr);


            client.execute(MLRegisterAgentAction.INSTANCE, registerRequest, new ActionListener<MLRegisterAgentResponse>() {
                @Override
                public void onResponse(MLRegisterAgentResponse response) {
                    log.info("Agent registration completed successfully. Agent ID: {}, Agent Name: {}",
                            response.getAgentId(), agentName);
                    log.debug("Registration response details: {}", response);
                }

                @Override
                public void onFailure(Exception e) {
                    log.error("Failed to register agent '{}': {}", agentName, e.getMessage(), e);
                }
            });

        } catch (Exception e) {
            log.error("Exception occurred while preparing agent registration for '{}': {}", agentName, e.getMessage(), e);
        }
    }
}
