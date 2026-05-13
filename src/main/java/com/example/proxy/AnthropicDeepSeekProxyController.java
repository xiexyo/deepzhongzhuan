package com.example.proxy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;

@RestController
public class AnthropicDeepSeekProxyController {

    private static final Logger log = LoggerFactory.getLogger(AnthropicDeepSeekProxyController.class);

    private final ObjectMapper mapper;
    private final ProxyProperties properties;
    private final WebClient deepseekClient;

    public AnthropicDeepSeekProxyController(ObjectMapper mapper, ProxyProperties properties) {
        this.mapper = mapper;
        this.properties = properties;

        String baseUrl = removeTrailingSlash(properties.getDeepseekBaseUrl());

        this.deepseekClient = WebClient.builder()
                .baseUrl(baseUrl)
                .defaultHeaders(headers -> {
                    String apiKey = properties.getDeepseekApiKey();
                    if (apiKey != null && !apiKey.isBlank()) {
                        headers.setBearerAuth(apiKey);
                    }
                })
                .build();
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "ok");
        result.put("upstream", properties.getDeepseekBaseUrl());
        result.put("model", properties.getDeepseekModel());
        result.put("forwardTools", properties.isForwardTools());
        result.put("forwardToolChoice", properties.isForwardToolChoice());
        result.put("forwardStopSequences", properties.isForwardStopSequences());
        result.put("includeUsage", properties.isIncludeUsage());
        result.put("defaultMaxTokens", properties.getDefaultMaxTokens());
        result.put("maxTokensLimit", properties.getMaxTokensLimit());
        result.put("logStreamChunks", properties.isLogStreamChunks());
        result.put("thinkingType", properties.getThinkingType());
        result.put("reasoningEffort", properties.getReasoningEffort());
        result.put("agentReasoningEffort", properties.getAgentReasoningEffort());
        result.put("exposeThinking", properties.isExposeThinking());
        return result;
    }

    /**
     * Claude Code / Anthropic Messages API 入口
     */
    /**
     * Claude Code / Anthropic Messages API 入口
     */
    @PostMapping("/v1/messages")
    public Mono<ResponseEntity<?>> messages(@RequestBody JsonNode anthropicRequest) {
        String requestId = newRequestId();

        boolean stream = anthropicRequest.path("stream").asBoolean(false);

        logAnthropicRequestSummary(requestId, anthropicRequest);

        if (stream) {
            /*
             * 重点：
             * 这里不用 Flux<ServerSentEvent<String>> 返回给 Claude 插件，
             * 而是手写 Anthropic SSE 文本格式。
             */
            Flux<String> flux = streamMessages(requestId, anthropicRequest);

            ResponseEntity<Flux<String>> entity = ResponseEntity.ok()
                    .header("Cache-Control", "no-cache, no-transform")
                    .header("X-Accel-Buffering", "no")
                    .contentType(MediaType.TEXT_EVENT_STREAM)
                    .body(flux);

            return Mono.just((ResponseEntity<?>) entity);
        }

        ObjectNode deepseekPayload = toDeepSeekPayload(anthropicRequest, false);

        logDeepSeekRequest(requestId, false, deepseekPayload);

        return deepseekClient.post()
                .uri("/chat/completions")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue(deepseekPayload)
                .exchangeToMono(response -> {
                    HttpStatusCode status = response.statusCode();

                    log.info(
                            "[{}] DeepSeek non-stream response status={}",
                            requestId,
                            status
                    );

                    /*
                     * 这里不要直接 bodyToMono(JsonNode.class)，
                     * 先拿 String，才能把后端原始返回完整打印出来。
                     */
                    return response.bodyToMono(String.class)
                            .defaultIfEmpty("")
                            .flatMap(body -> {
                                logDeepSeekRawBody(requestId, "non-stream", body);

                                if (status.isError()) {
                                    log.error(
                                            "[{}] DeepSeek upstream non-stream error: status={}, body={}, payload={}",
                                            requestId,
                                            status,
                                            limit(body, logBodyMaxChars()),
                                            properties.isLogUpstreamRequest()
                                                    ? limit(toJsonString(deepseekPayload), logBodyMaxChars())
                                                    : "[payload logging disabled]"
                                    );

                                    ObjectNode error = anthropicErrorResponse(
                                            "upstream_error",
                                            "DeepSeek upstream returned " + status + ": " + limit(body, 2000)
                                    );

                                    return Mono.just(
                                            (ResponseEntity<?>) ResponseEntity
                                                    .status(HttpStatus.BAD_GATEWAY)
                                                    .contentType(MediaType.APPLICATION_JSON)
                                                    .body(error)
                                    );
                                }

                                JsonNode deepseekResponse;
                                try {
                                    deepseekResponse = mapper.readTree(body);
                                } catch (Exception e) {
                                    log.error(
                                            "[{}] Failed to parse DeepSeek non-stream response as JSON, raw body={}",
                                            requestId,
                                            limit(body, logBodyMaxChars()),
                                            e
                                    );

                                    ObjectNode error = anthropicErrorResponse(
                                            "invalid_upstream_response",
                                            "DeepSeek returned invalid JSON: " + limit(body, 2000)
                                    );

                                    return Mono.just(
                                            (ResponseEntity<?>) ResponseEntity
                                                    .status(HttpStatus.BAD_GATEWAY)
                                                    .contentType(MediaType.APPLICATION_JSON)
                                                    .body(error)
                                    );
                                }

                                ObjectNode anthropicResponse =
                                        toAnthropicResponse(anthropicRequest, deepseekResponse);

                                if (properties.isLogUpstreamResponse()) {
                                    log.info(
                                            "[{}] Converted Anthropic non-stream response={}",
                                            requestId,
                                            limit(toJsonString(anthropicResponse), logBodyMaxChars())
                                    );
                                }

                                return Mono.just(
                                        (ResponseEntity<?>) ResponseEntity.ok()
                                                .contentType(MediaType.APPLICATION_JSON)
                                                .body(anthropicResponse)
                                );
                            });
                })
                .onErrorResume(e -> {
                    /*
                     * 这里一般是：
                     * 1. 连接超时
                     * 2. DNS 失败
                     * 3. TCP 连接失败
                     * 4. TLS 握手失败
                     *
                     * 也就是说，大概率没有任何后端响应 body。
                     */
                    log.error(
                            "[{}] Proxy non-stream request failed before valid upstream response: url={}, payload={}",
                            requestId,
                            upstreamChatCompletionsUrl(),
                            properties.isLogUpstreamRequest()
                                    ? limit(toJsonString(deepseekPayload), logBodyMaxChars())
                                    : "[payload logging disabled]",
                            e
                    );

                    ObjectNode error = anthropicErrorResponse(
                            "proxy_error",
                            "Proxy request failed: " + e.getMessage()
                    );

                    return Mono.just(
                            (ResponseEntity<?>) ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                                    .contentType(MediaType.APPLICATION_JSON)
                                    .body(error)
                    );
                });
    }

    /**
     * Claude 有些客户端可能会调用 count_tokens。
     * 这里给一个简化估算，避免客户端直接报 404。
     */
    @PostMapping("/v1/messages/count_tokens")
    public ObjectNode countTokens(@RequestBody JsonNode request) {
        int chars = countTextChars(request);
        int estimatedTokens = Math.max(1, chars / 4);

        ObjectNode result = mapper.createObjectNode();
        result.put("input_tokens", estimatedTokens);
        return result;
    }

    /**
     * 转换 Anthropic 请求 -> OpenAI / DeepSeek chat/completions 请求
     */
    private ObjectNode toDeepSeekPayload(JsonNode anthropicRequest, boolean stream) {
        ObjectNode payload = mapper.createObjectNode();
        ThinkingConfig thinking = resolveThinking(anthropicRequest);

        payload.put("model", properties.getDeepseekModel());
        payload.put("stream", stream);

        /*
         * max_tokens 处理。
         *
         * Claude Code / VSCode 写代码、写文档时，如果 max_tokens 太小，
         * 会表现为输出很短或者提前停止。
         */
        int maxTokens = 4096;

        if (properties.getDefaultMaxTokens() != null && properties.getDefaultMaxTokens() > 0) {
            maxTokens = properties.getDefaultMaxTokens();
        }

        if (anthropicRequest.has("max_tokens") && anthropicRequest.get("max_tokens").canConvertToInt()) {
            maxTokens = anthropicRequest.get("max_tokens").asInt(maxTokens);
        }

        Integer limit = properties.getMaxTokensLimit();
        if (limit != null && limit > 0) {
            maxTokens = Math.min(maxTokens, limit);
        }

        if (maxTokens <= 0) {
            maxTokens = 4096;
        }

        payload.put("max_tokens", maxTokens);

        /*
         * DeepSeek 文档说明：
         * 思考模式下 temperature、top_p、presence_penalty、frequency_penalty 不生效。
         *
         * 所以 thinking enabled 时不转发这些参数，避免误解。
         */
        if (!thinking.effectiveEnabled) {
            if (anthropicRequest.has("temperature")) {
                payload.set("temperature", anthropicRequest.get("temperature"));
            }

            if (anthropicRequest.has("top_p")) {
                payload.set("top_p", anthropicRequest.get("top_p"));
            }

            if (anthropicRequest.has("presence_penalty")) {
                payload.set("presence_penalty", anthropicRequest.get("presence_penalty"));
            }

            if (anthropicRequest.has("frequency_penalty")) {
                payload.set("frequency_penalty", anthropicRequest.get("frequency_penalty"));
            }
        }

        /*
         * 重点：
         * Claude/Anthropic 的 stop_sequences 不一定适合 DeepSeek。
         * 对 Claude Code / VSCode 插件，建议默认不要转发。
         */
        boolean stopForwarded = properties.isForwardStopSequences()
                && anthropicRequest.has("stop_sequences")
                && anthropicRequest.get("stop_sequences").isArray()
                && !anthropicRequest.get("stop_sequences").isEmpty();

        if (stopForwarded) {
            payload.set("stop", anthropicRequest.get("stop_sequences"));
        }

        /*
         * 部分 OpenAI-compatible 服务支持 stream_options.include_usage。
         * 如果你的内网 DeepSeek 不支持，可以把 include-usage 设为 false。
         */
        if (stream && properties.isIncludeUsage()) {
            ObjectNode streamOptions = mapper.createObjectNode();
            streamOptions.put("include_usage", true);
            payload.set("stream_options", streamOptions);
        }

        ArrayNode messages = mapper.createArrayNode();

        if (anthropicRequest.has("system") && !anthropicRequest.get("system").isNull()) {
            ObjectNode systemMessage = mapper.createObjectNode();
            systemMessage.put("role", "system");
            systemMessage.put("content", contentToText(anthropicRequest.get("system")));
            messages.add(systemMessage);
        }

        JsonNode anthropicMessages = anthropicRequest.path("messages");
        if (anthropicMessages.isArray()) {
            for (JsonNode msg : anthropicMessages) {
                appendConvertedMessage(msg, messages);
            }
        }
        /*
         * DeepSeek thinking 参数。
         *
         * OpenAI 格式：
         * {
         *   "thinking": {
         *     "type": "enabled"
         *   },
         *   "reasoning_effort": "high"
         * }
         */
        if (thinking.sendThinkingParam) {
            ObjectNode thinkingNode = mapper.createObjectNode();
            thinkingNode.put("type", thinking.type);
            payload.set("thinking", thinkingNode);
        }

        if (thinking.effectiveEnabled) {
            payload.put("reasoning_effort", thinking.effort);
        }
        payload.set("messages", messages);

        boolean hasTools = anthropicRequest.has("tools")
                && anthropicRequest.get("tools").isArray()
                && !anthropicRequest.get("tools").isEmpty();

        if (properties.isForwardTools() && hasTools) {
            payload.set("tools", convertTools(anthropicRequest.get("tools")));

            if (properties.isForwardToolChoice() && anthropicRequest.has("tool_choice")) {
                JsonNode toolChoice = convertToolChoice(anthropicRequest.get("tool_choice"));
                if (toolChoice != null) {
                    payload.set("tool_choice", toolChoice);
                }
            }
        }

        log.info(
                "Forward to DeepSeek: stream={}, model={}, max_tokens={}, tools={}, stopForwarded={}, includeUsage={}, thinking={}, effort={}, exposeThinking={}",
                stream,
                properties.getDeepseekModel(),
                maxTokens,
                hasTools && properties.isForwardTools(),
                stopForwarded,
                stream && properties.isIncludeUsage(),
                thinking.type,
                thinking.effort,
                thinking.exposeThinking
        );

        if (log.isDebugEnabled()) {
            log.debug("DeepSeek payload={}", limit(toJsonString(payload), 12000));
        }

        return payload;
    }

    /**
     * Anthropic message -> OpenAI messages
     */
    private void appendConvertedMessage(JsonNode anthropicMessage, ArrayNode out) {
        String role = anthropicMessage.path("role").asText();
        JsonNode content = anthropicMessage.get("content");

        if ("user".equals(role)) {
            appendUserMessage(content, out);
            return;
        }

        if ("assistant".equals(role)) {
            appendAssistantMessage(content, out);
            return;
        }

        ObjectNode msg = mapper.createObjectNode();
        msg.put("role", role);
        msg.put("content", contentToText(content));
        out.add(msg);
    }

    /**
     * Anthropic user content 可能包含 tool_result。
     *
     * OpenAI / DeepSeek 要求 role=tool 的消息必须紧跟在带 tool_calls 的 assistant 消息后。
     * 所以如果一个 Anthropic user content 里同时有 text 和 tool_result，
     * 这里先输出 tool 消息，再输出普通 user 文本。
     */
    private void appendUserMessage(JsonNode content, ArrayNode out) {
        if (content == null || content.isNull()) {
            addUserText(out, "");
            return;
        }

        if (content.isTextual()) {
            addUserText(out, content.asText());
            return;
        }

        if (!content.isArray()) {
            addUserText(out, contentToText(content));
            return;
        }

        List<ObjectNode> toolMessages = new ArrayList<>();
        StringBuilder userText = new StringBuilder();

        for (JsonNode block : content) {
            String type = block.path("type").asText();

            if ("tool_result".equals(type)) {
                ObjectNode toolMsg = mapper.createObjectNode();
                toolMsg.put("role", "tool");
                toolMsg.put("tool_call_id", block.path("tool_use_id").asText());

                String toolContent = contentToText(block.get("content"));
                if (block.path("is_error").asBoolean(false)) {
                    toolContent = "Tool execution failed:\\n" + toolContent;
                }

                toolMsg.put("content", toolContent == null ? "" : toolContent);
                toolMessages.add(toolMsg);
            } else if ("text".equals(type)) {
                appendText(userText, block.path("text").asText());
            } else if ("image".equals(type)) {
                /*
                 * 你明确不需要 image 输入。
                 * 这里直接忽略，避免把 base64 图片塞给 DeepSeek。
                 */
                appendText(userText, "[Image input ignored by proxy]");
            } else {
                appendText(userText, contentToText(block));
            }
        }

        for (ObjectNode toolMsg : toolMessages) {
            out.add(toolMsg);
        }

        if (userText.length() > 0) {
            addUserText(out, userText.toString());
        }

        if (toolMessages.isEmpty() && userText.length() == 0) {
            addUserText(out, "");
        }
    }

    private void addUserText(ArrayNode out, String text) {
        ObjectNode msg = mapper.createObjectNode();
        msg.put("role", "user");
        msg.put("content", text == null ? "" : text);
        out.add(msg);
    }

    private void appendText(StringBuilder sb, String text) {
        if (text == null || text.isEmpty()) {
            return;
        }

        if (sb.length() > 0) {
            sb.append("\n");
        }

        sb.append(text);
    }

    /**
     * Anthropic assistant content 可能包含 tool_use，需要转换成 OpenAI assistant.tool_calls。
     */
    private void appendAssistantMessage(JsonNode content, ArrayNode out) {
        ObjectNode msg = mapper.createObjectNode();
        msg.put("role", "assistant");

        StringBuilder text = new StringBuilder();
        StringBuilder reasoning = new StringBuilder();
        ArrayNode toolCalls = mapper.createArrayNode();

        if (content == null || content.isNull()) {
            msg.put("content", "");
            out.add(msg);
            return;
        }

        if (content.isTextual()) {
            msg.put("content", content.asText());
            out.add(msg);
            return;
        }

        if (content.isArray()) {
            for (JsonNode block : content) {
                String type = block.path("type").asText();
                if ("text".equals(type)) {
                    appendText(text, block.path("text").asText());
                } else if ("thinking".equals(type)) {
                    /*
                     * Anthropic thinking block -> DeepSeek reasoning_content。
                     *
                     * 这对 thinking + tool call 的多轮场景很重要。
                     */
                    String thinkingText = block.path("thinking").asText("");
                    if (thinkingText == null || thinkingText.isBlank()) {
                        thinkingText = block.path("text").asText("");
                    }
                    appendText(reasoning, thinkingText);
                } else if ("redacted_thinking".equals(type)) {
                    /*
                     * Anthropic 可能存在 redacted_thinking。
                     * DeepSeek 无法使用，忽略。
                     */
                } else if ("tool_use".equals(type)) {
                    ObjectNode toolCall = mapper.createObjectNode();
                    toolCall.put("id", block.path("id").asText());
                    toolCall.put("type", "function");

                    ObjectNode function = mapper.createObjectNode();
                    function.put("name", block.path("name").asText());
                    function.put("arguments", toJsonString(block.path("input")));

                    toolCall.set("function", function);
                    toolCalls.add(toolCall);
                }
            }
        } else {
            appendText(text, contentToText(content));
        }

        /*
         * 很多 OpenAI 兼容服务对 content=null 支持不好。
         * 这里统一用空字符串，兼容性更好。
         */
        msg.put("content", text.length() > 0 ? text.toString() : "");
        if (reasoning.length() > 0) {
            msg.put("reasoning_content", reasoning.toString());
        }
        if (!toolCalls.isEmpty()) {
            msg.set("tool_calls", toolCalls);
        }

        out.add(msg);
    }

    /**
     * Anthropic tools -> OpenAI tools
     */
    private ArrayNode convertTools(JsonNode anthropicTools) {
        ArrayNode tools = mapper.createArrayNode();

        for (JsonNode anthropicTool : anthropicTools) {
            ObjectNode tool = mapper.createObjectNode();
            tool.put("type", "function");

            ObjectNode function = mapper.createObjectNode();
            function.put("name", anthropicTool.path("name").asText());

            if (anthropicTool.has("description")) {
                function.put("description", anthropicTool.path("description").asText());
            }

            if (anthropicTool.has("input_schema") && anthropicTool.get("input_schema").isObject()) {
                function.set("parameters", anthropicTool.get("input_schema"));
            } else {
                ObjectNode parameters = mapper.createObjectNode();
                parameters.put("type", "object");
                parameters.set("properties", mapper.createObjectNode());
                function.set("parameters", parameters);
            }

            tool.set("function", function);
            tools.add(tool);
        }

        return tools;
    }

    /**
     * Anthropic tool_choice -> OpenAI tool_choice
     */
    private JsonNode convertToolChoice(JsonNode anthropicToolChoice) {
        String type = anthropicToolChoice.path("type").asText();

        if ("auto".equals(type)) {
            return TextNode.valueOf("auto");
        }

        if ("any".equals(type)) {
            return TextNode.valueOf("required");
        }

        if ("none".equals(type)) {
            return TextNode.valueOf("none");
        }

        if ("tool".equals(type)) {
            ObjectNode result = mapper.createObjectNode();
            result.put("type", "function");

            ObjectNode function = mapper.createObjectNode();
            function.put("name", anthropicToolChoice.path("name").asText());

            result.set("function", function);
            return result;
        }

        return null;
    }

    /**
     * 非流式 DeepSeek 响应 -> Anthropic 响应
     */
    private ObjectNode toAnthropicResponse(JsonNode anthropicRequest, JsonNode deepseekResponse) {
        ObjectNode result = mapper.createObjectNode();

        String responseId = deepseekResponse.path("id").asText("chatcmpl-" + UUID.randomUUID());

        result.put("id", "msg_" + responseId);
        result.put("type", "message");
        result.put("role", "assistant");
        result.put("model", anthropicRequest.path("model").asText(properties.getDeepseekModel()));

        ArrayNode content = mapper.createArrayNode();

        JsonNode choices = deepseekResponse.path("choices");
        JsonNode choice = choices.isArray() && !choices.isEmpty()
                ? choices.get(0)
                : mapper.createObjectNode();

        JsonNode message = choice.path("message");

        ThinkingConfig thinking = resolveThinking(anthropicRequest);

        String reasoningContent = message.path("reasoning_content").asText("");
        if (thinking.effectiveEnabled
                && thinking.exposeThinking
                && reasoningContent != null
                && !reasoningContent.isBlank()) {
            ObjectNode thinkingBlock = mapper.createObjectNode();
            thinkingBlock.put("type", "thinking");
            thinkingBlock.put("thinking", reasoningContent);
            content.add(thinkingBlock);
        }

        String text = message.path("content").asText("");
        if (text != null && !text.isBlank()) {
            ObjectNode textBlock = mapper.createObjectNode();
            textBlock.put("type", "text");
            textBlock.put("text", text);
            content.add(textBlock);
        }

        JsonNode toolCalls = message.path("tool_calls");
        if (toolCalls.isArray()) {
            for (JsonNode toolCall : toolCalls) {
                ObjectNode block = mapper.createObjectNode();
                block.put("type", "tool_use");
                block.put("id", toolCall.path("id").asText("toolu_" + UUID.randomUUID()));
                block.put("name", toolCall.path("function").path("name").asText());

                String args = toolCall.path("function").path("arguments").asText("{}");
                block.set("input", parseJsonOrEmptyObject(args));

                content.add(block);
            }
        }

        result.set("content", content);

        String finishReason = choice.path("finish_reason").asText("stop");
        result.put("stop_reason", mapStopReason(finishReason));
        result.putNull("stop_sequence");

        ObjectNode usage = mapper.createObjectNode();
        usage.put("input_tokens", deepseekResponse.path("usage").path("prompt_tokens").asInt(0));
        usage.put("output_tokens", deepseekResponse.path("usage").path("completion_tokens").asInt(0));
        result.set("usage", usage);

        log.info(
                "DeepSeek non-stream finished: finish_reason={}, input_tokens={}, output_tokens={}",
                finishReason,
                usage.path("input_tokens").asInt(0),
                usage.path("output_tokens").asInt(0)
        );

        return result;
    }

    /**
     * 流式处理：OpenAI SSE -> Anthropic SSE
     *
     * 返回 Flux<String>，手写 SSE：
     *
     * event: xxx
     * data: {...}
     *
     */
    private Flux<String> streamMessages(String requestId, JsonNode anthropicRequest) {
        ObjectNode deepseekPayload = toDeepSeekPayload(anthropicRequest, true);

        logDeepSeekRequest(requestId, true, deepseekPayload);

        ThinkingConfig thinking = resolveThinking(anthropicRequest);

        StreamState state = new StreamState(
                mapper,
                anthropicRequest.path("model").asText(properties.getDeepseekModel()),
                thinking
        );

        return deepseekClient.post()
                .uri("/chat/completions")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(deepseekPayload)
                .exchangeToFlux(response -> {
                    HttpStatusCode status = response.statusCode();

                    log.info(
                            "[{}] DeepSeek stream response status={}",
                            requestId,
                            status
                    );

                    if (status.isError()) {
                        return response.bodyToMono(String.class)
                                .defaultIfEmpty("")
                                .flatMapMany(body -> {
                                    logDeepSeekRawBody(requestId, "stream-error", body);

                                    log.error(
                                            "[{}] DeepSeek upstream stream error: status={}, body={}, payload={}",
                                            requestId,
                                            status,
                                            limit(body, logBodyMaxChars()),
                                            properties.isLogUpstreamRequest()
                                                    ? limit(toJsonString(deepseekPayload), logBodyMaxChars())
                                                    : "[payload logging disabled]"
                                    );

                                    return Flux.just(
                                            sse("error", anthropicErrorResponse(
                                                    "upstream_error",
                                                    "DeepSeek upstream returned " + status + ": " + limit(body, 2000)
                                            ))
                                    );
                                });
                    }

                    Flux<String> start = Flux.just(
                            sse("message_start", state.messageStart())
                    );

                    Flux<String> body = response
                            .bodyToFlux(new ParameterizedTypeReference<ServerSentEvent<String>>() {
                            })
                            .doOnSubscribe(subscription -> {
                                log.info("[{}] DeepSeek stream body subscribed", requestId);
                            })
                            .doOnNext(event -> {
                                /*
                                 * 这里会打印后端原始 SSE event。
                                 *
                                 * 受这些配置控制：
                                 * proxy.log-upstream-response=true
                                 * 或
                                 * proxy.log-stream-chunks=true
                                 */
                                logDeepSeekStreamEvent(requestId, event);
                            })
                            .filter(event -> event.data() != null)
                            .concatMap(event -> {
                                String data = event.data();
                                return Flux.fromIterable(state.onDeepSeekSseData(data));
                            })
                            .doOnComplete(() -> {
                                log.info("[{}] DeepSeek stream body completed", requestId);
                            })
                            .onErrorResume(e -> {
                                /*
                                 * 流已经开始后发生错误：
                                 * 可能是上游中途断开、读取超时、SSE 格式异常等。
                                 */
                                log.error(
                                        "[{}] DeepSeek upstream stream body failed: url={}, payload={}",
                                        requestId,
                                        upstreamChatCompletionsUrl(),
                                        properties.isLogUpstreamRequest()
                                                ? limit(toJsonString(deepseekPayload), logBodyMaxChars())
                                                : "[payload logging disabled]",
                                        e
                                );

                                /*
                                 * 流已经开始后，不建议直接抛 error。
                                 * Claude VSCode 插件更容易表现为截断。
                                 * 这里尽量补齐 message_stop。
                                 */
                                return Flux.fromIterable(state.finishEvents());
                            });

                    Flux<String> end = Flux.defer(() -> {
                        log.info("[{}] Anthropic stream finishing", requestId);
                        return Flux.fromIterable(state.finishEvents());
                    });

                    return start.concatWith(body).concatWith(end);
                })
                .doOnCancel(() -> {
                    log.warn("[{}] Downstream client cancelled stream", requestId);
                })
                .onErrorResume(e -> {
                    /*
                     * 这里是请求还没有拿到上游 HTTP response 就失败。
                     *
                     * 你之前那个：
                     * Connection timed out: /10.0.37.102:80
                     *
                     * 就会走这里。
                     */
                    log.error(
                            "[{}] Proxy stream request failed before upstream response: url={}, payload={}",
                            requestId,
                            upstreamChatCompletionsUrl(),
                            properties.isLogUpstreamRequest()
                                    ? limit(toJsonString(deepseekPayload), logBodyMaxChars())
                                    : "[payload logging disabled]",
                            e
                    );

                    return Flux.just(
                            sse("error", anthropicErrorResponse(
                                    "proxy_error",
                                    "Proxy stream request failed: " + e.getMessage()
                            ))
                    );
                });
    }

    /**
     * 手写 Anthropic SSE。
     *
     * 注意：
     * JSON 字符串里的换行会被 Jackson 转义成 \n，
     * 所以 data 一行是安全的。
     */
    private String sse(String eventName, JsonNode data) {
        return "event: " + eventName + "\n" +
                "data: " + toJsonString(data) + "\n\n";
    }

    private ObjectNode anthropicErrorResponse(String type, String message) {
        ObjectNode result = mapper.createObjectNode();
        result.put("type", "error");

        ObjectNode error = mapper.createObjectNode();
        error.put("type", type == null || type.isBlank() ? "api_error" : type);
        error.put("message", message == null ? "" : message);

        result.set("error", error);
        return result;
    }

    private String contentToText(JsonNode content) {
        if (content == null || content.isNull()) {
            return "";
        }

        if (content.isTextual()) {
            return content.asText();
        }

        if (content.isArray()) {
            StringBuilder sb = new StringBuilder();

            for (JsonNode block : content) {
                if (block == null || block.isNull()) {
                    continue;
                }

                if (block.isTextual()) {
                    appendText(sb, block.asText());
                    continue;
                }

                String type = block.path("type").asText();

                if ("text".equals(type)) {
                    appendText(sb, block.path("text").asText());
                } else if ("image".equals(type)) {
                    /*
                     * 你不需要 image 输入，这里忽略。
                     */
                    appendText(sb, "[Image input ignored by proxy]");
                } else if (block.has("text")) {
                    appendText(sb, block.path("text").asText());
                } else if (block.has("content")) {
                    appendText(sb, contentToText(block.get("content")));
                } else {
                    appendText(sb, toJsonString(block));
                }
            }

            return sb.toString();
        }

        return toJsonString(content);
    }

    private JsonNode parseJsonOrEmptyObject(String json) {
        if (json == null || json.isBlank()) {
            return mapper.createObjectNode();
        }

        try {
            return mapper.readTree(json);
        } catch (Exception e) {
            ObjectNode fallback = mapper.createObjectNode();
            fallback.put("_raw", json);
            return fallback;
        }
    }

    private String mapStopReason(String openAiFinishReason) {
        if (openAiFinishReason == null) {
            return "end_turn";
        }

        return switch (openAiFinishReason) {
            case "tool_calls", "function_call" -> "tool_use";
            case "length" -> "max_tokens";
            case "stop" -> "end_turn";
            case "content_filter" -> "stop_sequence";
            default -> "end_turn";
        };
    }

    private String toJsonString(JsonNode node) {
        try {
            return mapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String removeTrailingSlash(String s) {
        if (s == null) {
            return "";
        }

        while (s.endsWith("/")) {
            s = s.substring(0, s.length() - 1);
        }

        return s;
    }

    private String limit(String text, int max) {
        if (text == null) {
            return "";
        }

        if (text.length() <= max) {
            return text;
        }

        return text.substring(0, max) + "...[truncated]";
    }

    private int countTextChars(JsonNode node) {
        if (node == null || node.isNull()) {
            return 0;
        }

        if (node.isTextual()) {
            return node.asText().length();
        }

        int sum = 0;

        if (node.isArray()) {
            for (JsonNode child : node) {
                sum += countTextChars(child);
            }
        } else if (node.isObject()) {
            Iterator<JsonNode> iterator = node.elements();
            while (iterator.hasNext()) {
                sum += countTextChars(iterator.next());
            }
        }

        return sum;
    }

    /**
     * 维护 OpenAI SSE -> Anthropic SSE 的流式状态。
     */
    private class StreamState {

        private final ObjectMapper mapper;
        private final String model;
        private final String messageId;
        private final ThinkingConfig thinking;

        private boolean thinkingBlockOpen = false;
        private int currentThinkingIndex = -1;
        private final StringBuilder pendingThinkingDelta = new StringBuilder();

        private boolean textBlockOpen = false;
        private int currentTextIndex = -1;
        private final StringBuilder pendingTextDelta = new StringBuilder();

        private static final int TEXT_DELTA_FLUSH_CHARS = 32;
        private static final int THINKING_DELTA_FLUSH_CHARS = 64;

        private int nextContentIndex = 0;
        private String finishReason = "stop";
        private int inputTokens = 0;
        private int outputTokens = 0;

        private boolean finished = false;

        private final Map<Integer, ToolBlock> toolBlocks = new LinkedHashMap<>();

        StreamState(ObjectMapper mapper, String model, ThinkingConfig thinking) {
            this.mapper = mapper;
            this.model = model;
            this.thinking = thinking;
            this.messageId = "msg_" + UUID.randomUUID();
        }

        JsonNode messageStart() {
            ObjectNode data = mapper.createObjectNode();
            data.put("type", "message_start");

            ObjectNode message = mapper.createObjectNode();
            message.put("id", messageId);
            message.put("type", "message");
            message.put("role", "assistant");
            message.put("model", model);
            message.set("content", mapper.createArrayNode());
            message.putNull("stop_reason");
            message.putNull("stop_sequence");

            ObjectNode usage = mapper.createObjectNode();
            usage.put("input_tokens", 0);
            usage.put("output_tokens", 0);
            message.set("usage", usage);

            data.set("message", message);
            return data;
        }

        List<String> onDeepSeekSseData(String rawData) {
            List<String> events = new ArrayList<>();

            if (rawData == null || rawData.isBlank()) {
                return events;
            }

            String data = rawData.trim();

            if (data.startsWith("data:")) {
                data = data.substring("data:".length()).trim();
            }

            if ("[DONE]".equals(data)) {
                return events;
            }

            JsonNode root;
            try {
                root = mapper.readTree(data);
            } catch (Exception e) {
                log.warn("Ignore invalid upstream SSE data: {}", limit(data, 1000));
                return events;
            }

            JsonNode usage = root.path("usage");
            if (usage.isObject()) {
                inputTokens = usage.path("prompt_tokens").asInt(inputTokens);
                outputTokens = usage.path("completion_tokens").asInt(outputTokens);
            }

            JsonNode choices = root.path("choices");
            if (!choices.isArray() || choices.isEmpty()) {
                return events;
            }

            JsonNode choice = choices.get(0);

            if (!choice.path("finish_reason").isMissingNode()
                    && !choice.path("finish_reason").isNull()) {
                finishReason = choice.path("finish_reason").asText("stop");

                log.info(
                        "DeepSeek stream finish_reason={}, input_tokens={}, output_tokens={}",
                        finishReason,
                        inputTokens,
                        outputTokens
                );
            }

            JsonNode delta = choice.path("delta");

            /*
             * DeepSeek thinking / reasoning_content。
             */
            if (delta.has("reasoning_content") && delta.get("reasoning_content").isTextual()) {
                String reasoning = delta.get("reasoning_content").asText();

                if (reasoning != null && !reasoning.isEmpty()) {
                    if (thinking.effectiveEnabled && thinking.exposeThinking) {
                        if (!thinkingBlockOpen) {
                            currentThinkingIndex = nextContentIndex++;
                            thinkingBlockOpen = true;
                            events.add(sse("content_block_start", contentBlockStartThinking(currentThinkingIndex)));
                        }

                        appendThinkingDelta(events, reasoning);
                    } else if (properties.isLogStreamChunks()) {
                        log.info("Ignore reasoning_content chunk: {}", limit(reasoning, 1000));
                    }
                }
            }

            /*
             * 最终文本 content。
             *
             * 一旦开始输出最终文本，就关闭 thinking block。
             */
            if (delta.has("content") && delta.get("content").isTextual()) {
                String text = delta.get("content").asText();

                if (!text.isEmpty()) {
                    closeThinkingBlockIfOpen(events);

                    if (!textBlockOpen) {
                        currentTextIndex = nextContentIndex++;
                        textBlockOpen = true;
                        events.add(sse("content_block_start", contentBlockStartText(currentTextIndex)));
                    }

                    appendTextDelta(events, text);
                }
            }

            /*
             * 工具调用。
             *
             * 一旦开始工具调用，也关闭 thinking block 和 text block。
             */
            JsonNode toolCalls = delta.path("tool_calls");
            if (toolCalls.isArray()) {
                closeThinkingBlockIfOpen(events);
                closeTextBlockIfOpen(events);

                for (JsonNode toolCallDelta : toolCalls) {
                    int openAiToolIndex = toolCallDelta.path("index").asInt(0);

                    ToolBlock block = toolBlocks.get(openAiToolIndex);
                    if (block == null) {
                        block = new ToolBlock();
                        block.openAiIndex = openAiToolIndex;
                        block.id = toolCallDelta.path("id").asText("");
                        toolBlocks.put(openAiToolIndex, block);
                    }

                    if (toolCallDelta.has("id") && toolCallDelta.get("id").isTextual()) {
                        String id = toolCallDelta.get("id").asText();
                        if (!id.isBlank()) {
                            block.id = id;
                        }
                    }

                    JsonNode function = toolCallDelta.path("function");

                    if (function.has("name") && function.get("name").isTextual()) {
                        String newName = function.get("name").asText();
                        if (!newName.isBlank()) {
                            block.name = newName;
                        }
                    }

                    if (!block.started && block.name != null && !block.name.isBlank()) {
                        startToolBlock(events, block);
                    }

                    if (function.has("arguments") && function.get("arguments").isTextual()) {
                        String partialJson = function.get("arguments").asText();

                        if (!partialJson.isEmpty()) {
                            if (block.started) {
                                events.add(sse(
                                        "content_block_delta",
                                        contentBlockDeltaToolJson(block.index, partialJson)
                                ));
                            } else {
                                block.pendingArgumentDeltas.add(partialJson);
                            }
                        }
                    }
                }
            }

            return events;
        }

        List<String> finishEvents() {
            if (finished) {
                return Collections.emptyList();
            }

            finished = true;

            List<String> events = new ArrayList<>();

            closeThinkingBlockIfOpen(events);
            closeTextBlockIfOpen(events);

            for (ToolBlock block : toolBlocks.values()) {
                if (!block.started) {
                    if (block.name == null || block.name.isBlank()) {
                        block.name = "unknown_tool";
                    }

                    startToolBlock(events, block);
                }

                if (!block.stopped) {
                    block.stopped = true;
                    events.add(sse("content_block_stop", contentBlockStop(block.index)));
                }
            }

            events.add(sse("message_delta", messageDelta()));
            events.add(sse("message_stop", messageStop()));

            return events;
        }

        private void appendThinkingDelta(List<String> events, String text) {
            if (text == null || text.isEmpty()) {
                return;
            }

            pendingThinkingDelta.append(text);

            if (pendingThinkingDelta.length() >= THINKING_DELTA_FLUSH_CHARS || text.contains("\n")) {
                flushThinkingDelta(events);
            }
        }

        private void flushThinkingDelta(List<String> events) {
            if (pendingThinkingDelta.length() == 0) {
                return;
            }

            events.add(sse(
                    "content_block_delta",
                    contentBlockDeltaThinking(currentThinkingIndex, pendingThinkingDelta.toString())
            ));

            pendingThinkingDelta.setLength(0);
        }

        private void closeThinkingBlockIfOpen(List<String> events) {
            if (thinkingBlockOpen) {
                flushThinkingDelta(events);

                events.add(sse("content_block_stop", contentBlockStop(currentThinkingIndex)));
                thinkingBlockOpen = false;
                currentThinkingIndex = -1;
            }
        }

        private void appendTextDelta(List<String> events, String text) {
            if (text == null || text.isEmpty()) {
                return;
            }

            pendingTextDelta.append(text);

            if (pendingTextDelta.length() >= TEXT_DELTA_FLUSH_CHARS || text.contains("\n")) {
                flushTextDelta(events);
            }
        }

        private void flushTextDelta(List<String> events) {
            if (pendingTextDelta.length() == 0) {
                return;
            }

            events.add(sse(
                    "content_block_delta",
                    contentBlockDeltaText(currentTextIndex, pendingTextDelta.toString())
            ));

            pendingTextDelta.setLength(0);
        }

        private void closeTextBlockIfOpen(List<String> events) {
            if (textBlockOpen) {
                flushTextDelta(events);

                events.add(sse("content_block_stop", contentBlockStop(currentTextIndex)));
                textBlockOpen = false;
                currentTextIndex = -1;
            }
        }

        private void startToolBlock(List<String> events, ToolBlock block) {
            if (block.started) {
                return;
            }

            block.started = true;
            block.index = nextContentIndex++;

            if (block.id == null || block.id.isBlank()) {
                block.id = "toolu_" + UUID.randomUUID();
            }

            if (block.name == null || block.name.isBlank()) {
                block.name = "unknown_tool";
            }

            events.add(sse("content_block_start", contentBlockStartTool(block)));

            if (!block.pendingArgumentDeltas.isEmpty()) {
                for (String pending : block.pendingArgumentDeltas) {
                    events.add(sse(
                            "content_block_delta",
                            contentBlockDeltaToolJson(block.index, pending)
                    ));
                }

                block.pendingArgumentDeltas.clear();
            }
        }

        private JsonNode contentBlockStartThinking(int index) {
            ObjectNode data = mapper.createObjectNode();
            data.put("type", "content_block_start");
            data.put("index", index);

            ObjectNode block = mapper.createObjectNode();
            block.put("type", "thinking");
            block.put("thinking", "");

            data.set("content_block", block);
            return data;
        }

        private JsonNode contentBlockDeltaThinking(int index, String thinkingText) {
            ObjectNode data = mapper.createObjectNode();
            data.put("type", "content_block_delta");
            data.put("index", index);

            ObjectNode delta = mapper.createObjectNode();
            delta.put("type", "thinking_delta");
            delta.put("thinking", thinkingText);

            data.set("delta", delta);
            return data;
        }

        private JsonNode contentBlockStartText(int index) {
            ObjectNode data = mapper.createObjectNode();
            data.put("type", "content_block_start");
            data.put("index", index);

            ObjectNode block = mapper.createObjectNode();
            block.put("type", "text");
            block.put("text", "");

            data.set("content_block", block);
            return data;
        }

        private JsonNode contentBlockDeltaText(int index, String text) {
            ObjectNode data = mapper.createObjectNode();
            data.put("type", "content_block_delta");
            data.put("index", index);

            ObjectNode delta = mapper.createObjectNode();
            delta.put("type", "text_delta");
            delta.put("text", text);

            data.set("delta", delta);
            return data;
        }

        private JsonNode contentBlockStartTool(ToolBlock block) {
            ObjectNode data = mapper.createObjectNode();
            data.put("type", "content_block_start");
            data.put("index", block.index);

            ObjectNode contentBlock = mapper.createObjectNode();
            contentBlock.put("type", "tool_use");
            contentBlock.put("id", block.id);
            contentBlock.put("name", block.name);
            contentBlock.set("input", mapper.createObjectNode());

            data.set("content_block", contentBlock);
            return data;
        }

        private JsonNode contentBlockDeltaToolJson(int index, String partialJson) {
            ObjectNode data = mapper.createObjectNode();
            data.put("type", "content_block_delta");
            data.put("index", index);

            ObjectNode delta = mapper.createObjectNode();
            delta.put("type", "input_json_delta");
            delta.put("partial_json", partialJson);

            data.set("delta", delta);
            return data;
        }

        private JsonNode contentBlockStop(int index) {
            ObjectNode data = mapper.createObjectNode();
            data.put("type", "content_block_stop");
            data.put("index", index);
            return data;
        }

        private JsonNode messageDelta() {
            ObjectNode data = mapper.createObjectNode();
            data.put("type", "message_delta");

            ObjectNode delta = mapper.createObjectNode();
            delta.put("stop_reason", mapStopReason(finishReason));
            delta.putNull("stop_sequence");

            ObjectNode usage = mapper.createObjectNode();
            usage.put("output_tokens", outputTokens);

            data.set("delta", delta);
            data.set("usage", usage);

            return data;
        }

        private JsonNode messageStop() {
            ObjectNode data = mapper.createObjectNode();
            data.put("type", "message_stop");
            return data;
        }
    }

    private static class ToolBlock {
        int openAiIndex;
        int index = -1;
        String id;
        String name;
        boolean started = false;
        boolean stopped = false;
        List<String> pendingArgumentDeltas = new ArrayList<>();
    }

    private static class ThinkingConfig {
        String type;
        String effort;
        boolean sendThinkingParam;
        boolean effectiveEnabled;
        boolean exposeThinking;
    }
    private ThinkingConfig resolveThinking(JsonNode anthropicRequest) {
        ThinkingConfig cfg = new ThinkingConfig();

        String type = normalizeThinkingType(properties.getThinkingType());

        /*
         * 优先尊重 Anthropic 请求里的 thinking。
         *
         * 例如：
         * {
         *   "thinking": {
         *     "type": "enabled"
         *   }
         * }
         */
        JsonNode thinkingNode = anthropicRequest.path("thinking");
        if (thinkingNode.isObject()) {
            String requestType = normalizeThinkingType(thinkingNode.path("type").asText(""));
            if ("enabled".equals(requestType) || "disabled".equals(requestType)) {
                type = requestType;
            }
        }

        boolean hasTools = anthropicRequest.has("tools")
                && anthropicRequest.get("tools").isArray()
                && !anthropicRequest.get("tools").isEmpty();

        /*
         * Anthropic 格式思考强度：
         * {
         *   "output_config": {
         *     "effort": "high"
         *   }
         * }
         */
        String effort = null;

        JsonNode outputConfig = anthropicRequest.path("output_config");
        if (outputConfig.isObject()) {
            effort = outputConfig.path("effort").asText(null);
        }

        /*
         * 兼容某些客户端直接传 reasoning_effort。
         */
        if (effort == null || effort.isBlank()) {
            effort = anthropicRequest.path("reasoning_effort").asText(null);
        }

        if (effort == null || effort.isBlank()) {
            effort = hasTools
                    ? properties.getAgentReasoningEffort()
                    : properties.getReasoningEffort();
        }

        effort = normalizeReasoningEffort(effort);

        cfg.type = type;
        cfg.effort = effort;
        cfg.exposeThinking = properties.isExposeThinking();

        /*
         * disabled/enabled：显式发送 thinking 参数。
         * auto：不发送 thinking 参数，使用 DeepSeek 默认行为。
         */
        cfg.sendThinkingParam = "enabled".equals(type) || "disabled".equals(type);

        /*
         * effectiveEnabled 用于决定是否处理 reasoning_content。
         *
         * auto 下，DeepSeek 默认是 enabled，所以这里按 true 处理。
         */
        cfg.effectiveEnabled = "enabled".equals(type) || "auto".equals(type);

        return cfg;
    }

    private String normalizeThinkingType(String type) {
        if (type == null || type.isBlank()) {
            return "disabled";
        }

        String t = type.trim().toLowerCase(Locale.ROOT);

        if ("enabled".equals(t) || "enable".equals(t) || "on".equals(t) || "true".equals(t)) {
            return "enabled";
        }

        if ("disabled".equals(t) || "disable".equals(t) || "off".equals(t) || "false".equals(t)) {
            return "disabled";
        }

        if ("auto".equals(t)) {
            return "auto";
        }

        return "disabled";
    }

    private String normalizeReasoningEffort(String effort) {
        if (effort == null || effort.isBlank()) {
            return "high";
        }

        String e = effort.trim().toLowerCase(Locale.ROOT);

        return switch (e) {
            case "low", "medium", "high" -> "high";
            case "xhigh", "max" -> "max";
            default -> "high";
        };
    }
    private String newRequestId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private int logBodyMaxChars() {
        int max = properties.getLogBodyMaxChars();
        return max > 0 ? max : 12000;
    }

    private String upstreamChatCompletionsUrl() {
        return removeTrailingSlash(properties.getDeepseekBaseUrl()) + "/chat/completions";
    }

    private void logDeepSeekRequest(String requestId, boolean stream, ObjectNode payload) {
        log.info(
                "[{}] DeepSeek request start: stream={}, url={}, model={}",
                requestId,
                stream,
                upstreamChatCompletionsUrl(),
                payload.path("model").asText("")
        );

        if (properties.isLogUpstreamRequest()) {
            log.info(
                    "[{}] DeepSeek request payload={}",
                    requestId,
                    limit(toJsonString(payload), logBodyMaxChars())
            );
        }
    }

    private void logDeepSeekRawBody(String requestId, String scene, String body) {
        if (properties.isLogUpstreamResponse()) {
            log.info(
                    "[{}] DeepSeek {} raw body={}",
                    requestId,
                    scene,
                    limit(body, logBodyMaxChars())
            );
        }
    }

    private void logDeepSeekStreamEvent(String requestId, ServerSentEvent<String> event) {
        if (properties.isLogUpstreamResponse() || properties.isLogStreamChunks()) {
            log.info(
                    "[{}] DeepSeek stream raw event: event={}, id={}, data={}",
                    requestId,
                    event.event(),
                    event.id(),
                    limit(event.data(), logBodyMaxChars())
            );
        }
    }

    private void logAnthropicRequestSummary(String requestId, JsonNode anthropicRequest) {
        JsonNode messages = anthropicRequest.path("messages");
        JsonNode tools = anthropicRequest.path("tools");

        log.info(
                "[{}] Anthropic request received: stream={}, model={}, max_tokens={}, messages={}, tools={}, thinking={}, output_config={}",
                requestId,
                anthropicRequest.path("stream").asBoolean(false),
                anthropicRequest.path("model").asText(""),
                anthropicRequest.path("max_tokens").asText(""),
                messages.isArray() ? messages.size() : 0,
                tools.isArray() ? tools.size() : 0,
                anthropicRequest.has("thinking") ? limit(anthropicRequest.path("thinking").toString(), 500) : "null",
                anthropicRequest.has("output_config") ? limit(anthropicRequest.path("output_config").toString(), 500) : "null"
        );
    }
}