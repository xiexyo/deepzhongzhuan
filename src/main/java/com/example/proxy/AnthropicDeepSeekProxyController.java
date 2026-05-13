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
        return Map.of(
                "status", "ok",
                "upstream", properties.getDeepseekBaseUrl(),
                "model", properties.getDeepseekModel(),
                "forwardTools", properties.isForwardTools(),
                "forwardToolChoice", properties.isForwardToolChoice(),
                "maxTokensLimit", properties.getMaxTokensLimit()
        );
    }

    /**
     * Claude Code / Anthropic Messages API 入口
     */
    @PostMapping("/v1/messages")
    public Mono<ResponseEntity<?>> messages(@RequestBody JsonNode anthropicRequest) {
        boolean stream = anthropicRequest.path("stream").asBoolean(false);

        if (stream) {
            Flux<ServerSentEvent<String>> flux = streamMessages(anthropicRequest);

            return Mono.just(
                    ResponseEntity.ok()
                            .contentType(MediaType.TEXT_EVENT_STREAM)
                            .body(flux)
            );
        }

        ObjectNode deepseekPayload = toDeepSeekPayload(anthropicRequest, false);

        return deepseekClient.post()
                .uri("/chat/completions")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue(deepseekPayload)
                .exchangeToMono(response -> {
                    HttpStatusCode status = response.statusCode();

                    if (status.isError()) {
                        return response.bodyToMono(String.class)
                                .defaultIfEmpty("")
                                .map(body -> {
                                    log.error(
                                            "DeepSeek upstream error, status={}, body={}, payload={}",
                                            status,
                                            limit(body, 4000),
                                            limit(toJsonString(deepseekPayload), 8000)
                                    );

                                    ObjectNode error = anthropicErrorResponse(
                                            "upstream_error",
                                            "DeepSeek upstream returned " + status + ": " + limit(body, 2000)
                                    );

                                    return (ResponseEntity<?>) ResponseEntity
                                            .status(HttpStatus.BAD_GATEWAY)
                                            .contentType(MediaType.APPLICATION_JSON)
                                            .body(error);
                                });
                    }

                    return response.bodyToMono(JsonNode.class)
                            .map(deepseekResponse -> {
                                ObjectNode anthropicResponse =
                                        toAnthropicResponse(anthropicRequest, deepseekResponse);

                                return (ResponseEntity<?>) ResponseEntity.ok()
                                        .contentType(MediaType.APPLICATION_JSON)
                                        .body(anthropicResponse);
                            });
                })
                .onErrorResume(e -> {
                    log.error("Proxy non-stream request failed, payload={}",
                            limit(toJsonString(deepseekPayload), 8000), e);

                    ObjectNode error = anthropicErrorResponse(
                            "proxy_error",
                            "Proxy request failed: " + e.getMessage()
                    );

                    return Mono.just(
                            ResponseEntity.status(HttpStatus.BAD_GATEWAY)
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

        payload.put("model", properties.getDeepseekModel());
        payload.put("stream", stream);

        if (anthropicRequest.has("max_tokens")) {
            JsonNode maxTokensNode = anthropicRequest.get("max_tokens");
            Integer limit = properties.getMaxTokensLimit();

            if (limit != null && limit > 0 && maxTokensNode.canConvertToInt()) {
                payload.put("max_tokens", Math.min(maxTokensNode.asInt(), limit));
            } else {
                payload.set("max_tokens", maxTokensNode);
            }
        }

        if (anthropicRequest.has("temperature")) {
            payload.set("temperature", anthropicRequest.get("temperature"));
        }

        if (anthropicRequest.has("top_p")) {
            payload.set("top_p", anthropicRequest.get("top_p"));
        }

        if (anthropicRequest.has("stop_sequences")
                && anthropicRequest.get("stop_sequences").isArray()
                && !anthropicRequest.get("stop_sequences").isEmpty()) {
            payload.set("stop", anthropicRequest.get("stop_sequences"));
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
     * 关键修复：
     * OpenAI / DeepSeek 要求 role=tool 的消息必须紧跟在带 tool_calls 的 assistant 消息后。
     * 所以如果一个 Anthropic user content 里同时有 text 和 tool_result，
     * 必须先输出 tool 消息，再输出普通 user 文本。
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

        return result;
    }

    /**
     * 流式处理：OpenAI SSE -> Anthropic SSE
     */
    private Flux<ServerSentEvent<String>> streamMessages(JsonNode anthropicRequest) {
        ObjectNode deepseekPayload = toDeepSeekPayload(anthropicRequest, true);

        StreamState state = new StreamState(
                mapper,
                anthropicRequest.path("model").asText(properties.getDeepseekModel())
        );

        return deepseekClient.post()
                .uri("/chat/completions")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(deepseekPayload)
                .exchangeToFlux(response -> {
                    HttpStatusCode status = response.statusCode();

                    if (status.isError()) {
                        return response.bodyToMono(String.class)
                                .defaultIfEmpty("")
                                .flatMapMany(body -> {
                                    log.error(
                                            "DeepSeek upstream stream error, status={}, body={}, payload={}",
                                            status,
                                            limit(body, 4000),
                                            limit(toJsonString(deepseekPayload), 8000)
                                    );

                                    return Flux.just(
                                            sse("error", anthropicErrorResponse(
                                                    "upstream_error",
                                                    "DeepSeek upstream returned " + status + ": " + limit(body, 2000)
                                            ))
                                    );
                                });
                    }

                    Flux<ServerSentEvent<String>> start = Flux.just(
                            sse("message_start", state.messageStart())
                    );

                    Flux<ServerSentEvent<String>> body = response
                            .bodyToFlux(new ParameterizedTypeReference<ServerSentEvent<String>>() {
                            })
                            .filter(event -> event.data() != null)
                            .concatMap(event ->
                                    Flux.fromIterable(state.onDeepSeekSseData(event.data()))
                            );

                    Flux<ServerSentEvent<String>> end = Flux.defer(() ->
                            Flux.fromIterable(state.finishEvents())
                    );

                    return start.concatWith(body).concatWith(end);
                })
                .onErrorResume(e -> {
                    log.error("Proxy stream request failed, payload={}",
                            limit(toJsonString(deepseekPayload), 8000), e);

                    return Flux.just(
                            sse("error", anthropicErrorResponse(
                                    "proxy_error",
                                    "Proxy stream request failed: " + e.getMessage()
                            ))
                    );
                });
    }

    private ServerSentEvent<String> sse(String eventName, JsonNode data) {
        return ServerSentEvent.<String>builder(toJsonString(data))
                .event(eventName)
                .build();
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

        private boolean textStarted = false;
        private boolean textStopped = false;
        private int textIndex = -1;

        private int nextContentIndex = 0;
        private String finishReason = "stop";
        private int outputTokens = 0;

        private final Map<Integer, ToolBlock> toolBlocks = new LinkedHashMap<>();

        StreamState(ObjectMapper mapper, String model) {
            this.mapper = mapper;
            this.model = model;
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

        List<ServerSentEvent<String>> onDeepSeekSseData(String rawData) {
            List<ServerSentEvent<String>> events = new ArrayList<>();

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
            }

            JsonNode delta = choice.path("delta");

            if (delta.has("content") && delta.get("content").isTextual()) {
                String text = delta.get("content").asText();

                if (!text.isEmpty()) {
                    if (!textStarted) {
                        textStarted = true;
                        textIndex = nextContentIndex++;

                        events.add(sse("content_block_start", contentBlockStartText(textIndex)));
                    }

                    events.add(sse("content_block_delta", contentBlockDeltaText(textIndex, text)));
                }
            }

            JsonNode toolCalls = delta.path("tool_calls");
            if (toolCalls.isArray()) {
                if (textStarted && !textStopped) {
                    textStopped = true;
                    events.add(sse("content_block_stop", contentBlockStop(textIndex)));
                }

                for (JsonNode toolCallDelta : toolCalls) {
                    int openAiToolIndex = toolCallDelta.path("index").asInt(0);

                    ToolBlock block = toolBlocks.get(openAiToolIndex);
                    if (block == null) {
                        String id = toolCallDelta.path("id").asText("toolu_" + UUID.randomUUID());
                        String name = toolCallDelta.path("function").path("name").asText("unknown_tool");

                        block = new ToolBlock();
                        block.index = nextContentIndex++;
                        block.id = id;
                        block.name = name;

                        toolBlocks.put(openAiToolIndex, block);

                        events.add(sse("content_block_start", contentBlockStartTool(block)));
                    } else {
                        JsonNode function = toolCallDelta.path("function");
                        if (function.has("name") && function.get("name").isTextual()) {
                            String newName = function.get("name").asText();
                            if (!newName.isBlank()) {
                                block.name = newName;
                            }
                        }
                    }

                    JsonNode function = toolCallDelta.path("function");
                    if (function.has("arguments") && function.get("arguments").isTextual()) {
                        String partialJson = function.get("arguments").asText();

                        if (!partialJson.isEmpty()) {
                            events.add(sse(
                                    "content_block_delta",
                                    contentBlockDeltaToolJson(block.index, partialJson)
                            ));
                        }
                    }
                }
            }

            return events;
        }

        List<ServerSentEvent<String>> finishEvents() {
            List<ServerSentEvent<String>> events = new ArrayList<>();

            if (textStarted && !textStopped) {
                textStopped = true;
                events.add(sse("content_block_stop", contentBlockStop(textIndex)));
            }

            for (ToolBlock block : toolBlocks.values()) {
                if (!block.stopped) {
                    block.stopped = true;
                    events.add(sse("content_block_stop", contentBlockStop(block.index)));
                }
            }

            events.add(sse("message_delta", messageDelta()));
            events.add(sse("message_stop", messageStop()));

            return events;
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
        int index;
        String id;
        String name;
        boolean stopped = false;
    }
}