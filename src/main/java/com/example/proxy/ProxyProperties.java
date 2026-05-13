package com.example.proxy;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "proxy")
public class ProxyProperties {

    /**
     * 例如：http://your-internal-deepseek:8000/v1
     */
    private String deepseekBaseUrl;

    /**
     * 内部 DeepSeek API Key
     */
    private String deepseekApiKey;

    /**
     * 实际发送给 DeepSeek 的模型名
     */
    private String deepseekModel;

    /**
     * 如果 Claude / VSCode 插件没有传 max_tokens，则使用这个默认值。
     *
     * 写代码场景不要太小，建议 4096 或 8192。
     */
    private Integer defaultMaxTokens = 4096;

    /**
     * 可选：限制 max_tokens，避免 Claude Code 传入过大的 max_tokens 导致上游 400。
     *
     * 如果为 null，则不限制。
     *
     * 建议：
     * - 普通代码场景：8192
     * - 上游支持长输出：16384
     */
    private Integer maxTokensLimit = 8192;

    /**
     * 是否转发 tools。
     *
     * Claude Code / VSCode 插件写代码时通常需要 tools，
     * 所以建议 true。
     *
     * 如果你的上游 OpenAI 兼容接口不支持 tools，可以改成 false。
     */
    private boolean forwardTools = true;

    /**
     * 是否转发 tool_choice。
     *
     * 有些 OpenAI 兼容服务支持 tools，但不支持 tool_choice=required/none。
     * 如果上游 400，可以先把这个设为 false。
     */
    private boolean forwardToolChoice = true;

    /**
     * 是否转发 Anthropic stop_sequences 到 OpenAI/DeepSeek stop。
     *
     * 重点：
     * Claude/Anthropic 的 stop_sequences 不一定适合 DeepSeek。
     * VSCode 插件里出现“输出几个字符就截断”，经常和 stop_sequences 有关。
     *
     * 建议默认 false。
     */
    private boolean forwardStopSequences = false;

    /**
     * 流式请求时是否加：
     *
     * stream_options: {
     *   include_usage: true
     * }
     *
     * 有些 OpenAI-compatible 服务不支持这个字段。
     * 如果上游报 400，就改成 false。
     */
    private boolean includeUsage = true;

    /**
     * 是否打印上游流式 chunk。
     *
     * 排查截断问题时可以临时开 true。
     * 正常使用建议 false，避免日志过大。
     */
    private boolean logStreamChunks = false;
    /**
     * 思考模式：
     *
     * disabled：代理显式关闭 DeepSeek thinking，最稳，推荐默认
     * enabled：代理显式开启 DeepSeek thinking
     * auto：不传 thinking 参数，使用 DeepSeek 默认行为
     */
    private String thinkingType = "disabled";

    /**
     * 普通请求思考强度。
     *
     * DeepSeek 文档支持 high / max。
     * low / medium 会按 high 处理。
     * xhigh 会按 max 处理。
     */
    private String reasoningEffort = "high";

    /**
     * Agent / Claude Code / tools 场景下默认思考强度。
     */
    private String agentReasoningEffort = "max";

    /**
     * 是否把 DeepSeek reasoning_content 暴露成 Anthropic thinking block。
     *
     * 如果 thinking enabled，建议 true。
     * 如果 false，则只给用户最终答案。
     */
    private boolean exposeThinking = true;
    /**
     * 是否打印转发给 DeepSeek 的完整请求 payload。
     * 注意：里面会包含用户 prompt、代码内容、系统提示词，生产环境慎开。
     */
    private boolean logUpstreamRequest = false;

    /**
     * 是否打印 DeepSeek 返回的原始响应。
     * 非流式会打印完整 raw body。
     * 流式会打印每个 SSE chunk。
     */
    private boolean logUpstreamResponse = false;

    /**
     * 日志里单段 body 最大打印字符数。
     */
    private int logBodyMaxChars = 12000;
    public boolean isLogUpstreamRequest() {
        return logUpstreamRequest;
    }

    public void setLogUpstreamRequest(boolean logUpstreamRequest) {
        this.logUpstreamRequest = logUpstreamRequest;
    }

    public boolean isLogUpstreamResponse() {
        return logUpstreamResponse;
    }

    public void setLogUpstreamResponse(boolean logUpstreamResponse) {
        this.logUpstreamResponse = logUpstreamResponse;
    }

    public int getLogBodyMaxChars() {
        return logBodyMaxChars;
    }

    public void setLogBodyMaxChars(int logBodyMaxChars) {
        this.logBodyMaxChars = logBodyMaxChars;
    }
    public String getThinkingType() {
        return thinkingType;
    }

    public void setThinkingType(String thinkingType) {
        this.thinkingType = thinkingType;
    }

    public String getReasoningEffort() {
        return reasoningEffort;
    }

    public void setReasoningEffort(String reasoningEffort) {
        this.reasoningEffort = reasoningEffort;
    }

    public String getAgentReasoningEffort() {
        return agentReasoningEffort;
    }

    public void setAgentReasoningEffort(String agentReasoningEffort) {
        this.agentReasoningEffort = agentReasoningEffort;
    }

    public boolean isExposeThinking() {
        return exposeThinking;
    }

    public void setExposeThinking(boolean exposeThinking) {
        this.exposeThinking = exposeThinking;
    }
    public String getDeepseekBaseUrl() {
        return deepseekBaseUrl;
    }

    public void setDeepseekBaseUrl(String deepseekBaseUrl) {
        this.deepseekBaseUrl = deepseekBaseUrl;
    }

    public String getDeepseekApiKey() {
        return deepseekApiKey;
    }

    public void setDeepseekApiKey(String deepseekApiKey) {
        this.deepseekApiKey = deepseekApiKey;
    }

    public String getDeepseekModel() {
        return deepseekModel;
    }

    public void setDeepseekModel(String deepseekModel) {
        this.deepseekModel = deepseekModel;
    }

    public Integer getDefaultMaxTokens() {
        return defaultMaxTokens;
    }

    public void setDefaultMaxTokens(Integer defaultMaxTokens) {
        this.defaultMaxTokens = defaultMaxTokens;
    }

    public Integer getMaxTokensLimit() {
        return maxTokensLimit;
    }

    public void setMaxTokensLimit(Integer maxTokensLimit) {
        this.maxTokensLimit = maxTokensLimit;
    }

    public boolean isForwardTools() {
        return forwardTools;
    }

    public void setForwardTools(boolean forwardTools) {
        this.forwardTools = forwardTools;
    }

    public boolean isForwardToolChoice() {
        return forwardToolChoice;
    }

    public void setForwardToolChoice(boolean forwardToolChoice) {
        this.forwardToolChoice = forwardToolChoice;
    }

    public boolean isForwardStopSequences() {
        return forwardStopSequences;
    }

    public void setForwardStopSequences(boolean forwardStopSequences) {
        this.forwardStopSequences = forwardStopSequences;
    }

    public boolean isIncludeUsage() {
        return includeUsage;
    }

    public void setIncludeUsage(boolean includeUsage) {
        this.includeUsage = includeUsage;
    }

    public boolean isLogStreamChunks() {
        return logStreamChunks;
    }

    public void setLogStreamChunks(boolean logStreamChunks) {
        this.logStreamChunks = logStreamChunks;
    }
}