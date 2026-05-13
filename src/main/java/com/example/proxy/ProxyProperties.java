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
     * 可选：限制 max_tokens，避免 Claude Code 传入过大的 max_tokens 导致上游 400。
     * 如果为 null，则不限制。
     */
    private Integer maxTokensLimit;

    /**
     * 是否转发 tools。
     * 如果你的上游 OpenAI 兼容接口不支持 tools，可以改成 false。
     */
    private boolean forwardTools = true;

    /**
     * 是否转发 tool_choice。
     * 有些 OpenAI 兼容服务支持 tools，但不支持 tool_choice=required/none。
     * 如果仍然 400，可以先把这个设为 false。
     */
    private boolean forwardToolChoice = true;

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
}