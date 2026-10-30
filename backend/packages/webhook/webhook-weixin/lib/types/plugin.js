/** 微信 iLink Cordis 插件：校验配置并把连接器生命周期交给 effect。 */
import z from '@deepseek-ai/schemastery';
import { probe } from "./diagnostics/probe.js";
import { install_weixin_connector } from "./runtime/weixinConnector.js";
/** Cordis function-plugin 名称。 */
export const name = 'webhook-weixin';
/**
 * 连接器所需的 Harness 原生服务。
 * 重构设计要求（§8.2）：依赖必须在装配期声明，缺失时**启动即失败**，
 * 而不是等第一条消息到达才暴露。
 */
export const inject = ['agents', 'agentPresets', 'sessionPersistence'];
/** Schemastery 配置定义，保持模型/API 配置与代码分离。 */
export const Config = z.object({
    base_url: z.string().required(),
    token: z.string().role('secret').required(),
    cdn_base_url: z.string(),
    state_file: z.string(),
    workspace_path: z.string().required(),
    provider: z.string().required(),
    model: z.string().required(),
    reasoning_effort: z.string(),
    agent_preset: z.string(),
    send_retry_count: z.number(),
    send_retry_delay_ms: z.number(),
    state_db: z.string(),
    lease_marker: z.string(),
});
/** 注册微信连接器，并把长轮询与生命周期交给 Cordis effect。 */
export function apply(ctx, config) {
    if (!config.base_url.startsWith('http'))
        throw new Error('webhook-weixin base_url must be an HTTP URL');
    if (!config.token.trim())
        throw new Error('webhook-weixin token must be non-empty');
    probe('lifecycle', 'plugin.apply', {
        base_url: config.base_url,
        preset: config.agent_preset ?? '(default)',
        provider: config.provider,
        model: config.model,
        state_db: config.state_db ?? '(default)',
    });
    install_weixin_connector(ctx, config);
}
//# sourceMappingURL=plugin.js.map