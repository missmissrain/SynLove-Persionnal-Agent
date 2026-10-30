/**
 * 微信链路的统一领域模型：入站信封、控制命令、delivery 状态、outbox 记录。
 *
 * 设计原则（对应重构设计文档 §3.2）：一个事实只能有一个权威写入者。
 * - 执行状态（inbox/turn/step/cancel/steer/transcript）由 Harness Agent/Session 唯一持有；
 * - 本文件只描述微信协议侧必需的幂等键、路由、游标与发送状态。
 * @module dsh-webhook-weixin/domain
 */
/** 该状态是否允许提交其所在批次的 cursor（设计文档 §5.2）。 */
export function is_cursor_committable(status) {
    return status === 'injected' || status === 'control_applied' || status === 'failed_terminal';
}
//# sourceMappingURL=domain.js.map