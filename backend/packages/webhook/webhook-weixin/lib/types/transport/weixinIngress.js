/**
 * 微信入站传输层：只负责 HTTP 长轮询、协议错误分类、退避与规范化。
 *
 * 重构设计文档 §3.2：WeixinIngress **不**创建 Agent、**不**保存回复上下文、
 * **不**处理工具调用、**不**决定 Session 生命周期；它只把"一批原始消息"变成"一批规范化信封"。
 *
 * 状态归属：本模块只维护 HTTP 重试计数，不写任何持久状态（cursor 由协调器在批次完成后提交）。
 * @module dsh-webhook-weixin/ingress
 */
import { probe } from "../diagnostics/probe.js";
import { MessageNormalizer } from "../message/messageNormalizer.js";
/** iLink getUpdates 的传输封装。 */
export class WeixinIngress {
    normalizer = new MessageNormalizer();
    client;
    observer;
    /** 绑定协议客户端与观察器。 */
    constructor(options) {
        this.client = options.client;
        this.observer = options.observer ?? {};
    }
    /** 通知服务端消费者上线；失败抛出，由协调器决定是否致命。 */
    async notify_start(signal) {
        await this.client.notify_start(signal);
        probe('ingress', 'notify_start.ok');
    }
    /** 通知服务端消费者下线；尽力而为。 */
    async notify_stop(signal) {
        try {
            await this.client.notify_stop(signal);
            probe('ingress', 'notify_stop.ok');
        }
        catch (error) {
            probe('ingress', 'notify_stop.failed', { error: String(error) });
        }
    }
    /**
     * 执行一轮长轮询并规范化消息。
     *
     * 分类而非抛错：把"可继续"（超时/网络抖动）与"必须停止"（鉴权失效/未知协议）
     * 明确区分，交给协调器按设计文档 §10.2 处理，避免无限重试风暴。
     */
    async poll(cursor, signal) {
        let response;
        try {
            response = await this.client.get_updates(cursor, signal);
        }
        catch (error) {
            if (signal?.aborted)
                throw error;
            const aborted = error instanceof Error && error.name === 'AbortError';
            if (aborted) {
                probe('ingress', 'poll.long_poll_timeout');
                return { kind: 'session_timeout' };
            }
            probe('ingress', 'poll.network_error', { error: String(error) });
            this.observer.on_error?.(error);
            // 网络错误由协调器退避重试，cursor 不前进；这里用 protocol_error 表达"本轮无结果"。
            return { kind: 'protocol_error', detail: error instanceof Error ? error.message : String(error) };
        }
        const errmsg = response.errmsg?.toLowerCase() ?? '';
        if (errmsg.includes('session timeout')) {
            probe('ingress', 'poll.session_timeout');
            return { kind: 'session_timeout' };
        }
        const ret = response.ret ?? 0;
        const errcode = response.errcode ?? 0;
        if (ret !== 0 || errcode !== 0) {
            const detail = response.errmsg ?? String(ret !== 0 ? ret : errcode);
            if (ret === 401 || ret === 403 || errcode === 401 || errcode === 403
                || /token|auth|unauthor|login|expired/i.test(detail)) {
                probe('ingress', 'poll.auth_required', { ret, errcode, detail });
                this.observer.on_status?.('auth_required', detail);
                return { kind: 'auth_required', detail };
            }
            probe('ingress', 'poll.protocol_error', { ret, errcode, detail });
            this.observer.on_error?.(new Error(`iLink getUpdates failed: ${detail}`));
            return { kind: 'protocol_error', detail };
        }
        const raw = response.msgs ?? [];
        const cursor_next = response.get_updates_buf;
        if (raw.length === 0) {
            probe('ingress', 'poll.idle');
            return { kind: 'idle', cursor: cursor_next };
        }
        const messages = raw.map((message) => this.normalizer.normalize_message(message));
        probe('ingress', 'poll.messages', {
            count: messages.length,
            users: [...new Set(messages.map(m => m.user_id))],
            delivery_ids: messages.map(m => m.delivery_id),
        });
        return { kind: 'messages', messages, cursor: cursor_next };
    }
    /** 下载媒体（供协调器的分片调度调用，不阻塞轮询）。 */
    async download_media(media) {
        return this.client.download_media(media);
    }
    /** 暴露规范化器的媒体补全能力，供协调器在分片任务中使用。 */
    get media_normalizer() {
        return this.normalizer;
    }
}
//# sourceMappingURL=weixinIngress.js.map