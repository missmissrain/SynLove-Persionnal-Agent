/**
 * QQ 机器人 WebSocket 网关（官方 v2）。
 *
 * 流程：`GET /gateway` 取 wss 地址 → 连上后收 `op=10 Hello`（带心跳间隔）→
 * 发 `op=2 Identify`（`token = "QQBot <access_token>"` + intents + shard）→
 * 按间隔发 `op=1 心跳` → 收 `op=0 Dispatch` 业务事件。
 *
 * 断线处理：`op=7 Reconnect` 或 socket 关闭 → 指数退避重连，优先 `op=6 Resume`
 * （带上次的 session_id + seq，平台会补发漏掉的事件）。
 *
 * @module dsh-webhook-qq/gateway
 */
/** 默认订阅：群 @ + C2C 单聊消息。 */
const DEFAULT_INTENTS = 1 << 25;
/** WebSocket 网关客户端。 */
export class QqGateway {
    options;
    socket;
    heartbeat;
    seq = 0;
    sessionId = '';
    stopped = false;
    attempt = 0;
    /** 连续被平台踢（op=7）的时间戳；短时间内太多就冷却，避免无限重连战争。 */
    kickTimes = [];
    /** 冷却截止时间（毫秒时间戳）；期间不重连。 */
    cooldownUntil = 0;
    constructor(options) {
        this.options = options;
    }
    /** 启动（立即连接，断线自动重连）。 */
    start() {
        this.stopped = false;
        void this.connect();
    }
    /** 停止（不再重连）。 */
    stop() {
        this.stopped = true;
        if (this.heartbeat !== undefined)
            clearInterval(this.heartbeat);
        this.heartbeat = undefined;
        try {
            this.socket?.close();
        }
        catch { /* ignore */ }
        this.socket = undefined;
    }
    /** 建一次连接；失败就按退避重试。 */
    async connect() {
        if (this.stopped)
            return;
        const log = this.options.log ?? (() => { });
        try {
            const base = this.options.baseUrl ?? 'https://api.bot.qq.com';
            const token = await this.options.token();
            const response = await fetch(`${base}/gateway`, {
                headers: { Authorization: `QQBot ${token}` },
                signal: AbortSignal.timeout(15_000),
            });
            if (!response.ok)
                throw new Error(`GET /gateway HTTP ${response.status}`);
            const body = await response.json();
            if (typeof body.url !== 'string' || body.url === '')
                throw new Error('GET /gateway 没返回 url');
            const socket = new WebSocket(body.url);
            this.socket = socket;
            socket.addEventListener('open', () => {
                log('gateway 已连接');
            });
            socket.addEventListener('message', (event) => {
                void this.handle_frame(typeof event.data === 'string' ? event.data : String(event.data));
            });
            socket.addEventListener('close', () => {
                if (this.heartbeat !== undefined)
                    clearInterval(this.heartbeat);
                this.heartbeat = undefined;
                if (this.stopped)
                    return;
                const backoff = Math.min(30_000, 1_000 * 2 ** this.attempt);
                const cooldown = Math.max(0, this.cooldownUntil - Date.now());
                const delay = Math.max(backoff, cooldown);
                this.attempt += 1;
                log(`gateway 断开，${Math.round(delay / 1000)}s 后重连（第 ${this.attempt} 次）`);
                setTimeout(() => { void this.connect(); }, delay);
            });
            socket.addEventListener('error', () => {
                // close 事件会随后触发，这里只记录
                log('gateway 连接错误（等 close 后重连）');
            });
        }
        catch (error) {
            if (this.stopped)
                return;
            const delay = Math.min(30_000, 1_000 * 2 ** this.attempt);
            this.attempt += 1;
            log(`gateway 连接失败：${String(error).slice(0, 120)}；${Math.round(delay / 1000)}s 后重试`);
            setTimeout(() => { void this.connect(); }, delay);
        }
    }
    /** 处理一帧：hello / dispatch / 心跳确认 / 重连 / 失效会话。 */
    async handle_frame(raw) {
        const log = this.options.log ?? (() => { });
        let frame;
        try {
            frame = JSON.parse(raw);
        }
        catch {
            return;
        }
        switch (frame.op) {
            case 10: {
                const interval = Number(frame.d?.heartbeat_interval ?? 30_000);
                this.start_heartbeat(interval);
                await this.identify();
                return;
            }
            case 11:
                return;
            case 0: {
                if (typeof frame.s === 'number')
                    this.seq = frame.s;
                const kind = frame.t;
                if (kind === 'READY') {
                    const session = frame.d?.session_id;
                    if (typeof session === 'string')
                        this.sessionId = session;
                    this.attempt = 0;
                    log('gateway READY');
                    return;
                }
                if (kind === 'C2C_MESSAGE_CREATE' || kind === 'GROUP_AT_MESSAGE_CREATE') {
                    this.options.on_event({ t: kind, d: (frame.d ?? {}) });
                }
                return;
            }
            case 7: {
                log('gateway 要求重连（op=7），清空会话后重新 identify');
                this.sessionId = '';
                this.seq = 0;
                // 短时间内被连续踢：说明旧连接还挂在平台侧，继续每秒重连只会互相踢。
                // 冷却 5 分钟，让平台把僵尸会话清掉，再重新 identify。
                const now = Date.now();
                this.kickTimes = this.kickTimes.filter(at => now - at < 10 * 60 * 1000);
                this.kickTimes.push(now);
                if (this.kickTimes.length >= 5) {
                    this.cooldownUntil = now + 5 * 60 * 1000;
                    this.kickTimes = [];
                    log('gateway 连续被踢 5 次，冷却 5 分钟后再连（避免重连战争）');
                }
                try {
                    this.socket?.close();
                }
                catch { /* ignore */ }
                return;
            }
            case 9: {
                log('gateway 会话失效（op=9），重新 identify');
                this.sessionId = '';
                this.seq = 0;
                await this.identify();
                return;
            }
            default:
                return;
        }
    }
    /** 启动心跳（d 带上次的 seq）。 */
    start_heartbeat(interval) {
        if (this.heartbeat !== undefined)
            clearInterval(this.heartbeat);
        const send = () => {
            try {
                this.socket?.send(JSON.stringify({ op: 1, d: this.seq }));
            }
            catch { /* 连接已断，close 会接管重连 */ }
        };
        send();
        this.heartbeat = setInterval(send, Math.max(5_000, interval));
    }
    /** 发 identify / resume。 */
    async identify() {
        const token = await this.options.token();
        const payload = this.sessionId === ''
            ? { op: 2, d: { token: `QQBot ${token}`, intents: this.options.intents ?? DEFAULT_INTENTS, shard: [0, 1], properties: {} } }
            : { op: 6, d: { token: `QQBot ${token}`, session_id: this.sessionId, seq: this.seq } };
        try {
            this.socket?.send(JSON.stringify(payload));
        }
        catch (error) {
            (this.options.log ?? (() => { }))(`gateway 发送 ${this.sessionId === '' ? 'identify' : 'resume'} 失败：${String(error).slice(0, 100)}`);
        }
    }
}
//# sourceMappingURL=gateway.js.map