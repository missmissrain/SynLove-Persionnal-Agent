/**
 * QQ 官方机器人 access_token 提供者。
 *
 * 官方规则：`POST https://api.bot.qq.com/app/getAppAccessToken`（appId + clientSecret）
 * 返回 `{ access_token, expires_in }``，有效期默认 **7200 秒（2 小时）**；
 * 有效期内重复获取返回同一个值，过期后必须自己刷新。
 *
 * 这里做两件事：缓存 + 提前 60 秒刷新（官方**在过期前 60 秒内**取会拿到新 token，
 * 旧的这 60 秒仍可用，正好可以平滑过渡）。
 *
 * @module dsh-webhook-qq/token
 */
/** access_token 提供者：缓存 + 提前刷新。 */
export class QqTokenProvider {
    options;
    token = '';
    expiresAt = 0;
    pending;
    constructor(options) {
        this.options = options;
    }
    /** 取一个可用 token（必要时刷新；并发调用只发一次请求）。 */
    async get() {
        if (this.token !== '' && Date.now() < this.expiresAt - 60_000)
            return this.token;
        this.pending ??= this.fetch_token().finally(() => { this.pending = undefined; });
        return this.pending;
    }
    /** 真正去换 token；失败直接抛（调用方按通道故障处理）。 */
    async fetch_token() {
        const base = this.options.baseUrl ?? 'https://api.bot.qq.com';
        const response = await fetch(`${base}/app/getAppAccessToken`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ appId: this.options.appId, clientSecret: this.options.clientSecret }),
            signal: AbortSignal.timeout(15_000),
        });
        if (!response.ok)
            throw new Error(`QQ getAppAccessToken HTTP ${response.status}`);
        const data = await response.json();
        if (typeof data.access_token !== 'string' || data.access_token === '') {
            throw new Error(`QQ getAppAccessToken failed: ${data.message ?? 'no access_token'}`);
        }
        const ttl = Number(data.expires_in ?? 7200);
        this.token = data.access_token;
        this.expiresAt = Date.now() + (Number.isFinite(ttl) && ttl > 0 ? ttl : 7200) * 1000;
        return this.token;
    }
}
//# sourceMappingURL=token.js.map