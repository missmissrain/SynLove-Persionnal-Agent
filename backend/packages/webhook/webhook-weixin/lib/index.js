import { createCipheriv, createDecipheriv, createHash, randomBytes } from "node:crypto";
import { dirname, join } from "node:path";
import { appendFile } from "node:fs/promises";
import { ObservationRecorder, createUserMessage } from "@deepseek-ai/dsh-llm";
import { DatabaseSync } from "node:sqlite";
import { execFileSync, spawn } from "node:child_process";
import { appendFileSync, closeSync, copyFileSync, existsSync, mkdirSync, openSync, readFileSync, readdirSync, renameSync, rmSync, statSync, unlinkSync, writeFileSync, writeSync } from "node:fs";
import { brandString } from "@deepseek-ai/dsh-brand";
import { defineTool } from "@deepseek-ai/dsh-tools";
import { createConnection } from "node:net";
import { createServer } from "node:http";
import { homedir } from "node:os";
//#region \0rolldown/runtime.js
var __defProp = Object.defineProperty;
var __exportAll = (all, no_symbols) => {
	let target = {};
	for (var name in all) __defProp(target, name, {
		get: all[name],
		enumerable: true
	});
	if (!no_symbols) __defProp(target, Symbol.toStringTag, { value: "Module" });
	return target;
};
//#endregion
//#region lib/types/diagnostics/probe.js
/**
* 微信链路诊断探针：把入站/状态/出站三个状态机的关键跃迁写成结构化 JSONL，
* 便于事后回答"这条消息走到哪一步、卡在哪、为什么没回复"。
*
* 与 llm 探针（DSH_LLM_PROBE_LOG）分工：
* - llm 探针：模型请求与原始流事件；
* - 本探针：微信协议、状态机、outbox、lease。
*
* 仅在设置 `DSH_WEIXIN_PROBE_LOG`（文件路径）时生效；探针写入失败绝不影响主流程。
* @module dsh-webhook-weixin/probe
*/
/** 解析探针日志路径；未配置返回 undefined。 */
function probe_path() {
	const path = process.env.DSH_WEIXIN_PROBE_LOG;
	return path !== void 0 && path.length > 0 ? path : void 0;
}
/** 探针是否启用。 */
function probe_enabled() {
	return probe_path() !== void 0;
}
/**
* 追加一条探针记录。
* @param scope - 子系统名：ingress / state / inbound / outbound / lifecycle / turn-context。
* @param event - 事件名，如 delivery.received / outbox.sent。
* @param fields - 结构化字段；值会被安全序列化并截断。
*/
function probe(scope, event, fields) {
	const path = probe_path();
	if (path === void 0) return;
	const session_id = typeof fields?.session_id === "string" ? fields.session_id : void 0;
	const trace = typeof fields?.outbound_id === "string" ? ObservationRecorder.outbound_context(fields.outbound_id) : session_id === void 0 ? void 0 : typeof fields?.turn === "number" ? ObservationRecorder.turn_context(session_id, fields.turn) : ObservationRecorder.current(session_id);
	if (trace && !event.startsWith("poll.")) ObservationRecorder.emit({
		traceId: trace.traceId,
		parentSpanId: trace.spanId,
		sessionId: session_id,
		kind: "transport",
		phase: "event",
		name: `${scope}.${event}`,
		deliveryId: fields?.delivery_id,
		outboxId: fields?.outbound_id,
		outcome: fields?.outcome,
		durationMs: fields?.duration_ms
	});
	const record = {
		at: (/* @__PURE__ */ new Date()).toISOString(),
		scope,
		event,
		...fields === void 0 ? {} : { data: sanitize(fields) }
	};
	appendFile(path, JSON.stringify(record) + "\n", "utf8").catch(() => void 0);
}
/** 计时器：`const done = probe_timer('scope','event',{...}); ... done({extra})`。 */
function probe_timer(scope, event, fields) {
	const started = Date.now();
	return (extra) => probe(scope, event, {
		...fields,
		...extra,
		duration_ms: Date.now() - started
	});
}
/** 截断长字符串、限制数组长度，保证探针日志不会自我爆炸。 */
function sanitize(value, depth = 0) {
	if (typeof value === "string") return value.length > 400 ? value.slice(0, 400) + `…(+${value.length - 400})` : value;
	if (typeof value === "number" || typeof value === "boolean" || value === null || value === void 0) return value;
	if (Array.isArray(value)) {
		const head = value.slice(0, 20).map((item) => sanitize(item, depth + 1));
		return value.length > 20 ? [...head, `…(+${value.length - 20})`] : head;
	}
	if (typeof value === "object") {
		if (depth > 3) return "[deep]";
		const out = {};
		for (const [key, item] of Object.entries(value)) out[key] = sanitize(item, depth + 1);
		return out;
	}
	return String(value);
}
//#endregion
//#region lib/types/media/mediaCrypto.js
/** 微信媒体加密层：负责 AES-128-ECB 解密和 AES key 编码解析，不负责消息路由。 */
/** 解析 iLink 的 base64 原始 key 或 base64(hex) key。 */
function parse_aes_key(value) {
	const decoded = Buffer.from(value, "base64");
	if (decoded.length === 16) return decoded;
	const text = decoded.toString("ascii");
	if (decoded.length === 32 && /^[0-9a-fA-F]{32}$/.test(text)) return Buffer.from(text, "hex");
	throw new Error(`invalid iLink AES key length: ${decoded.length}`);
}
/** 使用 AES-128-ECB 解密微信 CDN 返回的媒体字节。 */
function decrypt_aes_ecb(data, key) {
	const decipher = createDecipheriv("aes-128-ecb", Buffer.from(key), null);
	decipher.setAutoPadding(true);
	return Buffer.concat([decipher.update(Buffer.from(data)), decipher.final()]);
}
//#endregion
//#region lib/types/api/ilinkClient.js
/** 微信 iLink HTTP 客户端：封装长轮询、发消息、上传地址、启动停止通知和 CDN 媒体下载。 */
/**
* iLink 客户端身份头。
*
* 官方插件（@tencent-weixin/openclaw-weixin）在每个请求上都会带：
*   iLink-App-Id: <package.json 的 ilink_appid>   → "bot"
*   iLink-App-ClientVersion: uint32 = (major<<16)|(minor<<8)|patch
* 缺少这两个头发送会被服务端判为 `prepare failed`（ret=-2），
* 而接收（getUpdates/notifystart）却是正常的，极易误判成 token 失效。
*/
const ILINK_APP_ID = process.env.WEIXIN_ILINK_APP_ID?.trim() || "bot";
const ILINK_APP_CLIENT_VERSION = process.env.WEIXIN_ILINK_APP_VERSION?.trim() || "132105";
/** 失效标记文件路径。 */
function token_marker_path() {
	return process.env.DSH_WEIXIN_TOKEN_MARKER ?? join(process.env.DSH_HOME ?? join(process.env.USERPROFILE ?? ".", ".dsh"), "weixin-token-invalid.flag");
}
/**
* 记录"凭据失效"信号，供启动器/运维发现。
*
* 发送返回 ret=-2（prepare failed）实测等价于 bot token 失效：此时接收和 notifystart 都还正常，
* 只有发送被拒，非常难排查。写一个标记文件让上游能主动提示扫码。
*/
function mark_token_invalid(reason) {
	const path = token_marker_path();
	import("node:fs/promises").then(async (fs) => {
		const stat = await fs.stat(path).catch(() => void 0);
		if (stat !== void 0 && Date.now() - stat.mtimeMs < 6e4) return;
		await fs.appendFile(path, `${(/* @__PURE__ */ new Date()).toISOString()} ${reason}\n`, "utf8");
	}).catch(() => void 0);
}
/**
* 发送成功后清除失效标记。
*
* 标记只在"确实发不出去"时有意义；一旦恢复发送就必须清掉，
* 否则启动器会一直按"凭据失效"处理（反复弹登录器），造成假故障。
*/
function clear_token_invalid() {
	import("node:fs/promises").then((fs) => fs.unlink(token_marker_path())).catch(() => void 0);
}
/** 可注入的 iLink 请求客户端，便于本地模拟和协议测试。 */
var IlinkClient = class {
	options;
	fetch_impl;
	wechat_uin;
	/** 使用协议配置创建客户端；token 只保存在进程内。 */
	constructor(options) {
		if (!options.base_url || !options.token) throw new Error("iLink base_url and token are required");
		this.options = {
			...options,
			base_url: options.base_url.replace(/\/$/, ""),
			token: options.token,
			timeout_ms: options.timeout_ms ?? 15e3,
			long_poll_timeout_ms: options.long_poll_timeout_ms ?? 35e3
		};
		this.fetch_impl = options.fetch_impl ?? fetch;
		this.wechat_uin = options.random_uin ?? Buffer.from(randomBytes(4)).toString("base64");
	}
	/** 执行一次 getUpdates 长轮询；外部 signal 可立即取消请求。 */
	async get_updates(cursor, signal) {
		return this.post_json("ilink/bot/getupdates", {
			get_updates_buf: cursor,
			base_info: this.base_info()
		}, this.options.long_poll_timeout_ms, signal);
	}
	/** 向一个微信用户发送单条文本消息。 */
	async send_message(input, signal) {
		const response = await this.post_json("ilink/bot/sendmessage", {
			msg: {
				from_user_id: "",
				to_user_id: input.to_user_id,
				client_id: input.client_id ?? `dsh-${Date.now()}`,
				...input.context_token === void 0 ? {} : { context_token: input.context_token },
				message_type: 2,
				message_state: 2,
				item_list: input.item_list ?? (input.text ? [{
					type: 1,
					text_item: { text: input.text }
				}] : []),
				...input.run_id === void 0 ? {} : { run_id: input.run_id }
			},
			base_info: this.base_info()
		}, this.options.timeout_ms, signal);
		if (response.ret !== void 0 && response.ret !== 0) {
			const rate_limited = response.ret === -2;
			const effective_items = (input.item_list ?? (input.text ? [1] : [])).length;
			if (response.errcode === -14) mark_token_invalid(response.errmsg ?? "session timeout");
			const shown = input.context_token === void 0 ? "none" : `${input.context_token.slice(0, 12)}…${input.context_token.slice(-8)}`;
			if (rate_limited) probe("outbound", "send.rejected", {
				ret: response.ret,
				errmsg: response.errmsg,
				app_id: ILINK_APP_ID,
				app_client_version: ILINK_APP_CLIENT_VERSION,
				has_context_token: input.context_token !== void 0,
				context_token_hash: input.context_token === void 0 ? void 0 : createHash("sha256").update(input.context_token).digest("hex").slice(0, 12),
				token_hash: createHash("sha256").update(this.options.token).digest("hex").slice(0, 12),
				items: effective_items,
				text_len: (input.text ?? "").length
			});
			const error = /* @__PURE__ */ new Error(`iLink sendMessage failed: ${response.errmsg ?? response.ret} (ret=${response.ret}, errcode=${response.errcode ?? "-"}, items=${effective_items}, len=${(input.text ?? "").length}, token=${shown})`);
			error.retryable = rate_limited || response.ret >= 500 || response.ret === 429;
			error.rate_limited = rate_limited;
			throw error;
		}
		clear_token_invalid();
	}
	/** 以稳定 client_id 有限重试发送，避免瞬时网络错误导致回复丢失。 */
	async send_message_with_retry(input, signal) {
		const attempts = Math.max(1, (this.options.send_retry_count ?? 2) + 1);
		const client_id = input.client_id ?? `dsh-${Date.now()}-${randomBytes(6).toString("hex")}`;
		for (let attempt = 0; attempt < attempts; attempt += 1) try {
			await this.send_message({
				...input,
				client_id
			}, signal);
			return;
		} catch (error) {
			if (!(!(error?.rate_limited === true) && !(error && typeof error === "object" && "retryable" in error && error.retryable === false)) || signal?.aborted || error instanceof Error && error.name === "AbortError" || attempt === attempts - 1) throw error;
			const delay = Math.max(0, this.options.send_retry_delay_ms ?? 250) * 2 ** attempt;
			await new Promise((resolve, reject) => {
				const timer = setTimeout(resolve, delay);
				const on_abort = () => {
					clearTimeout(timer);
					reject(signal?.reason ?? /* @__PURE__ */ new Error("aborted"));
				};
				signal?.addEventListener("abort", on_abort, { once: true });
			});
		}
	}
	/** 通知微信连接器开始工作。 */
	async notify_start(signal) {
		await this.post_json("ilink/bot/msg/notifystart", { base_info: this.base_info() }, this.options.timeout_ms, signal);
	}
	/** 通知微信连接器停止工作。 */
	async notify_stop(signal) {
		await this.post_json("ilink/bot/msg/notifystop", { base_info: this.base_info() }, this.options.timeout_ms, signal);
	}
	/** 下载并解密一个媒体引用；无完整 URL 时要求调用方提供 CDN URL。 */
	async download_media(media, signal) {
		const url = media.full_url ?? (media.encrypt_query_param && this.options.cdn_base_url ? `${this.options.cdn_base_url.replace(/\/$/, "")}/download?encrypted_query_param=${encodeURIComponent(media.encrypt_query_param)}` : void 0);
		if (!url) throw new Error("iLink media full_url or cdn_base_url/encrypt_query_param is required");
		const response = await this.fetch_impl(url, signal === void 0 ? {} : { signal });
		if (!response.ok) throw new Error(`iLink media download failed: ${response.status}`);
		const bytes = Buffer.from(await response.arrayBuffer());
		return media.aes_key ? decrypt_aes_ecb(bytes, parse_aes_key(media.aes_key)) : bytes;
	}
	/** 获取微信 CDN 媒体上传地址，参数与官方 getuploadurl 协议一致。 */
	async get_upload_url(input, signal) {
		return this.post_json("ilink/bot/getuploadurl", {
			...input,
			base_info: this.base_info()
		}, this.options.timeout_ms, signal);
	}
	/** 使用官方 AES-ECB 上传链路上传媒体并返回发送所需 CDN 引用。 */
	async upload_media(input, signal) {
		const raw = Buffer.from(input.data);
		const key = randomBytes(16);
		const filekey = randomBytes(16).toString("hex");
		const encrypted = this.encrypt_aes_ecb(raw, key);
		const request = {
			filekey,
			media_type: input.media_type,
			to_user_id: input.to_user_id,
			rawsize: raw.length,
			rawfilemd5: createHash("md5").update(raw).digest("hex"),
			filesize: encrypted.length,
			no_need_thumb: true,
			aeskey: key.toString("hex")
		};
		const result = await this.get_upload_url(request, signal);
		const upload_url = result.upload_full_url ?? (result.upload_param && this.options.cdn_base_url ? `${this.options.cdn_base_url.replace(/\/$/, "")}/upload?encrypted_query_param=${encodeURIComponent(result.upload_param)}&filekey=${encodeURIComponent(filekey)}` : void 0);
		if (!upload_url) throw new Error("iLink getuploadurl returned no upload URL");
		const uploaded = await this.fetch_impl(upload_url, {
			method: "POST",
			headers: { "Content-Type": "application/octet-stream" },
			body: new Uint8Array(encrypted),
			...signal === void 0 ? {} : { signal }
		});
		if (!uploaded.ok) throw new Error(`iLink CDN upload failed: ${uploaded.status}`);
		const encrypted_query_param = uploaded.headers.get("x-encrypted-param");
		if (!encrypted_query_param) throw new Error("iLink CDN upload missing x-encrypted-param");
		return {
			filekey,
			encrypted_query_param,
			aes_key: key.toString("base64"),
			rawsize: raw.length,
			filesize: encrypted.length
		};
	}
	/** 上传并回传一张图片；文本说明会作为独立 item 一并发送。 */
	async send_image(input, signal) {
		const uploaded = await this.upload_media({
			data: input.data,
			to_user_id: input.to_user_id,
			media_type: 1
		}, signal);
		const item_list = [];
		if (input.caption) item_list.push({
			type: 1,
			text_item: { text: input.caption }
		});
		item_list.push({
			type: 2,
			image_item: {
				media: {
					encrypt_query_param: uploaded.encrypted_query_param,
					aes_key: uploaded.aes_key,
					encrypt_type: 1
				},
				mid_size: uploaded.filesize
			}
		});
		await this.send_message({
			to_user_id: input.to_user_id,
			text: "",
			...input.context_token === void 0 ? {} : { context_token: input.context_token },
			item_list
		}, signal);
	}
	/** 返回协议所需的脱敏基础信息。 */
	base_info() {
		return {
			channel_version: this.options.channel_version ?? "dsh-webhook-weixin",
			bot_agent: this.options.bot_agent ?? "DeepSeekHarness"
		};
	}
	/** 发起带超时、Bearer Token 和 base_info 的 JSON POST。 */
	async post_json(endpoint, body, timeout_ms, signal) {
		const controller = new AbortController();
		const timer = setTimeout(() => controller.abort(), timeout_ms);
		const on_abort = () => controller.abort(signal?.reason);
		signal?.addEventListener("abort", on_abort, { once: true });
		try {
			const response = await this.fetch_impl(`${this.options.base_url}/${endpoint}`, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					Authorization: `Bearer ${this.options.token}`,
					AuthorizationType: "ilink_bot_token",
					"X-WECHAT-UIN": this.wechat_uin,
					"iLink-App-Id": ILINK_APP_ID,
					"iLink-App-ClientVersion": ILINK_APP_CLIENT_VERSION
				},
				body: JSON.stringify(body),
				signal: controller.signal
			});
			const text = await response.text();
			if (!response.ok) throw new Error(`iLink ${endpoint} failed: HTTP ${response.status}`);
			return JSON.parse(text);
		} finally {
			clearTimeout(timer);
			signal?.removeEventListener("abort", on_abort);
		}
	}
	/** 使用 PKCS#7 填充执行 AES-128-ECB 加密。 */
	encrypt_aes_ecb(data, key) {
		const cipher = createCipheriv("aes-128-ecb", Buffer.from(key), null);
		return Buffer.concat([cipher.update(Buffer.from(data)), cipher.final()]);
	}
};
//#endregion
//#region lib/types/media/mediaTranscoder.js
var mediaTranscoder_exports = /* @__PURE__ */ __exportAll({ transcode_silk_to_wav: () => transcode_silk_to_wav });
/** 微信语音转码层：尝试 SILK 转 WAV，解码器不可用时保留原始 SILK。 */
/** 将 PCM 16-bit 单声道数据封装为 WAV。 */
function pcm_to_wav(pcm, sample_rate) {
	const result = Buffer.alloc(44 + pcm.byteLength);
	result.write("RIFF", 0);
	result.writeUInt32LE(result.length - 8, 4);
	result.write("WAVE", 8);
	result.write("fmt ", 12);
	result.writeUInt32LE(16, 16);
	result.writeUInt16LE(1, 20);
	result.writeUInt16LE(1, 22);
	result.writeUInt32LE(sample_rate, 24);
	result.writeUInt32LE(sample_rate * 2, 28);
	result.writeUInt16LE(2, 32);
	result.writeUInt16LE(16, 34);
	result.write("data", 36);
	result.writeUInt32LE(pcm.byteLength, 40);
	Buffer.from(pcm).copy(result, 44);
	return result;
}
/** 尝试用可选 silk-wasm 解码 SILK；失败时返回原始 SILK 和格式标记。 */
async function transcode_silk_to_wav(data, sample_rate = 24e3) {
	try {
		return {
			data: pcm_to_wav((await (await import("silk-wasm")).decode(data, sample_rate)).data, sample_rate),
			mime_type: "audio/wav",
			transcoded: true
		};
	} catch {
		return {
			data,
			mime_type: "audio/silk",
			transcoded: false
		};
	}
}
//#endregion
//#region lib/types/message/messageNormalizer.js
/** 微信消息规范化层：统一文本、图片、文件、语音元数据，并保留语音文本优先级。 */
/** 单个入站媒体的下载上限（字节）。超过就直接跳过，避免大文件把内存和链路拖死。 */
const MAX_MEDIA_BYTES = Number(process.env.DSH_WEIXIN_MAX_MEDIA_BYTES ?? String(20 * 1024 * 1024));
/** 将 iLink 消息转换为稳定的 Harness 入站结构；不执行网络和模型调用。 */
var MessageNormalizer = class {
	/** 规范化消息并生成稳定 delivery id。 */
	normalize_message(message) {
		const user_id = message.from_user_id ?? "";
		if (!user_id) throw new Error("iLink message has no from_user_id");
		const items = message.item_list ?? [];
		const text_parts = [];
		const media = [];
		for (const item of items) this.normalize_item(item, text_parts, media);
		return {
			delivery_id: String(message.message_id ?? message.seq ?? `${user_id}:${message.create_time_ms ?? Date.now()}`),
			user_id,
			...message.context_token === void 0 ? {} : { context_token: message.context_token },
			text: text_parts.join("\n").trim(),
			media,
			received_at: message.create_time_ms ?? Date.now()
		};
	}
	/** 下载并填充消息中的媒体字节；语音已有文本时不触发下载或 ASR。 */
	async hydrate_media(message, loader) {
		for (const media of message.media) {
			if (media.data || !media.media || media.kind === "voice" && media.source_text !== "voice_media_requires_asr") continue;
			const declared = media.media.rawsize ?? media.media.filesize;
			if (typeof declared === "number" && declared > MAX_MEDIA_BYTES) {
				media.source_text = `媒体过大已跳过（${Math.round(declared / 1024 / 1024)}MB > 上限 ${Math.round(MAX_MEDIA_BYTES / 1024 / 1024)}MB）`;
				continue;
			}
			media.data = await loader(media.media);
			if (media.data.length > MAX_MEDIA_BYTES) {
				delete media.data;
				media.source_text = `媒体过大已跳过（>${Math.round(MAX_MEDIA_BYTES / 1024 / 1024)}MB）`;
				continue;
			}
			if (media.kind === "voice") {
				media.raw_data = media.data;
				const { transcode_silk_to_wav } = await Promise.resolve().then(() => mediaTranscoder_exports);
				const converted = await transcode_silk_to_wav(media.data);
				media.data = converted.data;
				media.mime_type = converted.mime_type;
				media.transcoded = converted.transcoded;
			}
			if (media.kind === "image" && media.mime_type === void 0) media.mime_type = "image/jpeg";
		}
		return message;
	}
	/** 解析一个消息项，保证 voice_item.text 优先于语音媒体。 */
	normalize_item(item, text_parts, media) {
		if (item.type === 1 && item.text_item?.text !== void 0) text_parts.push(item.text_item.text);
		if (item.type === 2 && item.image_item) media.push({
			kind: "image",
			mime_type: "image/jpeg",
			name: "weixin-image.jpg",
			media: {
				...item.image_item.media,
				...item.image_item.aeskey === void 0 ? {} : { aes_key: Buffer.from(item.image_item.aeskey, "hex").toString("base64") }
			}
		});
		if (item.type === 3 && item.voice_item) if (item.voice_item.text) text_parts.push(item.voice_item.text);
		else media.push({
			kind: "voice",
			mime_type: "audio/silk",
			source_text: "voice_media_requires_asr",
			...item.voice_item.media === void 0 ? {} : { media: item.voice_item.media }
		});
		if (item.type === 4 && item.file_item) media.push({
			kind: "file",
			...item.file_item.file_name === void 0 ? {} : { name: item.file_item.file_name },
			mime_type: "application/octet-stream",
			...item.file_item.media === void 0 ? {} : { media: item.file_item.media }
		});
	}
};
//#endregion
//#region lib/types/types/domain.js
/**
* 微信链路的统一领域模型：入站信封、控制命令、delivery 状态、outbox 记录。
*
* 设计原则（对应重构设计文档 §3.2）：一个事实只能有一个权威写入者。
* - 执行状态（inbox/turn/step/cancel/steer/transcript）由 Harness Agent/Session 唯一持有；
* - 本文件只描述微信协议侧必需的幂等键、路由、游标与发送状态。
* @module dsh-webhook-weixin/domain
*/
/** 该状态是否允许提交其所在批次的 cursor（设计文档 §5.2）。 */
function is_cursor_committable(status) {
	return status === "injected" || status === "control_applied" || status === "failed_terminal";
}
//#endregion
//#region lib/types/state/weixinStateStore.js
/**
* 微信链路唯一状态存储（SQLite）。
*
* 重构设计文档 §5.1：不再新增临时 JSON 作为状态源，所有外部协议状态集中在一个库里：
*   consumer_lease / transport_cursor / route / delivery / outbox / turn_context / meta
*
* 关键语义（§5.3）：**任何写失败都必须 reject 调用方**，禁止"记日志后 resolve"。
* 事务由 SQLite 原生事务保证；跨进程互斥由 lease 表 + 主键冲突保证原子性。
*
* 使用 Node 内置 `node:sqlite`（Node 22+），无需第三方依赖。
* @module dsh-webhook-weixin/state-store
*/
/**
* 状态库 schema 版本；不匹配时 fail closed（禁止自动清空）。
* v2：delivery 增加 context_token 列（回复需要它，之前没存导致只能无 token 发送）。
* v3：delivery 增加 payload_json 列（崩溃恢复时据此**重新注入**，而不是误标 injected）；
*     outbox 增加 sent_parts_json 列（分段发送断点续传，避免重试重复发已成功段）。
*/
const STATE_SCHEMA_VERSION = 3;
/**
* meta 表里"这一轮还在进行中"的标记前缀：`inflight_turn:<session_id>`。
*
* 轮次开始写、正常结束**或用户取消**时删。进程被强杀时会留下残留，启动时据此
* 把该轮 delivery 退回 received 重投（见 weixinConnector.rescue_interrupted_turns）。
* 用户主动取消必须清掉它 —— 否则下次启动会把"用户已经叫停的任务"又跑一遍。
*/
const INFLIGHT_TURN_PREFIX = "inflight_turn:";
/** 微信链路状态库。所有公开写方法失败时抛出，调用方必须处理。 */
var WeixinStateStore = class {
	db;
	storeDir;
	closed = false;
	/** 打开（或创建）状态库并校验 schema。 */
	constructor(options) {
		const create = options.create ?? true;
		this.storeDir = dirname(options.path);
		if (!create && !existsSync(options.path)) throw new Error(`weixin state store 不存在且不允许创建：${options.path}`);
		mkdirSync(dirname(options.path), { recursive: true });
		const done = probe_timer("state", "store.open", { path: options.path });
		this.db = new DatabaseSync(options.path);
		try {
			this.db.exec("PRAGMA journal_mode = WAL");
			this.db.exec("PRAGMA synchronous = FULL");
			this.db.exec("PRAGMA foreign_keys = ON");
			this.migrate();
		} catch (error) {
			try {
				this.db.close();
			} catch {}
			this.closed = true;
			throw error;
		}
		done();
	}
	/** 建表 + schema 校验；不匹配则 fail closed。 */
	migrate() {
		this.db.exec(`
      CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS transport_cursor (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        cursor TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS route (
        user_id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS delivery (
        delivery_id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        session_id TEXT,
        status TEXT NOT NULL,
        received_at INTEGER NOT NULL,
        injected_at INTEGER,
        last_error TEXT,
        attempt INTEGER NOT NULL DEFAULT 0,
        context_token TEXT,
        payload_json TEXT
      );
      CREATE INDEX IF NOT EXISTS delivery_status_idx ON delivery (status, received_at);
      CREATE TABLE IF NOT EXISTS outbox (
        outbound_id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        turn INTEGER,
        sequence INTEGER NOT NULL,
        kind TEXT NOT NULL,
        text TEXT NOT NULL,
        context_token TEXT,
        client_id TEXT NOT NULL,
        status TEXT NOT NULL,
        attempt INTEGER NOT NULL DEFAULT 0,
        next_retry_at INTEGER,
        last_error TEXT,
        created_at INTEGER NOT NULL,
        sent_at INTEGER,
        sent_parts_json TEXT NOT NULL DEFAULT '[]'
      );
      CREATE INDEX IF NOT EXISTS outbox_queue_idx ON outbox (status, session_id, sequence);
      CREATE TABLE IF NOT EXISTS turn_context (
        session_id TEXT NOT NULL,
        turn INTEGER NOT NULL,
        delivery_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        context_token TEXT,
        created_at INTEGER NOT NULL,
        PRIMARY KEY (session_id, turn)
      );
      CREATE TABLE IF NOT EXISTS consumer_lease (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        owner_id TEXT NOT NULL,
        pid INTEGER NOT NULL,
        process_started_at INTEGER NOT NULL,
        account_id TEXT NOT NULL,
        acquired_at INTEGER NOT NULL,
        heartbeat_at INTEGER NOT NULL
      );
    `);
		const row = this.db.prepare("SELECT value FROM meta WHERE key = ?").get("schema_version");
		if (row === void 0) {
			this.db.prepare("INSERT INTO meta (key, value) VALUES (?, ?)").run("schema_version", String(3));
			return;
		}
		const found = Number(row.value);
		if (found === 3) return;
		if (found >= 1 && found < 3) {
			const delivery_columns = this.column_names("delivery");
			if (!delivery_columns.includes("context_token")) this.db.exec("ALTER TABLE delivery ADD COLUMN context_token TEXT");
			if (!delivery_columns.includes("payload_json")) this.db.exec("ALTER TABLE delivery ADD COLUMN payload_json TEXT");
			if (!this.column_names("outbox").includes("sent_parts_json")) this.db.exec("ALTER TABLE outbox ADD COLUMN sent_parts_json TEXT NOT NULL DEFAULT '[]'");
			this.db.prepare("UPDATE meta SET value = ? WHERE key = ?").run(String(3), "schema_version");
			probe("state", "schema.migrated", {
				from: found,
				to: 3
			});
			return;
		}
		throw new Error(`weixin state store schema 版本不匹配：期望 3，实际 ${found}。拒绝自动迁移或清空，请人工处理后再启动。`);
	}
	/** 读取某张表的列名集合；用于幂等的前向迁移。 */
	column_names(table) {
		return this.db.prepare(`PRAGMA table_info(${table})`).all().map((column) => column.name);
	}
	/** 关闭数据库；幂等。 */
	close() {
		if (this.closed) return;
		this.closed = true;
		this.db.close();
	}
	/**
	* 在一个事务里执行；抛错自动回滚并向上传播。
	* 这是"一个事实一个权威写入者"的实现手段：相关状态要么一起成功，要么一起不生效。
	*/
	transaction(fn) {
		this.db.exec("BEGIN IMMEDIATE");
		try {
			const result = fn();
			this.db.exec("COMMIT");
			return result;
		} catch (error) {
			try {
				this.db.exec("ROLLBACK");
			} catch {}
			probe("state", "transaction.rollback", { error: String(error) });
			throw error;
		}
	}
	/** 读取当前 getUpdates 游标。 */
	get_cursor() {
		return this.db.prepare("SELECT cursor FROM transport_cursor WHERE id = 1").get()?.cursor ?? "";
	}
	/** 提交游标；失败抛错（调用方不得把未落盘当作已提交）。 */
	set_cursor(cursor) {
		this.db.prepare("INSERT INTO transport_cursor (id, cursor, updated_at) VALUES (1, ?, ?) ON CONFLICT(id) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at").run(cursor, Date.now());
		probe("state", "cursor.committed", { cursor_len: cursor.length });
	}
	/** 读一个 meta 键。 */
	get_meta(key) {
		return this.db.prepare("SELECT value FROM meta WHERE key = ?").get(key)?.value;
	}
	/** 写一个 meta 键。 */
	set_meta(key, value) {
		this.db.prepare("INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value").run(key, value);
	}
	/** 按前缀列出 meta 键值（启动时找"进行中的轮次"标记用）。 */
	list_meta_prefix(prefix) {
		return this.db.prepare("SELECT key, value FROM meta WHERE key LIKE ? ORDER BY key").all(`${prefix}%`);
	}
	/** 删除一个 meta 键（不存在也算成功）。 */
	delete_meta(key) {
		this.db.prepare("DELETE FROM meta WHERE key = ?").run(key);
	}
	/**
	* 共享会话的基名（用户要求"微信和 QQ 单聊聊同一段"时启用）。
	*
	* 开关：环境变量 `DSH_SESSION_SHARED` 非空；基名存在 `$DSH_HOME/shared-session.txt`
	* （清空上下文后代次会变，文件也会被改写，QQ 侧每次读它以保证两边同一个会话）。
	* 未启用时返回空串，行为与以前完全一致。
	*/
	shared_base() {
		if ((process.env.DSH_SESSION_SHARED ?? "").trim() === "") return "";
		try {
			return (readFileSync(join(this.storeDir, "shared-session.txt"), "utf8").split("\n")[0] ?? "").trim().replace(/-c\d+$/, "") || "main";
		} catch {
			return "main";
		}
	}
	/** 把共享会话当前 id 写回文件（只在变化时写），让 QQ 通道跟着走。 */
	persist_shared(id) {
		try {
			const file = join(this.storeDir, "shared-session.txt");
			if ((existsSync(file) ? (readFileSync(file, "utf8").split("\n")[0] ?? "").trim() : "") !== id) writeFileSync(file, `${id}\n`, "utf8");
		} catch {}
	}
	/** 读取 user → session 路由。 */
	get_session_id(user_id) {
		if (this.shared_base() !== "") return this.session_id_for(user_id);
		return this.db.prepare("SELECT session_id FROM route WHERE user_id = ?").get(user_id)?.session_id;
	}
	/**
	* 清空出站队列：删掉所有**未终态**的行（待发 / 发送中 / 停放待问）。
	*
	* 用"删"而不是"标记失败"：队列的用途只是"还没发出去的东西"，
	* 清空之后 `bridge_results` 里也不会再翻出这些旧内容——这才是用户要的"清空队列"。
	* 已发送（sent）与明确失败（failed_terminal）的历史不动，它们只是记录。
	*
	* @returns 删掉的条数。
	*/
	clear_queue() {
		const result = this.db.prepare("DELETE FROM outbox WHERE status IN ('pending', 'sending', 'failed_retryable')").run();
		const removed = Number(result.changes ?? 0);
		probe("state", "outbox.cleared", { removed });
		return removed;
	}
	/**
	* 某个用户当前应使用的会话 id（把上下文代次编进去）。
	*
	* 代次 0（从没清空过）就用 `weixin-<user>`，保持历史数据不变；
	* 清空过就用 `weixin-<user>-c<epoch>`——换名字是为了**让新会话不与旧文件重名**，
	* 这样即便旧文件没删掉，也一定会新建一个空上下文的会话。
	*/
	session_id_for(user_id) {
		const base = this.shared_base();
		if (base !== "") {
			const epoch = this.context_epoch(user_id);
			const id = epoch <= 0 ? base : `${base}-c${epoch}`;
			this.persist_shared(id);
			return id;
		}
		const epoch = this.context_epoch(user_id);
		return epoch <= 0 ? `weixin-${user_id}` : `weixin-${user_id}-c${epoch}`;
	}
	/**
	* 删除 user → session 路由（清空上下文用）。
	*
	* 删掉后下一条消息会走"新建会话"分支，于是拿到一个**全新**的会话（空上下文）。
	* 只删路由是不够的——旧会话文件还在原地，所以必须配合"换一个会话 id"（见 epoch），
	* 否则新建时会撞上已存在的同名会话、又恢复回旧上下文。
	*/
	clear_session_id(user_id) {
		this.db.prepare("DELETE FROM route WHERE user_id = ?").run(user_id);
		probe("state", "route.cleared", { user_id });
	}
	/**
	* 上下文代次（epoch）：同一用户名下第几段会话。
	*
	* 每次"清空上下文"就 +1，于是新会话 id 变成 `weixin-<user>` 之外的另一个名字，
	* 不会与旧会话文件重名。**这样即使旧文件删不掉（路径推导失败/被占用），
	* 上下文也一定是干净的**——文件删除只是顺手回收磁盘。
	*/
	context_epoch(user_id) {
		const raw = this.get_meta(`context_epoch:${user_id}`);
		const value = Number(raw);
		return Number.isFinite(value) && value > 0 ? Math.floor(value) : 0;
	}
	/** 上下文代次 +1，返回新值。 */
	bump_context_epoch(user_id) {
		const next = this.context_epoch(user_id) + 1;
		this.set_meta(`context_epoch:${user_id}`, String(next));
		probe("state", "context.epoch_bumped", {
			user_id,
			epoch: next
		});
		return next;
	}
	/** 写入 user → session 路由。 */
	set_session_id(user_id, session_id) {
		this.db.prepare("INSERT INTO route (user_id, session_id, updated_at) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET session_id = excluded.session_id, updated_at = excluded.updated_at").run(user_id, session_id, Date.now());
		probe("state", "route.bound", {
			user_id,
			session_id
		});
	}
	/** 列出所有有路由的微信用户（主动推送时要知道推给谁）。 */
	list_users() {
		return this.db.prepare("SELECT user_id FROM route ORDER BY updated_at DESC").all().map((row) => row.user_id);
	}
	/** 统计路由条数与会话数（装配检查/诊断用）。 */
	route_summary() {
		const row = this.db.prepare("SELECT COUNT(*) AS users, COUNT(DISTINCT session_id) AS sessions FROM route").get();
		return {
			users: row?.users ?? 0,
			sessions: row?.sessions ?? 0
		};
	}
	/**
	* 幂等登记一条 delivery。
	* @returns `true` 表示首次登记（应继续处理）；`false` 表示此前已登记（重复投递）。
	* 注意：这里只登记接收事实，**不代表已注入会话**；cursor 提交仍取决于后续状态。
	*/
	put_delivery_if_absent(record) {
		if (this.db.prepare("SELECT delivery_id FROM delivery WHERE delivery_id = ?").get(record.delivery_id) !== void 0) {
			probe("state", "delivery.duplicate", {
				delivery_id: record.delivery_id,
				user_id: record.user_id
			});
			return false;
		}
		this.db.prepare("INSERT INTO delivery (delivery_id, user_id, status, received_at, attempt, context_token, payload_json) VALUES (?, ?, ?, ?, 0, ?, ?)").run(record.delivery_id, record.user_id, "received", record.received_at, record.context_token ?? null, record.payload_json ?? null);
		probe("state", "delivery.received", {
			delivery_id: record.delivery_id,
			user_id: record.user_id,
			has_context_token: record.context_token !== void 0
		});
		return true;
	}
	/** 最近一条入站消息的用户；调试注入口在未指定 user_id 时用它作为默认收件人。 */
	latest_user_id() {
		return this.db.prepare("SELECT user_id FROM delivery ORDER BY received_at DESC LIMIT 1").get()?.user_id;
	}
	/** 读取某条 delivery 的 context_token（回复时必须用它）。 */
	delivery_context_token(delivery_id) {
		return this.db.prepare("SELECT context_token FROM delivery WHERE delivery_id = ?").get(delivery_id)?.context_token ?? void 0;
	}
	/** 读取一条 delivery。 */
	get_delivery(delivery_id) {
		const row = this.db.prepare("SELECT * FROM delivery WHERE delivery_id = ?").get(delivery_id);
		return row === void 0 ? void 0 : to_delivery(row);
	}
	/**
	* 更新 delivery 状态（CAS：可指定期望的当前状态）。
	*
	* attempt 只统计**实际处理失败**的次数，不由状态迁移本身消耗：
	* `received → routing → injected` 是正常流转，不应该吃掉重试预算；
	* 只有重试与终态失败才传 `increment_attempt: true`。
	*/
	update_delivery(delivery_id, status, fields) {
		const current = this.get_delivery(delivery_id);
		if (current === void 0) throw new Error(`delivery 不存在：${delivery_id}`);
		if (fields?.expect !== void 0 && current.status !== fields.expect) {
			probe("state", "delivery.cas_miss", {
				delivery_id,
				expected: fields.expect,
				actual: current.status
			});
			return false;
		}
		if (this.db.prepare("UPDATE delivery SET status = ?, session_id = COALESCE(?, session_id), injected_at = COALESCE(?, injected_at), last_error = ?, attempt = attempt + ? WHERE delivery_id = ? AND status = ?").run(status, fields?.session_id ?? null, fields?.injected_at ?? null, fields?.last_error ?? null, fields?.increment_attempt === true ? 1 : 0, delivery_id, fields?.expect ?? current.status).changes === 0) return false;
		probe("state", "delivery.status", {
			delivery_id,
			from: current.status,
			to: status,
			attempt: current.attempt + (fields?.increment_attempt === true ? 1 : 0)
		});
		return true;
	}
	/** 列出未完成（需要恢复）的 delivery。 */
	list_unfinished_deliveries(limit = 200) {
		return this.db.prepare("SELECT * FROM delivery WHERE status IN ('received','routing','failed_retryable') ORDER BY received_at LIMIT ?").all(limit).map(to_delivery);
	}
	/** 批次内 delivery 是否都达到可提交 cursor 的状态。 */
	batch_committable(delivery_ids) {
		if (delivery_ids.length === 0) return true;
		const placeholders = delivery_ids.map(() => "?").join(",");
		return (this.db.prepare(`SELECT COUNT(*) AS pending FROM delivery WHERE delivery_id IN (${placeholders}) AND status NOT IN ('injected','control_applied','failed_terminal')`).get(...delivery_ids)?.pending ?? 0) === 0;
	}
	/**
	* 入队一条出站消息；sequence 在同一 Session 内单调递增（由本方法分配）。
	* @returns 落库后的完整记录（含分配的 sequence）。
	*/
	enqueue_outbox(input) {
		return this.transaction(() => {
			const sequence = (this.db.prepare("SELECT COALESCE(MAX(sequence), 0) AS last FROM outbox WHERE session_id = ?").get(input.session_id)?.last ?? 0) + 1;
			const outbound_id = `${input.session_id}:${sequence}`;
			const client_id = input.client_id ?? `dsh-${input.session_id}-${sequence}-${Date.now().toString(36)}`;
			const created_at = Date.now();
			this.db.prepare("INSERT INTO outbox (outbound_id, session_id, user_id, turn, sequence, kind, text, context_token, client_id, status, attempt, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)").run(outbound_id, input.session_id, input.user_id, input.turn ?? null, sequence, input.kind, input.text, input.context_token ?? null, client_id, "pending", created_at);
			probe("state", "outbox.enqueued", {
				outbound_id,
				session_id: input.session_id,
				kind: input.kind,
				sequence,
				text_len: input.text.length
			});
			return {
				outbound_id,
				session_id: input.session_id,
				user_id: input.user_id,
				...input.turn === void 0 ? {} : { turn: input.turn },
				sequence,
				kind: input.kind,
				text: input.text,
				...input.context_token === void 0 ? {} : { context_token: input.context_token },
				client_id,
				status: "pending",
				attempt: 0,
				created_at
			};
		});
	}
	/**
	* 取出某 Session 中下一条可发送记录（pending 或到时间的 failed_retryable）。
	*
	* **优先级**：`turn` 非空的是"针对某一轮的正式回复"，`turn` 为空的是通知类
	* （codex/opencode 完成汇报、进度、心跳）。平台对短时发送条数有限额，通知量一大
	* 就会把正式回复挤到窗口外，所以回复一律排在通知前面。
	*/
	next_outbox(session_id, now = Date.now()) {
		const row = this.db.prepare("SELECT * FROM outbox WHERE session_id = ? AND (status = 'pending' OR (status = 'failed_retryable' AND (next_retry_at IS NULL OR next_retry_at <= ?))) ORDER BY (turn IS NULL), sequence LIMIT 1").get(session_id, now);
		return row === void 0 ? void 0 : to_outbox(row);
	}
	/** 列出所有会话中待处理的下一条（用于跨会话并发发送）。 */
	next_outbox_per_session(now = Date.now()) {
		const rows = this.db.prepare("SELECT * FROM outbox WHERE status = 'pending' OR (status = 'failed_retryable' AND (next_retry_at IS NULL OR next_retry_at <= ?)) ORDER BY session_id, (turn IS NULL), sequence").all(now);
		const seen = /* @__PURE__ */ new Set();
		const result = [];
		for (const row of rows) {
			if (seen.has(row.session_id)) continue;
			seen.add(row.session_id);
			result.push(to_outbox(row));
		}
		return result;
	}
	/** 标记发送中；失败抛错。 */
	mark_outbox_sending(outbound_id) {
		this.db.prepare("UPDATE outbox SET status = 'sending', attempt = attempt + 1 WHERE outbound_id = ?").run(outbound_id);
		probe("state", "outbox.sending", { outbound_id });
	}
	/**
	* 列出"已停放、尚未送达"的处理结果（通知类），供工具按需取回。
	*
	* 场景：codex/opencode 任务做完了，但用户此时不在微信里（会话窗口关闭），
	* 平台不允许推送。这些结果**停放在队列里保留一段时间**，等用户主动问起时由工具返回，
	* 而不是反复重试（重试必然失败，还会拖慢真实回复）。
	*/
	parked_results(limit = 20) {
		return this.db.prepare("SELECT outbound_id, sequence, text, created_at FROM outbox WHERE turn IS NULL AND status IN ('pending','failed_retryable','failed_terminal') ORDER BY sequence LIMIT ?").all(limit);
	}
	/** 丢弃超过保留期的停放结果（默认 6 小时），避免长期堆积。 */
	prune_parked_results(max_age_ms, now = Date.now()) {
		const removed = this.db.prepare("DELETE FROM outbox WHERE turn IS NULL AND status IN ('pending','failed_retryable','failed_terminal') AND created_at < ?").run(now - max_age_ms).changes;
		if (removed > 0) probe("state", "parked.pruned", {
			removed,
			max_age_ms
		});
		return Number(removed);
	}
	/**
	* 每个会话最多保留多少条**待发通知**（turn 为空）。
	*
	* 通知类消息（任务完成汇报、进度）时效性很强，积压后往往已经没意义，
	* 而且会占用平台的发送限额、把正式回复挤掉——所以超出上限时**丢最旧的**。
	*/
	trim_pending_notices(session_id, keep = 3) {
		const removed = this.db.prepare("DELETE FROM outbox WHERE outbound_id IN (  SELECT outbound_id FROM outbox WHERE session_id = ? AND status = 'pending' AND turn IS NULL   ORDER BY sequence DESC LIMIT -1 OFFSET ?)").run(session_id, keep).changes;
		if (removed > 0) probe("state", "outbox.notices_trimmed", {
			session_id,
			removed,
			keep
		});
		return Number(removed);
	}
	/**
	* 记录某一分段已成功送达（立即落盘）。
	*
	* 必须在每段发送成功后马上写，而不是整条发完再写：否则进程在中间崩溃，
	* 重启后会从头重发，用户收到重复内容。
	*/
	mark_outbox_part_sent(outbound_id, index) {
		const row = this.db.prepare("SELECT sent_parts_json FROM outbox WHERE outbound_id = ?").get(outbound_id);
		if (row === void 0) return;
		const parts = parse_sent_parts(row.sent_parts_json);
		if (parts.includes(index)) return;
		parts.push(index);
		parts.sort((a, b) => a - b);
		this.db.prepare("UPDATE outbox SET sent_parts_json = ? WHERE outbound_id = ?").run(JSON.stringify(parts), outbound_id);
		probe("state", "outbox.part_sent", {
			outbound_id,
			index,
			parts: parts.length
		});
	}
	/** 标记已发送。 */
	mark_outbox_sent(outbound_id) {
		this.db.prepare("UPDATE outbox SET status = 'sent', sent_at = ?, last_error = NULL WHERE outbound_id = ?").run(Date.now(), outbound_id);
		probe("state", "outbox.sent", { outbound_id });
	}
	/** 标记失败（可重试/终态），并给出下次重试时间。 */
	mark_outbox_failed(outbound_id, status, error, next_retry_at) {
		this.db.prepare("UPDATE outbox SET status = ?, last_error = ?, next_retry_at = ? WHERE outbound_id = ?").run(status, error, next_retry_at ?? null, outbound_id);
		probe("state", "outbox.failed", {
			outbound_id,
			status,
			error
		});
	}
	/** 读取一条 outbox 记录。 */
	get_outbox(outbound_id) {
		const row = this.db.prepare("SELECT * FROM outbox WHERE outbound_id = ?").get(outbound_id);
		return row === void 0 ? void 0 : to_outbox(row);
	}
	/** 列出未发送完成的记录（恢复用）。 */
	list_pending_outbox(limit = 500) {
		return this.db.prepare("SELECT * FROM outbox WHERE status IN ('pending','sending','failed_retryable') ORDER BY session_id, sequence LIMIT ?").all(limit).map(to_outbox);
	}
	/** 绑定 `session_id + turn` → delivery/context_token（设计文档 §7.1）。 */
	bind_turn_context(input) {
		this.db.prepare("INSERT INTO turn_context (session_id, turn, delivery_id, user_id, context_token, created_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(session_id, turn) DO UPDATE SET delivery_id = excluded.delivery_id, user_id = excluded.user_id, context_token = excluded.context_token").run(input.session_id, input.turn, input.delivery_id, input.user_id, input.context_token ?? null, Date.now());
		probe("state", "turn_context.bound", {
			session_id: input.session_id,
			turn: input.turn,
			delivery_id: input.delivery_id,
			has_token: input.context_token !== void 0
		});
	}
	/** 读取某 turn 的上下文绑定。 */
	get_turn_context(session_id, turn) {
		const row = this.db.prepare("SELECT * FROM turn_context WHERE session_id = ? AND turn = ?").get(session_id, turn);
		if (row === void 0) return void 0;
		return {
			delivery_id: row.delivery_id,
			user_id: row.user_id,
			...row.context_token === null ? {} : { context_token: row.context_token }
		};
	}
	/** 最近一条 delivery → session 绑定（重启后补 turn context 用）。 */
	latest_delivery_for_session(session_id) {
		const row = this.db.prepare("SELECT * FROM delivery WHERE session_id = ? AND status IN ('injected','control_applied') ORDER BY received_at DESC LIMIT 1").get(session_id);
		return row === void 0 ? void 0 : to_delivery(row);
	}
	/** 清理过期的 turn_context（保留最近 N 个 turn）。 */
	prune_turn_context(session_id, keep = 20) {
		this.db.prepare("DELETE FROM turn_context WHERE session_id = ? AND turn NOT IN (SELECT turn FROM turn_context WHERE session_id = ? ORDER BY turn DESC LIMIT ?)").run(session_id, session_id, keep);
	}
	/** 读取当前 lease（若有）。`process_started_at` 用于识别 pid 被回收的情况。 */
	get_lease() {
		return this.db.prepare("SELECT owner_id, pid, heartbeat_at, account_id, process_started_at FROM consumer_lease WHERE id = 1").get();
	}
	/**
	* 原子获取 lease：主键唯一 + UPDATE ... WHERE 条件保证只有一个进程成功。
	* @returns true 表示获取成功。
	*/
	try_acquire_lease(input) {
		const now = Date.now();
		return this.transaction(() => {
			const existing = this.get_lease();
			if (existing !== void 0) {
				const alive = lease_holder_alive(existing.pid, existing.process_started_at);
				if (alive) {
					probe("state", "lease.busy", {
						held_by: existing.owner_id,
						pid: existing.pid,
						age_ms: now - existing.heartbeat_at
					});
					return false;
				}
				probe("state", "lease.takeover", {
					held_by: existing.owner_id,
					pid: existing.pid,
					pid_alive: alive,
					age_ms: now - existing.heartbeat_at,
					reason: alive ? "stale" : "holder_dead"
				});
			}
			this.db.prepare("INSERT INTO consumer_lease (id, owner_id, pid, process_started_at, account_id, acquired_at, heartbeat_at) VALUES (1, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET owner_id = excluded.owner_id, pid = excluded.pid, process_started_at = excluded.process_started_at, account_id = excluded.account_id, acquired_at = excluded.acquired_at, heartbeat_at = excluded.heartbeat_at").run(input.owner_id, input.pid, input.process_started_at, input.account_id, now, now);
			probe("state", "lease.acquired", {
				owner_id: input.owner_id,
				pid: input.pid,
				account_id: input.account_id
			});
			return true;
		});
	}
	/** 续租；只有持有者能续。 */
	heartbeat_lease(owner_id) {
		const row = this.db.prepare("SELECT owner_id FROM consumer_lease WHERE id = 1").get();
		if (row === void 0 || row.owner_id !== owner_id) return false;
		this.db.prepare("UPDATE consumer_lease SET heartbeat_at = ? WHERE id = 1").run(Date.now());
		return true;
	}
	/** 释放 lease；只有持有者能释放。 */
	release_lease(owner_id) {
		this.db.prepare("DELETE FROM consumer_lease WHERE id = 1 AND owner_id = ?").run(owner_id);
		probe("state", "lease.released", { owner_id });
	}
	/** 诊断快照：各状态计数，供启动日志与探针使用。 */
	snapshot() {
		const count = (sql) => {
			return this.db.prepare(sql).get()?.n ?? 0;
		};
		return {
			cursor_len: this.get_cursor().length,
			routes: this.route_summary(),
			delivery: {
				received: count("SELECT COUNT(*) AS n FROM delivery WHERE status = 'received'"),
				injected: count("SELECT COUNT(*) AS n FROM delivery WHERE status = 'injected'"),
				control_applied: count("SELECT COUNT(*) AS n FROM delivery WHERE status = 'control_applied'"),
				failed_retryable: count("SELECT COUNT(*) AS n FROM delivery WHERE status = 'failed_retryable'"),
				failed_terminal: count("SELECT COUNT(*) AS n FROM delivery WHERE status = 'failed_terminal'")
			},
			outbox: {
				pending: count("SELECT COUNT(*) AS n FROM outbox WHERE status = 'pending'"),
				sending: count("SELECT COUNT(*) AS n FROM outbox WHERE status = 'sending'"),
				sent: count("SELECT COUNT(*) AS n FROM outbox WHERE status = 'sent'"),
				failed_retryable: count("SELECT COUNT(*) AS n FROM outbox WHERE status = 'failed_retryable'"),
				failed_terminal: count("SELECT COUNT(*) AS n FROM outbox WHERE status = 'failed_terminal'")
			},
			lease: this.get_lease() ?? null
		};
	}
};
/**
* 判断 pid 是否仍存活。
* EPERM 表示进程存在但无权限，同样算存活；其余错误（ESRCH 等）视为已退出。
*/
function is_pid_alive(pid) {
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		return error.code === "EPERM";
	}
}
/**
* 读取某个 pid 的**进程启动时刻**（毫秒）；拿不到返回 undefined。
*
* 为什么需要它：Windows 会**回收 pid**。一个被强杀的持有者留下的 pid 可能被
* 另一个无关进程复用，于是"pid 存在 + 心跳还新"会让 lease 误判成"还活着"，
* 连接器就拒绝启动 —— 表现为"重启后怎么都起不来、微信不回消息"（实测踩过）。
* 真正可靠的身份判据是**启动时刻**：同一个 pid 的启动时刻对不上，就说明持有者已死。
*/
function process_started_at(pid) {
	try {
		if (process.platform === "win32") {
			const out = execFileSync("powershell.exe", [
				"-NoLogo",
				"-NoProfile",
				"-Command",
				`[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; $p = Get-Process -Id ${pid} -ErrorAction SilentlyContinue; if ($p) { Write-Output ([DateTimeOffset]$p.StartTime).ToUnixTimeMilliseconds() }`
			], {
				encoding: "utf8",
				timeout: 5e3,
				windowsHide: true
			});
			const parsed = Number(out.trim());
			return Number.isFinite(parsed) && parsed > 0 ? parsed : void 0;
		}
		const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
		const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
		const startTicks = Number(fields[19]);
		if (!Number.isFinite(startTicks)) return void 0;
		const bootTime = readFileSync("/proc/stat", "utf8").split("\n").find((line) => line.startsWith("btime "))?.split(" ")[1];
		const uptimeSeconds = Number(bootTime);
		if (!Number.isFinite(uptimeSeconds)) return void 0;
		return (uptimeSeconds + startTicks / 100) * 1e3;
	} catch {
		return;
	}
}
/**
* 判断 lease 里的持有者是否**真的还在**：pid 存活 **且** 启动时刻对得上。
* 启动时刻拿不到时（权限/平台差异）退回"仅 pid 存活"，保持原有行为。
*/
function lease_holder_alive(pid, recorded_started_at) {
	if (!is_pid_alive(pid)) return false;
	const actual = process_started_at(pid);
	if (actual === void 0) return true;
	return Math.abs(actual - recorded_started_at) <= 6e4;
}
/** 行 → DeliveryRecord。 */
function to_delivery(row) {
	return {
		delivery_id: row.delivery_id,
		user_id: row.user_id,
		...row.session_id === null ? {} : { session_id: row.session_id },
		status: row.status,
		received_at: row.received_at,
		...row.injected_at === null ? {} : { injected_at: row.injected_at },
		...row.last_error === null ? {} : { last_error: row.last_error },
		attempt: row.attempt,
		...row.context_token === null ? {} : { context_token: row.context_token },
		...row.payload_json === null ? {} : { payload_json: row.payload_json }
	};
}
/** 行 → OutboxRecord。 */
function to_outbox(row) {
	return {
		outbound_id: row.outbound_id,
		session_id: row.session_id,
		user_id: row.user_id,
		...row.turn === null ? {} : { turn: row.turn },
		sequence: row.sequence,
		kind: row.kind,
		text: row.text,
		...row.context_token === null ? {} : { context_token: row.context_token },
		client_id: row.client_id,
		status: row.status,
		attempt: row.attempt,
		...row.next_retry_at === null ? {} : { next_retry_at: row.next_retry_at },
		...row.last_error === null ? {} : { last_error: row.last_error },
		created_at: row.created_at,
		...row.sent_at === null ? {} : { sent_at: row.sent_at },
		sent_parts: parse_sent_parts(row.sent_parts_json)
	};
}
/** 解析 outbox 的已发送分段下标；任何异常都按"没有已发送段"处理（最坏只是重发）。 */
function parse_sent_parts(raw) {
	if (raw === null || raw === "") return [];
	try {
		const parsed = JSON.parse(raw);
		if (!Array.isArray(parsed)) return [];
		return parsed.filter((item) => typeof item === "number");
	} catch {
		return [];
	}
}
//#endregion
//#region lib/types/state/turnContextStore.js
/**
* Turn 级回复上下文：把 `session_id + turn` 绑定到产生该 turn 的入站 delivery 与其 context_token。
*
* 重构设计文档 §7.1：回复层必须按 turn 查找上下文，而不是读"当前 Session 的最后一条 token"。
* 旧实现用 Session 级单值 Map，长任务期间连续来消息会让旧 turn 误用新 token。
*
* 本模块只是 WeixinStateStore 的语义化包装，保证调用点不直接拼表名。
* @module dsh-webhook-weixin/turn-context
*/
/** Turn 上下文的读写入口。 */
var TurnContextStore = class {
	store;
	/** 绑定到状态库。 */
	constructor(store) {
		this.store = store;
	}
	/** 绑定 `session_id + turn`；同 turn 重复绑定会覆盖为最新。 */
	bind(session_id, turn, context) {
		this.store.bind_turn_context({
			session_id,
			turn,
			delivery_id: context.delivery_id,
			user_id: context.user_id,
			...context.context_token === void 0 ? {} : { context_token: context.context_token }
		});
	}
	/** 读取 turn 上下文；未绑定返回 undefined（回复层应回退到最近一条，而不是别的 turn 的 token）。 */
	get(session_id, turn) {
		return this.store.get_turn_context(session_id, turn);
	}
	/**
	* 仅在**尚未绑定**时写入（first-wins）。
	*
	* 这是修复"同一 turn 的回复上下文被后续消息覆盖"的关键原语：
	* 长任务期间用户再发消息时，那条消息属于下一个 turn，
	* 绝不能改写本轮回复应当使用的 delivery / context_token。
	*
	* @returns true = 本次完成绑定；false = 已存在绑定，保持原值不变。
	*/
	bind_if_absent(session_id, turn, context) {
		if (this.get(session_id, turn) !== void 0) {
			probe("turn-context", "bind.skipped_exists", {
				session_id,
				turn,
				delivery_id: context.delivery_id
			});
			return false;
		}
		this.bind(session_id, turn, context);
		return true;
	}
	/**
	* 解析用于回复的上下文：
	* 1. 优先精确命中 `session_id + turn`；
	* 2. 未命中（重启/历史 turn）时回退到该会话最近一条已注入的 delivery；
	* 3. 仍无则返回 undefined —— 此时必须**无 token 发送**或报错，绝不复用其它 turn 的 token。
	*/
	resolve(session_id, turn) {
		const exact = this.get(session_id, turn);
		if (exact !== void 0) return exact;
		const fallback = this.store.latest_delivery_for_session(session_id);
		if (fallback === void 0) {
			probe("turn-context", "resolve.miss", {
				session_id,
				turn
			});
			return;
		}
		probe("turn-context", "resolve.fallback", {
			session_id,
			turn,
			delivery_id: fallback.delivery_id,
			received_at: fallback.received_at,
			has_context_token: fallback.context_token !== void 0
		});
		return {
			delivery_id: fallback.delivery_id,
			user_id: fallback.user_id,
			...fallback.context_token === void 0 ? {} : { context_token: fallback.context_token }
		};
	}
	/**
	* 该会话最近一次可用的回复上下文（含 context_token）；没有则 undefined。
	*
	* 给"不针对某一轮"的消息（通知/进度）用：它们没有自己的 turn，
	* 但带上最近的 token 才有机会在会话窗口开着时送达。
	*/
	latest(session_id) {
		const fallback = this.store.latest_delivery_for_session(session_id);
		if (fallback === void 0) return void 0;
		return {
			delivery_id: fallback.delivery_id,
			user_id: fallback.user_id,
			...fallback.context_token === void 0 ? {} : { context_token: fallback.context_token }
		};
	}
	/**
	* 该会话最近一次的 context_token，**但只在它足够新鲜时才返回**。
	*
	* 为什么需要"新鲜度"判断：token 会过期，而**过期 token 的发送必然被拒**。
	* 实测教训：给通知类无脑带上"最近一次 token"后，旧 token 被拒 →
	* 被误当成真限流 → 全局熔断冻结整条链路（10 小时发不出消息）。
	* 所以这里只在新入站刚发生（会话窗口还开着）时才带 token，否则宁可不带。
	*
	* @param session_id - 会话。
	* @param max_age_ms - 允许多旧，默认 10 分钟。
	*/
	latest_token_if_fresh(session_id, max_age_ms = 10 * 6e4) {
		const fallback = this.store.latest_delivery_for_session(session_id);
		if (fallback?.context_token === void 0) return void 0;
		return Date.now() - fallback.received_at <= max_age_ms ? fallback.context_token : void 0;
	}
	/** 一轮结束后清理历史，避免表无限增长。 */
	prune(session_id, keep = 20) {
		this.store.prune_turn_context(session_id, keep);
	}
};
//#endregion
//#region lib/types/state/migrateLegacyRouteState.js
/**
* 旧路由状态（JSON）→ SQLite 的一次性迁移。
*
* 重构设计文档 §11 阶段 5：旧文件**只读保留**，迁移失败时禁止从空状态启动。
* 迁移是幂等的：目标库已有数据时直接跳过，不会重复导入。
* @module dsh-webhook-weixin/migrate-legacy
*/
/**
* 把旧 JSON 路由状态导入 SQLite。
*
* @param store - 目标状态库。
* @param legacy_path - 旧 JSON 路径；不存在时视为无需迁移。
* @throws 旧文件存在但损坏/无法解析时抛出（fail closed，禁止静默从空状态启动）。
*/
function migrate_legacy_route_state(store, legacy_path) {
	if (!existsSync(legacy_path)) return {
		migrated: false,
		reason: "legacy_absent",
		sessions: 0,
		deliveries: 0,
		cursor: false
	};
	const existing = store.route_summary();
	if (existing.users > 0 || store.get_cursor().length > 0) {
		probe("state", "migrate.skip", {
			reason: "target_not_empty",
			users: existing.users
		});
		return {
			migrated: false,
			reason: "target_not_empty",
			sessions: 0,
			deliveries: 0,
			cursor: false
		};
	}
	let parsed;
	try {
		parsed = JSON.parse(readFileSync(legacy_path, "utf8"));
	} catch (error) {
		probe("state", "migrate.failed", { error: String(error) });
		throw new Error(`旧路由状态无法解析，拒绝从空状态启动（fail closed）：${legacy_path} —— ${error instanceof Error ? error.message : String(error)}`);
	}
	const snapshot = `${legacy_path}.migrated-${Date.now()}`;
	try {
		copyFileSync(legacy_path, snapshot);
	} catch (error) {
		probe("state", "migrate.snapshot_failed", { error: String(error) });
	}
	const sessions = Object.entries(parsed.sessions ?? {});
	const deliveries = parsed.deliveries ?? [];
	store.transaction(() => {
		for (const [user_id, session_id] of sessions) store.set_session_id(user_id, session_id);
		for (const delivery_id of deliveries) {
			const user_id = sessions.find(([, sid]) => sid !== void 0)?.[0] ?? "legacy";
			if (store.put_delivery_if_absent({
				delivery_id,
				user_id,
				received_at: Date.now()
			})) store.update_delivery(delivery_id, "injected", {
				injected_at: Date.now(),
				session_id: user_id
			});
		}
		if (parsed.cursor) store.set_cursor(parsed.cursor);
	});
	const result = {
		migrated: true,
		reason: "migrated",
		sessions: sessions.length,
		deliveries: deliveries.length,
		cursor: Boolean(parsed.cursor)
	};
	probe("state", "migrate.done", { ...result });
	console.error(`[webhook-weixin] 已迁移旧路由状态：路由 ${result.sessions} 条、去重 ${result.deliveries} 条、游标 ${result.cursor ? "有" : "无"}；旧文件保留在 ${legacy_path}`);
	return result;
}
//#endregion
//#region lib/types/transport/weixinIngress.js
/**
* 微信入站传输层：只负责 HTTP 长轮询、协议错误分类、退避与规范化。
*
* 重构设计文档 §3.2：WeixinIngress **不**创建 Agent、**不**保存回复上下文、
* **不**处理工具调用、**不**决定 Session 生命周期；它只把"一批原始消息"变成"一批规范化信封"。
*
* 状态归属：本模块只维护 HTTP 重试计数，不写任何持久状态（cursor 由协调器在批次完成后提交）。
* @module dsh-webhook-weixin/ingress
*/
/** iLink getUpdates 的传输封装。 */
var WeixinIngress = class {
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
		probe("ingress", "notify_start.ok");
	}
	/** 通知服务端消费者下线；尽力而为。 */
	async notify_stop(signal) {
		try {
			await this.client.notify_stop(signal);
			probe("ingress", "notify_stop.ok");
		} catch (error) {
			probe("ingress", "notify_stop.failed", { error: String(error) });
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
		} catch (error) {
			if (signal?.aborted) throw error;
			if (error instanceof Error && error.name === "AbortError") {
				probe("ingress", "poll.long_poll_timeout");
				return { kind: "session_timeout" };
			}
			probe("ingress", "poll.network_error", { error: String(error) });
			this.observer.on_error?.(error);
			return {
				kind: "protocol_error",
				detail: error instanceof Error ? error.message : String(error)
			};
		}
		if ((response.errmsg?.toLowerCase() ?? "").includes("session timeout")) {
			probe("ingress", "poll.session_timeout");
			return { kind: "session_timeout" };
		}
		const ret = response.ret ?? 0;
		const errcode = response.errcode ?? 0;
		if (ret !== 0 || errcode !== 0) {
			const detail = response.errmsg ?? String(ret !== 0 ? ret : errcode);
			if (ret === 401 || ret === 403 || errcode === 401 || errcode === 403 || /token|auth|unauthor|login|expired/i.test(detail)) {
				probe("ingress", "poll.auth_required", {
					ret,
					errcode,
					detail
				});
				this.observer.on_status?.("auth_required", detail);
				return {
					kind: "auth_required",
					detail
				};
			}
			probe("ingress", "poll.protocol_error", {
				ret,
				errcode,
				detail
			});
			this.observer.on_error?.(/* @__PURE__ */ new Error(`iLink getUpdates failed: ${detail}`));
			return {
				kind: "protocol_error",
				detail
			};
		}
		const raw = response.msgs ?? [];
		const cursor_next = response.get_updates_buf;
		if (raw.length === 0) {
			probe("ingress", "poll.idle");
			return {
				kind: "idle",
				cursor: cursor_next
			};
		}
		const messages = raw.map((message) => this.normalizer.normalize_message(message));
		probe("ingress", "poll.messages", {
			count: messages.length,
			users: [...new Set(messages.map((m) => m.user_id))],
			delivery_ids: messages.map((m) => m.delivery_id)
		});
		return {
			kind: "messages",
			messages,
			cursor: cursor_next
		};
	}
	/** 下载媒体（供协调器的分片调度调用，不阻塞轮询）。 */
	async download_media(media) {
		return this.client.download_media(media);
	}
	/** 暴露规范化器的媒体补全能力，供协调器在分片任务中使用。 */
	get media_normalizer() {
		return this.normalizer;
	}
};
//#endregion
//#region lib/types/coordination/inboundCoordinator.js
/**
* 入站协调器：微信链路唯一的消息协调者。
*
* 重构设计文档 §3.2 / §6：负责幂等登记、路由、控制命令、投递顺序与恢复。
* 它**不**维护模型执行状态（那是 Harness Agent inbox 的职责），**不**维护第二套队列，
* **不**负责发送（那是 OutboundCoordinator）。
*
* 关键变化（对应诊断 1/2/4/5/6）：
* - 全局单串行链 → 按 user_id 分片的 keyed scheduler（同用户保序，不同用户并发）；
* - 控制命令也走同一个状态机并做 delivery 去重；
* - 停止回执使用真实路由 SessionId，而不是 `weixin-${user_id}` 拼接；
* - cursor 提交改为"批次内所有 delivery 达到终态"语义；
* - 媒体下载与附件保存只阻塞该用户，不阻塞全局。
* @module dsh-webhook-weixin/inbound-coordinator
*/
/**
* 把规范化入站消息序列化成可持久化 JSON。
*
* 刻意**剥离二进制**（data/raw_data）：只保留文本与媒体下载引用（`media`），
* 恢复时若需要原件再按引用重新下载（见 hydrate_message 注入）。
*/
function serialize_payload(message) {
	return JSON.stringify({
		delivery_id: message.delivery_id,
		user_id: message.user_id,
		...message.context_token === void 0 ? {} : { context_token: message.context_token },
		text: message.text,
		received_at: message.received_at,
		media: message.media.map((item) => ({
			kind: item.kind,
			...item.name === void 0 ? {} : { name: item.name },
			...item.mime_type === void 0 ? {} : { mime_type: item.mime_type },
			...item.source_text === void 0 ? {} : { source_text: item.source_text },
			...item.media === void 0 ? {} : { media: item.media }
		}))
	});
}
/** 反序列化入站消息；结构不可信时返回 undefined（宁可判为无法恢复）。 */
function deserialize_payload(raw) {
	try {
		const parsed = JSON.parse(raw);
		if (typeof parsed.delivery_id !== "string" || typeof parsed.user_id !== "string") return void 0;
		return {
			delivery_id: parsed.delivery_id,
			user_id: parsed.user_id,
			...parsed.context_token === void 0 ? {} : { context_token: parsed.context_token },
			text: typeof parsed.text === "string" ? parsed.text : "",
			media: Array.isArray(parsed.media) ? parsed.media : [],
			received_at: typeof parsed.received_at === "number" ? parsed.received_at : Date.now()
		};
	} catch {
		return;
	}
}
/**
* 把同一合并窗口里的若干条消息并成"一条"。
*
* 规则：文本按收到顺序用换行连起来；媒体依次拼接；**context_token 取最后一条**——
* 回复窗口是"用户最近一次发言"开的，用最后那条的 token 才发得出去。
*/
function merge_messages(messages) {
	const last = messages[messages.length - 1];
	const first = messages[0];
	const text = messages.map((item) => item.text.trim()).filter((item) => item !== "").join("\n");
	const media = messages.flatMap((item) => [...item.media]);
	return {
		...last,
		delivery_id: first.delivery_id,
		text,
		media,
		received_at: last.received_at
	};
}
/** 仅媒体、无文字时最多缓存多少条媒体。 */
const PENDING_MEDIA_LIMIT = 20;
/**
* 合并窗口（毫秒）：连续消息攒这么久再一次性交给模型。设 0 关闭合并（逐条立刻处理）。
*
* 取 6 秒是实测折中：打字停顿一般 1～3 秒，6 秒足够把"在吗 / 帮我看下 / 那个文件"
* 收成一次提问，又不会让人觉得机器反应迟钝。
*/
const COALESCE_MS = Number(process.env.DSH_WEIXIN_COALESCE_MS ?? "6000");
/** 单条媒体下载/附件保存的超时（毫秒）。 */
const MEDIA_TIMEOUT_MS = Number(process.env.DSH_WEIXIN_MEDIA_TIMEOUT_MS ?? "120000");
/**
* 单条 delivery 的最大处理尝试次数。
* 超过就转 failed_terminal，让批次 cursor 能提交，避免"失败→重投→再失败"的热循环。
*/
const MAX_DELIVERY_ATTEMPTS = Number(process.env.DSH_WEIXIN_DELIVERY_MAX_ATTEMPTS ?? "3");
/**
* 单个用户入站链的最大排队深度（背压阈值）。
*
* 超过就拒绝新消息并如实回执：没有上限时高频消息会无限占内存，
* 而且排在后面的任务其实早就"过期"了，做出来也没意义。
* 正常聊天远达不到这个数（一轮对话也就几条）。
*/
const MAX_SHARD_DEPTH = Number(process.env.DSH_WEIXIN_MAX_INBOUND_DEPTH ?? "30");
/** 取消当前任务时是否同时清理排队消息（设计文档 §4.2 默认语义：清理）。 */
const CANCEL_CLEARS_PENDING = true;
/** 取消类指令。 */
const CANCEL_COMMANDS = new Set([
	"停止",
	"取消",
	"中断",
	"别做了",
	"停下",
	"stop",
	"cancel",
	"abort",
	"中断任务",
	"停止任务"
]);
/** 保留队列的取消指令（显式语义，不是隐式行为）。 */
const CANCEL_KEEP_COMMANDS = new Set([
	"停止当前但保留队列",
	"取消当前但保留队列",
	"stop-keep-queue"
]);
/** 插话指令前缀。 */
const STEER_PREFIXES = [
	"插话：",
	"插话:",
	"补充：",
	"补充:",
	"纠正：",
	"纠正:",
	"steer:",
	"steer："
];
/** 状态查询指令。 */
const STATUS_COMMANDS = new Set(["状态", "status"]);
/** 解析控制命令；普通正文返回 undefined。 */
function parse_control_command(text) {
	const trimmed = text.trim();
	const lower = trimmed.toLowerCase();
	if (CANCEL_COMMANDS.has(lower)) return {
		kind: "cancel_current",
		clear_pending: CANCEL_CLEARS_PENDING
	};
	if (CANCEL_KEEP_COMMANDS.has(lower)) return {
		kind: "cancel_current",
		clear_pending: false
	};
	if (STATUS_COMMANDS.has(lower)) return { kind: "status" };
	for (const prefix of STEER_PREFIXES) if (lower.startsWith(prefix.toLowerCase())) {
		const rest = trimmed.slice(prefix.length).trim();
		if (rest.length > 0) return {
			kind: "steer_current",
			text: rest
		};
	}
}
/** 微信入站协调器。 */
var InboundCoordinator = class {
	options;
	disposed = false;
	shards = /* @__PURE__ */ new Map();
	pending_media = /* @__PURE__ */ new Map();
	/** 各用户最近一次 context_token（媒体被动回复要用） */
	latest_token = /* @__PURE__ */ new Map();
	/**
	* 同一个人连续发的消息先攒着，静默 COALESCE_MS 之后**合并成一条**喂给模型。
	*
	* 为什么要攒：人是断续打的（"在吗" → "帮我看看" → "那个文件"），逐条喂会得到三次回答，
	* 又慢又吵；合并成一次提问，模型也更容易答到点上。控制命令（`/codex`、`/opencode`）
	* 与纯媒体消息**不缓冲**，立刻处理。
	*/
	coalesced = /* @__PURE__ */ new Map();
	/** 合并窗口内最近一次收到消息的时间（用于日志里的"等了多久"）。 */
	coalesced_last_at = /* @__PURE__ */ new Map();
	/** delivery 成功注入后记录的 `delivery_id → session_id`，供 outbox 绑定使用。 */
	delivery_sessions = /* @__PURE__ */ new Map();
	/** 绑定状态库与宿主。 */
	constructor(options) {
		this.options = options;
	}
	/** 暴露 delivery → session 映射（outbox 需要）。 */
	session_for_delivery(delivery_id) {
		return this.delivery_sessions.get(delivery_id);
	}
	/**
	* 处理一个批次：逐条登记 + 分片投递，最后按"全部达到终态"决定是否提交 cursor。
	*
	* @param messages - 本批消息
	* @param cursor - 平台游标
	* @param options.hold - true 时**只登记不投递**（压缩上下文期间用）：
	*   delivery 停在 `received` 且 payload 已落库，压缩结束后由 `recover_unfinished()` 重投。
	*   此时故意不返回可提交，让游标保持未提交、消息不会丢。
	* @returns 是否允许提交该批次 cursor。
	*/
	async handle_batch(messages, cursor, options) {
		const done = probe_timer("inbound", "batch.start", {
			count: messages.length,
			has_cursor: cursor !== void 0,
			hold: options?.hold === true
		});
		const tasks = [];
		for (const message of messages) {
			if (!this.options.store.put_delivery_if_absent({
				delivery_id: message.delivery_id,
				user_id: message.user_id,
				received_at: message.received_at,
				...message.context_token === void 0 ? {} : { context_token: message.context_token },
				payload_json: serialize_payload(message)
			})) {
				probe("inbound", "batch.skip_duplicate", { delivery_id: message.delivery_id });
				if (this.options.store.get_delivery(message.delivery_id)?.status === "failed_retryable") tasks.push(this.enqueue_for_user(message));
				continue;
			}
			this.options.record_entry?.({
				id: message.delivery_id,
				ts: message.received_at,
				role: "user",
				text: message.text,
				user_id: message.user_id,
				...this.options.store.get_session_id(message.user_id) === void 0 ? {} : { session_id: this.options.store.get_session_id(message.user_id) }
			});
			if (options?.hold === true) {
				probe("inbound", "batch.held", {
					delivery_id: message.delivery_id,
					user_id: message.user_id
				});
				continue;
			}
			tasks.push(this.enqueue_for_user(message));
		}
		const rejected = (await Promise.allSettled(tasks)).filter((item) => item.status === "rejected").length;
		if (rejected > 0) probe("inbound", "batch.task_rejected", { rejected });
		const ids = messages.map((message) => message.delivery_id);
		const committable = this.options.store.batch_committable(ids);
		done({
			committable,
			rejected
		});
		return committable;
	}
	/** 把一条消息排进其所属用户的分片链；同用户严格保序，不同用户并发。 */
	enqueue_for_user(message, run) {
		if (this.disposed) return Promise.resolve();
		const previous = this.shards.get(message.user_id) ?? {
			chain: Promise.resolve(),
			depth: 0
		};
		if (previous.depth >= MAX_SHARD_DEPTH) {
			probe("inbound", "shard.overflow", {
				user_id: message.user_id,
				depth: previous.depth,
				limit: MAX_SHARD_DEPTH
			});
			this.options.store.update_delivery(message.delivery_id, "failed_terminal", {
				last_error: `入站队列过深（${previous.depth} ≥ ${MAX_SHARD_DEPTH}），已丢弃`,
				increment_attempt: true
			});
			this.options.enqueue_outbound({
				session_id: this.options.store.get_session_id(message.user_id) ?? `weixin-${message.user_id}`,
				user_id: message.user_id,
				kind: "error",
				text: `消息一下有点多啦，这条我先放一放（队列已满 ${MAX_SHARD_DEPTH} 条）😅 稍后再发一次好吗？`
			});
			return Promise.resolve();
		}
		const shard = {
			chain: previous.chain,
			depth: previous.depth + 1
		};
		this.shards.set(message.user_id, shard);
		probe("inbound", "shard.enqueue", {
			user_id: message.user_id,
			depth: shard.depth
		});
		const task = shard.chain.then(async () => {
			await (run === void 0 ? this.process_message(message) : run());
		});
		shard.chain = task.catch((error) => {
			probe("inbound", "shard.task_failed", {
				user_id: message.user_id,
				delivery_id: message.delivery_id,
				error: String(error)
			});
		}).finally(() => {
			const current = this.shards.get(message.user_id);
			if (current !== void 0) {
				current.depth = Math.max(0, current.depth - 1);
				if (current.depth === 0) this.shards.delete(message.user_id);
			}
		});
		return task;
	}
	/** 处理单条消息：Codex 指令 / 控制命令 / 普通消息。 */
	async process_message(message) {
		if (this.options.hydrate_message !== void 0 && message.media.length > 0) try {
			message = await this.options.hydrate_message(message);
		} catch (error) {
			probe("inbound", "media.hydrate_failed", {
				delivery_id: message.delivery_id,
				error: String(error)
			});
		}
		const control = parse_control_command(message.text);
		try {
			const oc_command = this.options.opencode_bridge?.parse(message.text);
			if (oc_command !== void 0 && this.options.opencode_bridge !== void 0) {
				const session_id = this.options.store.get_session_id(message.user_id) ?? `weixin-${message.user_id}`;
				this.options.store.update_delivery(message.delivery_id, "control_applied", { injected_at: Date.now() });
				probe("inbound", "opencode.command", {
					delivery_id: message.delivery_id,
					action: oc_command.action
				});
				await this.options.opencode_bridge.handle(message.user_id, session_id, oc_command, message.context_token);
				return;
			}
			const codex_command = this.options.codex_bridge?.parse(message.text);
			if (codex_command !== void 0 && this.options.codex_bridge !== void 0) {
				const session_id = this.options.store.get_session_id(message.user_id) ?? `weixin-${message.user_id}`;
				this.options.store.update_delivery(message.delivery_id, "control_applied", { injected_at: Date.now() });
				probe("inbound", "codex.command", {
					delivery_id: message.delivery_id,
					action: codex_command.action
				});
				await this.options.codex_bridge.handle(message.user_id, session_id, codex_command, message.context_token);
				return;
			}
			if (control !== void 0) {
				await this.apply_control(message, control);
				return;
			}
			await this.coalesce_message(message);
		} catch (error) {
			this.handle_message_failure(message, error);
		}
	}
	/** message 为原始投递，error 为处理异常；统一计次、记录原错并在达到上限时发送失败回执。 */
	handle_message_failure(message, error) {
		if (this.disposed) return;
		const current = this.options.store.get_delivery(message.delivery_id);
		if (current !== void 0 && is_cursor_committable(current.status)) return;
		const detail = error instanceof Error ? error.message : String(error);
		const attempt = (this.options.store.get_delivery(message.delivery_id)?.attempt ?? 0) + 1;
		if (attempt >= MAX_DELIVERY_ATTEMPTS) {
			this.options.store.update_delivery(message.delivery_id, "failed_terminal", {
				last_error: detail,
				increment_attempt: true
			});
			probe("inbound", "message.failed_terminal", {
				delivery_id: message.delivery_id,
				attempt,
				error: detail
			});
			this.options.enqueue_outbound({
				session_id: this.options.store.get_session_id(message.user_id) ?? `weixin-${message.user_id}`,
				user_id: message.user_id,
				kind: "error",
				text: "（这条消息处理失败了，我已经记录下来；请稍后再发一次或告诉我换种说法）",
				...message.context_token === void 0 ? {} : { context_token: message.context_token }
			});
			return;
		}
		this.options.store.update_delivery(message.delivery_id, "failed_retryable", {
			last_error: detail,
			increment_attempt: true
		});
		probe("inbound", "message.failed", {
			delivery_id: message.delivery_id,
			attempt,
			error: detail
		});
	}
	/** 应用控制命令（幂等：delivery 已登记过就不会重复执行）。 */
	async apply_control(message, control) {
		const done = probe_timer("inbound", "control.apply", {
			delivery_id: message.delivery_id,
			user_id: message.user_id,
			command: control.kind
		});
		const session_id = this.options.store.get_session_id(message.user_id);
		if (session_id === void 0) {
			this.options.store.update_delivery(message.delivery_id, "control_applied");
			this.options.enqueue_outbound({
				session_id: `weixin-unrouted:${message.user_id}`,
				user_id: message.user_id,
				kind: "control_ack",
				text: "（当前没有正在进行的任务）",
				...message.context_token === void 0 ? {} : { context_token: message.context_token }
			});
			done({ outcome: "no_route" });
			return;
		}
		const agent = this.options.host.get_agent(brandString(session_id));
		if (agent === void 0) {
			this.options.store.update_delivery(message.delivery_id, "control_applied");
			this.options.enqueue_outbound({
				session_id,
				user_id: message.user_id,
				kind: "control_ack",
				text: control.kind === "status" ? "（当前空闲，没有正在进行的任务）" : "（当前没有正在进行的任务）",
				...message.context_token === void 0 ? {} : { context_token: message.context_token }
			});
			done({ outcome: "no_agent" });
			return;
		}
		if (control.kind === "cancel_current") if (agent.status === "idle") this.options.enqueue_outbound({
			session_id,
			user_id: message.user_id,
			kind: "control_ack",
			text: "（当前没有正在进行的任务）",
			...message.context_token === void 0 ? {} : { context_token: message.context_token }
		});
		else {
			agent.cancel({ kind: "user" }, { keepInbox: !control.clear_pending });
			this.options.store.delete_meta(`${INFLIGHT_TURN_PREFIX}${session_id}`);
			this.options.on_turn_cancelled?.(session_id);
			const stopped = await this.options.on_cancel?.(message.user_id);
			const external = stopped === void 0 || stopped.killed === 0 ? "" : `，并中断了 ${stopped.killed} 个外部任务`;
			this.options.enqueue_outbound({
				session_id,
				user_id: message.user_id,
				kind: "control_ack",
				text: `（已取消当前这一轮${external}${control.clear_pending ? "，排队消息已清空" : "，排队消息保留"}）`,
				...message.context_token === void 0 ? {} : { context_token: message.context_token }
			});
		}
		else if (control.kind === "steer_current") if (agent.status === "idle") this.options.enqueue_outbound({
			session_id,
			user_id: message.user_id,
			kind: "control_ack",
			text: "（没有正在进行的任务，插话未生效；请直接发消息）",
			...message.context_token === void 0 ? {} : { context_token: message.context_token }
		});
		else {
			agent.steer(createUserMessage({
				content: [{
					type: "text",
					text: control.text
				}],
				source: {
					kind: "plugin",
					plugin: "dsh-webhook-weixin",
					form: "notice",
					summary: "微信插话"
				}
			}));
			this.options.enqueue_outbound({
				session_id,
				user_id: message.user_id,
				kind: "control_ack",
				text: "（已把你的补充插进当前任务）",
				...message.context_token === void 0 ? {} : { context_token: message.context_token }
			});
		}
		else this.options.enqueue_outbound({
			session_id,
			user_id: message.user_id,
			kind: "control_ack",
			text: `（状态：Agent ${agent.status}）`,
			...message.context_token === void 0 ? {} : { context_token: message.context_token }
		});
		this.options.store.update_delivery(message.delivery_id, "control_applied", {
			session_id,
			injected_at: Date.now()
		});
		done({
			outcome: "applied",
			agent_status: agent.status
		});
	}
	/**
	* 普通消息入口：先攒进合并窗口，静默一段时间后一次性注入。
	*
	* 不缓冲的情况：
	* - 关闭了合并（COALESCE_MS = 0）；
	* - 只有媒体没有文字 —— 那种要等用户补文字，交给 `inject_message` 自己的媒体缓存逻辑。
	*/
	async coalesce_message(message) {
		const text = message.text.trim();
		if (COALESCE_MS <= 0 || text === "" && message.media.length > 0) {
			await this.inject_message(message);
			return;
		}
		this.begin_routing(message.delivery_id);
		const existing = this.coalesced.get(message.user_id);
		if (existing !== void 0) clearTimeout(existing.timer);
		const messages = [...existing?.messages ?? [], message];
		const first_at = existing?.first_at ?? Date.now();
		const timer = setTimeout(() => {
			this.enqueue_for_user(message, () => this.flush_coalesced(message.user_id)).catch((error) => probe("inbound", "coalesce.scheduler_failed", { error: String(error) }));
		}, COALESCE_MS);
		this.coalesced.set(message.user_id, {
			messages,
			timer,
			first_at
		});
		this.coalesced_last_at.set(message.user_id, Date.now());
		probe("inbound", "coalesce.buffered", {
			user_id: message.user_id,
			pending: messages.length,
			window_ms: COALESCE_MS
		});
	}
	/** 合并窗口到期：把攒下的消息并成一条注入（游标要等这条注入完才提交）。 */
	async flush_coalesced(user_id) {
		const bucket = this.coalesced.get(user_id);
		if (bucket === void 0) return;
		this.coalesced.delete(user_id);
		const waited = Date.now() - (this.coalesced_last_at.get(user_id) ?? Date.now());
		this.coalesced_last_at.delete(user_id);
		const merged = merge_messages(bucket.messages);
		const extras = bucket.messages.slice(1).map((item) => item.delivery_id);
		probe("inbound", "coalesce.flushed", {
			user_id,
			messages: bucket.messages.length,
			chars: merged.text.length,
			waited_ms: Date.now() - bucket.first_at,
			idle_ms: waited
		});
		try {
			await this.inject_message(merged, extras);
		} catch (error) {
			probe("inbound", "coalesce.inject_failed", {
				user_id,
				error: String(error)
			});
			for (const message of bucket.messages) this.handle_message_failure(message, error);
		}
	}
	/** delivery_id 为已登记投递；复用既有 routing，只从明确可处理状态进入路由。 */
	begin_routing(delivery_id) {
		const row = this.options.store.get_delivery(delivery_id);
		if (row?.status === "routing") return;
		const status = row?.status ?? "received";
		if (status !== "received" && status !== "failed_retryable") throw new Error(`delivery 不可路由：${status}`);
		if (this.options.store.update_delivery(delivery_id, "routing", { expect: status }) === false) throw new Error("delivery 状态已变更，路由未获得所有权");
	}
	/** 无参数；取消尚未到期的合并计时器，保留数据库载荷供下次恢复。 */
	async dispose() {
		this.disposed = true;
		for (const bucket of this.coalesced.values()) clearTimeout(bucket.timer);
		this.coalesced.clear();
		this.coalesced_last_at.clear();
		await Promise.allSettled([...this.shards.values()].map((shard) => shard.chain));
	}
	/** 普通消息：媒体 → Agent → followup → 标记 injected。 */
	async inject_message(message, also_mark = []) {
		if (this.disposed) return;
		const done = probe_timer("inbound", "message.inject", {
			delivery_id: message.delivery_id,
			user_id: message.user_id
		});
		this.begin_routing(message.delivery_id);
		const text = message.text.trim();
		if (!text && message.media.length > 0) {
			const pending = this.pending_media.get(message.user_id) ?? [];
			pending.push(...message.media);
			while (pending.length > PENDING_MEDIA_LIMIT) pending.shift();
			this.pending_media.set(message.user_id, pending);
			this.options.store.update_delivery(message.delivery_id, "control_applied", { injected_at: Date.now() });
			done({
				outcome: "media_buffered",
				buffered: pending.length
			});
			return;
		}
		const buffered = this.pending_media.get(message.user_id);
		if (buffered !== void 0 && buffered.length > 0) this.pending_media.delete(message.user_id);
		const media = buffered === void 0 || buffered.length === 0 ? message.media : [...buffered, ...message.media];
		let session_id = this.options.store.get_session_id(message.user_id);
		let agent;
		if (session_id !== void 0) {
			const branded = brandString(session_id);
			agent = this.options.host.get_agent(branded) ?? await this.options.host.resume_agent(branded);
		} else {
			session_id = this.options.store.session_id_for(message.user_id);
			agent = await this.options.host.create_agent(brandString(session_id));
			this.options.store.set_session_id(message.user_id, session_id);
		}
		const session_key = String(agent.id);
		if (this.disposed) return;
		this.options.on_agent_ready?.(agent);
		this.options.on_agent_bound?.(agent);
		if (message.context_token !== void 0) this.latest_token.set(message.user_id, message.context_token);
		if (this.options.send_media !== void 0) {
			const send_media = this.options.send_media;
			const latest_token = this.latest_token;
			const user_id = message.user_id;
			const media_tool = defineTool({
				name: "send_file",
				description: "把本地图片发到当前微信聊天（只支持 jpg/png/gif/webp 等图片；文件暂不支持）。用户说\"发我\"\"发过来\"\"截图给我\"时用它。",
				parameters: { path: {
					type: "string",
					description: "本地图片绝对路径"
				} },
				output: {
					schema: {
						type: "object",
						additionalProperties: false,
						properties: {
							ok: {
								type: "boolean",
								required: true
							},
							text: {
								type: "string",
								required: true
							}
						}
					},
					render: (_args, value) => [{
						type: "text",
						text: value.text
					}]
				},
				async execute(args) {
					const file = String(args.path ?? "").trim();
					if (file === "") return {
						ok: false,
						text: "path 不能为空"
					};
					try {
						await send_media(user_id, file, latest_token.get(user_id));
						return {
							ok: true,
							text: `已发送：${file}`
						};
					} catch (error) {
						return {
							ok: false,
							text: `发送失败：${String(error).slice(0, 160)}`
						};
					}
				}
			});
			const current = agent.ctx.tools.get("send_file", agent);
			if (current === void 0) agent.ctx.tools.register(media_tool);
			else {
				current.description = media_tool.description;
				current.parameters = media_tool.parameters;
				current.execute = media_tool.execute;
			}
		}
		this.options.on_user_message?.(session_key, message.user_id, text);
		const content = await this.build_content(text, media);
		if (content.length === 0) content.push({
			type: "text",
			text: "[微信空消息]"
		});
		const incoming = createUserMessage({
			content,
			source: {
				kind: "plugin",
				plugin: "dsh-webhook-weixin",
				form: "notice",
				summary: "微信消息"
			}
		});
		if (this.disposed) return;
		if (agent.status !== "idle") {
			agent.steer(incoming);
			probe("inbound", "message.steer", {
				delivery_id: message.delivery_id,
				session_id: session_key
			});
		} else agent.followup(incoming);
		this.delivery_sessions.set(message.delivery_id, session_key);
		this.options.store.update_delivery(message.delivery_id, "injected", {
			session_id: session_key,
			injected_at: Date.now()
		});
		for (const delivery_id of also_mark) {
			this.delivery_sessions.set(delivery_id, session_key);
			this.options.store.update_delivery(delivery_id, "injected", {
				session_id: session_key,
				injected_at: Date.now()
			});
		}
		done({
			outcome: "injected",
			session_id: session_key,
			blocks: content.length,
			merged: also_mark.length
		});
	}
	/** 构造消息内容块；媒体下载失败降级为说明文本，不影响文字投递。 */
	async build_content(text, media) {
		const content = [];
		if (text) content.push({
			type: "text",
			text
		});
		for (const item of media) try {
			if (item.data && item.kind === "image" && this.options.attachments) {
				const media_type = item.mime_type === "image/png" || item.mime_type === "image/webp" || item.mime_type === "image/gif" ? item.mime_type : "image/jpeg";
				const attachment = await with_timeout(this.options.attachments.saveImage({
					data: item.data,
					mediaType: media_type,
					...item.name === void 0 ? {} : { name: item.name }
				}), MEDIA_TIMEOUT_MS, "saveImage");
				content.push({
					type: "image",
					attachment
				});
			} else if (item.data && item.kind === "file" && this.options.attachments?.saveFile) {
				const attachment = await with_timeout(this.options.attachments.saveFile({
					data: item.data,
					...item.name === void 0 ? {} : { name: item.name }
				}), MEDIA_TIMEOUT_MS, "saveFile");
				content.push({
					type: "file",
					attachment
				});
			} else {
				const note = `[微信媒体 ${item.kind}${item.source_text ? `:${item.source_text}` : ""}]`;
				content.push({
					type: "text",
					text: note
				});
			}
		} catch (error) {
			const detail = error instanceof Error ? error.message : String(error);
			probe("inbound", "media.failed", {
				kind: item.kind,
				error: detail
			});
			content.push({
				type: "text",
				text: `[微信媒体 ${item.kind} 处理失败：${detail}]`
			});
		}
		return content;
	}
	/**
	* 恢复未完成的 delivery（设计文档 §5.2 的恢复协议）。
	*
	* 关键：**不能因为 delivery 已有 session_id 就把它标成 injected**。那样会掩盖这个窗口：
	* `delivery 已登记 → session_id 已写入 → agent.followup() 尚未执行 → 进程崩溃`。
	* 有 `payload_json` 的必须重新走一遍完整注入，成功才由 process_message 写 injected；
	* 失败则保持 failed_retryable，交给现有重试机制——绝不"假成功"地丢消息。
	*/
	async recover_unfinished() {
		const unfinished = this.options.store.list_unfinished_deliveries();
		let needs_reinject = 0;
		let reinjected = 0;
		for (const record of unfinished) {
			if (record.payload_json !== void 0) {
				const message = deserialize_payload(record.payload_json);
				if (message !== void 0) {
					needs_reinject += 1;
					probe("inbound", "recover.reinject", {
						delivery_id: record.delivery_id,
						user_id: record.user_id,
						from_status: record.status
					});
					await this.enqueue_for_user(message);
					reinjected += 1;
					continue;
				}
			}
			needs_reinject += 1;
			this.options.store.update_delivery(record.delivery_id, "failed_terminal", { last_error: "无法恢复：入站载荷缺失，未重新执行" });
		}
		probe("inbound", "recover.done", {
			unfinished: unfinished.length,
			needs_reinject,
			reinjected
		});
		return {
			recovered: unfinished.length,
			needs_reinject
		};
	}
	/** 该 delivery 是否可提交 cursor。 */
	is_committable(delivery_id) {
		const record = this.options.store.get_delivery(delivery_id);
		return record !== void 0 && is_cursor_committable(record.status);
	}
};
/** 给 promise 加超时，避免单个媒体处理拖住整个用户分片。 */
async function with_timeout(promise, ms, label) {
	let timer;
	try {
		return await Promise.race([promise, new Promise((_resolve, reject) => {
			timer = setTimeout(() => reject(/* @__PURE__ */ new Error(`${label} 超时（${ms}ms）`)), ms);
			timer.unref?.();
		})]);
	} finally {
		if (timer !== void 0) clearTimeout(timer);
	}
}
//#endregion
//#region lib/types/coordination/outboundCoordinator.js
/**
* 出站协调器：微信链路唯一的发送队列。
*
* 重构设计文档 §7：所有对外消息（正式回复、控制回执、进度、心跳、错误）都进入同一条
* durable outbox，按 `session_id + sequence` 有序发送；发送失败留下可见状态并可恢复。
*
* 心跳策略（按用户要求调整）：**思考摘要本身即心跳**。只有当"距上次输出思考摘要的时间"
* 超过阈值时，才补发一条心跳；不再固定间隔刷屏。
* @module dsh-webhook-weixin/outbound-coordinator
*/
/**
* 单条消息的最大字符数；超过就拆成多条依次发。
*
* iLink 对超长文本会直接判 `prepare failed`（整条发不出去），
* 之前链路里还额外做了 300/200 字的截断——用户看到的"截断"就是这个。
* 现在改为**分段发送**：内容完整送达，且每段都不超限。
*/
const MAX_TEXT_CHARS = Number(process.env.DSH_WEIXIN_MAX_TEXT_CHARS ?? "1000");
/**
* 把长文本按上限拆成多段；优先在换行处切，避免把一句话/一行命令拆断。
* @param text - 原始文本。
* @param limit - 每段最大字符数。
*/
function split_text(text, limit) {
	if (text.length <= limit) return [text];
	const parts = [];
	let rest = text;
	while (rest.length > limit) {
		const window = rest.slice(0, limit);
		const cut = Math.max(window.lastIndexOf("\n"), window.lastIndexOf("。"), window.lastIndexOf(" "));
		const at = cut > limit * .5 ? cut + 1 : limit;
		parts.push(rest.slice(0, at));
		rest = rest.slice(at);
	}
	if (rest.length > 0) parts.push(rest);
	return parts;
}
/** 微信出站协调器。 */
var OutboundCoordinator = class {
	store;
	turn_context;
	sender;
	classify_retryable;
	/** 把"最终回复"记进长期记忆；由连接器注入，失败也不影响发送。 */
	record_entry;
	max_attempts;
	retry_base_ms;
	heartbeat_idle_ms;
	heartbeat_max_per_turn;
	send_min_gap_ms;
	send_batch_limit;
	/** 每个会话上次发送的时间，用于节流。 */
	last_send_at = /* @__PURE__ */ new Map();
	/** 全局上次发送时间：不同会话也压制一下总速率。 */
	last_send_any_at = 0;
	progress = /* @__PURE__ */ new Map();
	draining = false;
	stopped = false;
	/** 限流熔断：在此时间戳之前整条链路暂停发送（见 options.rate_limit_cooldown_ms）。 */
	paused_until = 0;
	/** 熔断期间唯一的唤醒定时器；避免并发 drain 累积出定时器/日志风暴。 */
	pause_timer;
	/** 上次打印 drain.paused 的时间；熔断期间按秒级降噪。 */
	pause_logged_at = 0;
	/** 连续限流次数，用于指数加大冷却；发送成功即清零。 */
	rate_limit_streak = 0;
	rate_limit_cooldown_ms;
	rate_limit_cooldown_max_ms;
	/** 无 context_token 的消息被拒后，推迟多久再试（默认 5 分钟）。 */
	no_token_defer_ms;
	/** 通知类结果停放多久（默认 6 小时）。 */
	parked_retention_ms;
	/** 绑定状态库与发送实现。 */
	constructor(options) {
		this.store = options.store;
		this.turn_context = options.turn_context;
		this.sender = options.sender;
		this.classify_retryable = options.classify_retryable;
		this.record_entry = options.record_entry;
		this.max_attempts = options.max_attempts ?? 5;
		this.retry_base_ms = options.retry_base_ms ?? 500;
		this.heartbeat_idle_ms = options.heartbeat_idle_ms ?? Number(process.env.DSH_WEIXIN_HEARTBEAT_IDLE_MS ?? "300000");
		this.heartbeat_max_per_turn = options.heartbeat_max_per_turn ?? 6;
		this.send_min_gap_ms = options.send_min_gap_ms ?? Number(process.env.DSH_WEIXIN_SEND_MIN_GAP_MS ?? "2200");
		this.send_batch_limit = options.send_batch_limit ?? Number(process.env.DSH_WEIXIN_SEND_BATCH_LIMIT ?? "3");
		this.rate_limit_cooldown_ms = options.rate_limit_cooldown_ms ?? Number(process.env.DSH_WEIXIN_RATE_LIMIT_COOLDOWN_MS ?? "60000");
		this.rate_limit_cooldown_max_ms = options.rate_limit_cooldown_max_ms ?? Number(process.env.DSH_WEIXIN_RATE_LIMIT_COOLDOWN_MAX_MS ?? "600000");
		this.no_token_defer_ms = options.no_token_defer_ms ?? Number(process.env.DSH_WEIXIN_NO_TOKEN_DEFER_MS ?? "300000");
		this.parked_retention_ms = options.parked_retention_ms ?? Number(process.env.DSH_WEIXIN_PARKED_RETENTION_MS ?? "21600000");
	}
	/** 入队一条正式回复（转级上下文由 turn 决定）。 */
	enqueue_assistant(input) {
		if (!input.text.trim()) return;
		const context = this.turn_context.resolve(input.session_id, input.turn);
		const record = this.store.enqueue_outbox({
			session_id: input.session_id,
			user_id: context?.user_id ?? input.user_id,
			kind: "assistant",
			text: input.text,
			turn: input.turn,
			...context?.context_token === void 0 ? {} : { context_token: context.context_token }
		});
		ObservationRecorder.bind_outbound(record.outbound_id, input.session_id, input.turn);
		this.record_entry?.({
			id: record.outbound_id,
			ts: record.created_at,
			role: "assistant",
			text: input.text,
			user_id: record.user_id,
			session_id: record.session_id,
			turn: input.turn
		});
	}
	/** 入队控制回执 / 错误 / 进度。 */
	enqueue_notice(input) {
		if (!input.text.trim()) return;
		if (input.turn === void 0) this.store.trim_pending_notices(input.session_id);
		const fallback_token = input.context_token ?? this.turn_context.latest_token_if_fresh(input.session_id);
		this.store.enqueue_outbox({
			session_id: input.session_id,
			user_id: input.user_id,
			kind: input.kind,
			text: input.text,
			...input.turn === void 0 ? {} : { turn: input.turn },
			...fallback_token === void 0 ? {} : { context_token: fallback_token }
		});
	}
	/**
	* 泵出所有会话的可发送消息。
	* - 不同 Session 并发；同一 Session 严格按 sequence。
	* - 单条失败不阻塞其它会话。
	*/
	async drain() {
		if (this.stopped) return;
		const wait = this.paused_until - Date.now();
		if (wait > 0) {
			const now = Date.now();
			if (now - this.pause_logged_at > 5e3) {
				this.pause_logged_at = now;
				probe("outbound", "drain.paused", {
					remaining_ms: wait,
					streak: this.rate_limit_streak
				});
			}
			if (this.pause_timer === void 0) {
				this.pause_timer = setTimeout(() => {
					this.pause_timer = void 0;
					this.drain();
				}, wait + 50);
				this.pause_timer.unref?.();
			}
			return;
		}
		if (this.draining) return;
		this.draining = true;
		try {
			const heads = this.store.next_outbox_per_session();
			await Promise.all(heads.map((head) => this.send_head(head)));
		} finally {
			this.draining = false;
		}
	}
	/**
	* 发送某会话当前队首；成功后继续推进该会话。
	*
	* 关键防护：记录本次已处理过的 outbound_id，禁止在同一次泵出里重复发送同一条。
	* 否则"终态失败 → 入队 error → error 也失败 → 再入队 error"会变成无限循环。
	*/
	async send_head(record) {
		const handled = /* @__PURE__ */ new Set();
		let current = record;
		let sent = 0;
		while (current !== void 0 && !this.stopped) {
			if (handled.has(current.outbound_id)) {
				probe("outbound", "pump.guard_tripped", { outbound_id: current.outbound_id });
				return;
			}
			handled.add(current.outbound_id);
			await this.throttle(current.session_id);
			if (!await this.send_one(current)) return;
			sent += 1;
			if (sent >= this.send_batch_limit) {
				probe("outbound", "pump.batch_capped", {
					session_id: current.session_id,
					sent
				});
				return;
			}
			current = this.store.next_outbox(current.session_id);
		}
	}
	/** 发送前等待，保证最小间隔（会话级 + 全局级）。 */
	async throttle(session_id) {
		const now = Date.now();
		const last_same = this.last_send_at.get(session_id) ?? 0;
		const wait_same = this.send_min_gap_ms - (now - last_same);
		const wait_any = Math.floor(this.send_min_gap_ms / 2) - (now - this.last_send_any_at);
		const wait = Math.max(0, wait_same, wait_any);
		if (wait > 0) {
			probe("outbound", "throttle.wait", {
				session_id,
				wait_ms: wait
			});
			await new Promise((resolve) => setTimeout(resolve, wait));
		}
		this.last_send_at.set(session_id, Date.now());
		this.last_send_any_at = Date.now();
	}
	/** 发送单条；返回是否应继续推进该会话队列。 */
	async send_one(record) {
		const done = probe_timer("outbound", "send.attempt", {
			outbound_id: record.outbound_id,
			session_id: record.session_id,
			kind: record.kind,
			sequence: record.sequence,
			attempt: record.attempt,
			turn: record.turn
		});
		this.store.mark_outbox_sending(record.outbound_id);
		try {
			const send_token = record.context_token ?? this.turn_context.latest_token_if_fresh(record.session_id);
			if (send_token !== void 0 && record.context_token === void 0) probe("outbound", "token.picked_at_send", {
				outbound_id: record.outbound_id,
				kind: record.kind
			});
			const parts = split_text(record.text, MAX_TEXT_CHARS);
			const already_sent = new Set(record.sent_parts ?? []);
			let sent_any = already_sent.size > 0;
			for (let index = 0; index < parts.length; index += 1) {
				if (already_sent.has(index)) continue;
				if (sent_any) await this.throttle(record.session_id);
				await this.sender({
					user_id: record.user_id,
					text: parts[index],
					client_id: `${record.client_id}#${index}`,
					...send_token === void 0 ? {} : { context_token: send_token }
				});
				this.store.mark_outbox_part_sent(record.outbound_id, index);
				sent_any = true;
			}
			this.store.mark_outbox_sent(record.outbound_id);
			this.paused_until = 0;
			this.rate_limit_streak = 0;
			done({ outcome: "sent" });
			return true;
		} catch (error) {
			const detail = error instanceof Error ? error.message : String(error);
			const retryable = this.classify_retryable(error);
			const rate_limited = error?.rate_limited === true;
			const attempt = record.attempt + 1;
			if (rate_limited) {
				if (record.turn === void 0) {
					const park_ms = this.parked_retention_ms;
					this.store.mark_outbox_failed(record.outbound_id, "failed_retryable", "会话窗口已关闭：已停放，等用户询问时返回", Date.now() + park_ms);
					this.store.prune_parked_results(park_ms);
					done({
						outcome: "notice_parked",
						error: detail,
						attempt,
						park_ms
					});
					probe("outbound", "notice.parked", {
						outbound_id: record.outbound_id,
						park_ms,
						has_token: record.context_token !== void 0
					});
					return true;
				}
				if (record.context_token === void 0) {
					const defer_ms = this.no_token_defer_ms;
					this.store.mark_outbox_failed(record.outbound_id, "failed_retryable", detail, Date.now() + defer_ms);
					done({
						outcome: "deferred_no_token",
						error: detail,
						attempt,
						defer_ms
					});
					probe("outbound", "send.deferred_no_token", {
						outbound_id: record.outbound_id,
						kind: record.kind,
						defer_ms
					});
					return true;
				}
				this.rate_limit_streak += 1;
				const cooldown = Math.min(this.rate_limit_cooldown_ms * 2 ** (this.rate_limit_streak - 1), this.rate_limit_cooldown_max_ms);
				this.paused_until = Date.now() + cooldown;
				this.store.mark_outbox_failed(record.outbound_id, "failed_retryable", detail, Date.now() + cooldown);
				done({
					outcome: "rate_limited",
					error: detail,
					attempt,
					cooldown_ms: cooldown,
					streak: this.rate_limit_streak
				});
				return false;
			}
			if (!retryable || attempt >= this.max_attempts) {
				this.store.mark_outbox_failed(record.outbound_id, "failed_terminal", detail);
				done({
					outcome: "failed_terminal",
					error: detail,
					attempt
				});
				probe("outbound", "send.gave_up", {
					outbound_id: record.outbound_id,
					kind: record.kind,
					attempt,
					error: detail
				});
				return true;
			}
			const base = this.retry_base_ms;
			const next_retry_at = Date.now() + base * 2 ** Math.min(attempt, 6);
			this.store.mark_outbox_failed(record.outbound_id, "failed_retryable", detail, next_retry_at);
			done({
				outcome: "failed_retryable",
				error: detail,
				attempt,
				next_retry_in_ms: next_retry_at - Date.now()
			});
			return false;
		}
	}
	/** 一轮开始：记录进度状态并启动"摘要超时"检查。 */
	begin_turn(session_id, turn) {
		this.clear_progress(session_id);
		const state = {
			turn,
			last_visible_at: Date.now(),
			heartbeats: 0,
			active: true
		};
		if (this.heartbeat_idle_ms > 0) {
			state.timer = setInterval(() => {
				this.maybe_heartbeat(session_id);
			}, Math.max(5e3, Math.floor(this.heartbeat_idle_ms / 3)));
			state.timer.unref?.();
		}
		this.progress.set(session_id, state);
		probe("outbound", "turn.begin", {
			session_id,
			turn,
			heartbeat_idle_ms: this.heartbeat_idle_ms
		});
	}
	/**
	* 标记"刚有可见输出"。
	*
	* 思考摘要与正文分段都会调用这里——**摘要本身就是心跳**，
	* 所以它同时把 last_visible_at 推后，从而抑制额外的固定心跳。
	*/
	note_visible(session_id) {
		const state = this.progress.get(session_id);
		if (state !== void 0) state.last_visible_at = Date.now();
	}
	/** 一轮结束：停止检查。 */
	end_turn(session_id) {
		this.clear_progress(session_id);
		probe("outbound", "turn.end", { session_id });
	}
	/** 距上次可见输出超过阈值才补发心跳。 */
	async maybe_heartbeat(session_id) {
		const state = this.progress.get(session_id);
		if (state === void 0 || !state.active) return;
		const idle = Date.now() - state.last_visible_at;
		if (idle < this.heartbeat_idle_ms) return;
		if (state.heartbeats >= this.heartbeat_max_per_turn) {
			this.clear_progress(session_id);
			return;
		}
		const context = this.turn_context.resolve(session_id, state.turn);
		if (context === void 0) return;
		state.heartbeats += 1;
		state.last_visible_at = Date.now();
		this.store.enqueue_outbox({
			session_id,
			user_id: context.user_id,
			kind: "heartbeat",
			text: `（还在处理中，已等待约 ${Math.round(idle / 1e3)} 秒…）`,
			turn: state.turn,
			...context.context_token === void 0 ? {} : { context_token: context.context_token }
		});
		probe("outbound", "heartbeat.enqueued", {
			session_id,
			turn: state.turn,
			idle_ms: idle,
			count: state.heartbeats
		});
		await this.drain();
	}
	/** 入队一条思考摘要（进度）：它既是进度也是心跳。 */
	enqueue_progress(session_id, text) {
		const state = this.progress.get(session_id);
		if (state === void 0) return;
		const context = this.turn_context.resolve(session_id, state.turn);
		if (context === void 0) return;
		this.note_visible(session_id);
		this.store.enqueue_outbox({
			session_id,
			user_id: context.user_id,
			kind: "progress",
			text,
			turn: state.turn,
			...context.context_token === void 0 ? {} : { context_token: context.context_token }
		});
	}
	/** 清理某会话的进度状态。 */
	clear_progress(session_id) {
		const state = this.progress.get(session_id);
		if (state?.timer !== void 0) clearInterval(state.timer);
		this.progress.delete(session_id);
	}
	/** 停止出站泵；幂等。 */
	stop() {
		this.stopped = true;
		if (this.pause_timer !== void 0) {
			clearTimeout(this.pause_timer);
			this.pause_timer = void 0;
		}
		for (const session_id of [...this.progress.keys()]) this.clear_progress(session_id);
		probe("outbound", "stopped");
	}
	/** 启动时恢复 pending outbox（设计文档 §10.3）。 */
	async recover_pending() {
		const pending = this.store.list_pending_outbox();
		const now = Date.now();
		const stale_ms = Number(process.env.DSH_WEIXIN_OUTBOX_STALE_MS ?? String(10 * 6e4));
		const reply_stale_ms = Number(process.env.DSH_WEIXIN_REPLY_STALE_MS ?? String(30 * 6e4));
		let dropped = 0;
		for (const record of pending) {
			const limit = record.turn === void 0 ? Math.max(stale_ms, this.parked_retention_ms) : Math.max(stale_ms, reply_stale_ms);
			if (now - record.created_at > limit) {
				this.store.mark_outbox_failed(record.outbound_id, "failed_terminal", "重启恢复：超过补发时限，已丢弃");
				dropped += 1;
				continue;
			}
			if (record.status === "sending") this.store.mark_outbox_failed(record.outbound_id, "failed_retryable", "重启恢复：上次发送中断");
		}
		probe("outbound", "recover.pending", {
			count: pending.length,
			dropped_stale: dropped
		});
		return pending.length;
	}
};
//#endregion
//#region lib/types/lifecycle/consumerLease.js
/**
* 微信消费者单实例 lease + OpenClaw 隔离 preflight。
*
* 重构设计文档 §9.2：
* - lease 必须是**原子**获取（旧的"读文件-查 PID-写文件"存在竞态）；
* - 获取失败必须让连接器**启动失败**，不能只打印日志继续；
* - OpenClaw 不得同时拥有同一微信账号，启动前必须 preflight 检查。
*
* 本实现用状态库里的 `consumer_lease` 表做主（原子事务）；同时写一个人类可读的标记文件便于运维排查。
* @module dsh-webhook-weixin/consumer-lease
*/
/** lease 判定为过期的时间（毫秒）；超时未续租即可被抢占。 */
const DEFAULT_STALE_MS = 9e4;
/** 续租间隔（毫秒）；应显著小于 stale。 */
const DEFAULT_HEARTBEAT_MS = 3e4;
/** 原子 lease 的持有者句柄。 */
var ConsumerLease = class {
	owner_id;
	store;
	account_id;
	marker_path;
	stale_ms;
	heartbeat_ms;
	timer;
	released = false;
	/** 创建持有者（尚未获取）。 */
	constructor(options) {
		this.store = options.store;
		this.account_id = options.account_id;
		this.marker_path = options.marker_path;
		this.stale_ms = options.stale_ms ?? DEFAULT_STALE_MS;
		this.heartbeat_ms = options.heartbeat_ms ?? DEFAULT_HEARTBEAT_MS;
		this.owner_id = `${process.pid}-${Math.random().toString(36).slice(2, 10)}`;
	}
	/**
	* 尝试获取 lease；失败抛出（调用方必须让连接器启动失败）。
	* @throws 已有活跃消费者时抛出，错误信息包含持有者 pid/账号，便于直接定位。
	*/
	acquire() {
		if (!this.store.try_acquire_lease({
			owner_id: this.owner_id,
			pid: process.pid,
			process_started_at: Date.now() - Math.round(process.uptime() * 1e3),
			account_id: this.account_id,
			stale_ms: this.stale_ms
		})) {
			const existing = this.store.get_lease();
			const detail = existing === void 0 ? "（未知持有者）" : `pid=${existing.pid} owner=${existing.owner_id} account=${existing.account_id} 心跳于 ${Math.round((Date.now() - existing.heartbeat_at) / 1e3)} 秒前`;
			probe("lifecycle", "lease.acquire_failed", { detail });
			throw new Error(`webhook-weixin: 已有另一个微信消费者在运行，拒绝启动 —— ${detail}。同一微信账号只允许一个消费者，否则会重复处理消息、分裂会话。请先停掉它。`);
		}
		this.write_marker();
		this.timer = setInterval(() => {
			if (!this.store.heartbeat_lease(this.owner_id)) {
				probe("lifecycle", "lease.heartbeat_lost", { owner_id: this.owner_id });
				console.error("[webhook-weixin] lease 已丢失（可能被抢占），停止续租");
				this.stop_heartbeat();
			}
		}, this.heartbeat_ms);
		this.timer.unref?.();
		probe("lifecycle", "lease.held", {
			owner_id: this.owner_id,
			account_id: this.account_id
		});
	}
	/** 是否仍持有 lease（心跳丢失后为 false）。 */
	is_held() {
		return !this.released && this.store.get_lease()?.owner_id === this.owner_id;
	}
	/** 释放 lease；幂等。 */
	release() {
		if (this.released) return;
		this.released = true;
		this.stop_heartbeat();
		this.store.release_lease(this.owner_id);
		if (this.marker_path !== void 0) try {
			writeFileSync(this.marker_path, "", "utf8");
		} catch {}
	}
	/** 停止续租定时器。 */
	stop_heartbeat() {
		if (this.timer !== void 0) {
			clearInterval(this.timer);
			this.timer = void 0;
		}
	}
	/** 写人类可读标记；仅诊断用途。 */
	write_marker() {
		if (this.marker_path === void 0) return;
		try {
			mkdirSync(dirname(this.marker_path), { recursive: true });
			writeFileSync(this.marker_path, `owner=${this.owner_id} pid=${process.pid} account=${this.account_id}\n`, "utf8");
		} catch {}
	}
};
/**
* OpenClaw 隔离 preflight（设计文档 §9.2）。
*
* 只做只读检查：OpenClaw 的微信通道若处于启用状态，说明同一账号存在第二个消费者。
* 检查项：
* 1. `~/.openclaw/openclaw.json` 的 `plugins.entries.openclaw-weixin.enabled` 必须为 false；
* 2. 没有正在运行的 OpenClaw gateway 进程持有微信通道。
*
* @param openclawConfigPath - 配置文件路径；默认 ~/.openclaw/openclaw.json。
*/
function preflight_openclaw_isolation(openclawConfigPath) {
	const problems = [];
	const path = openclawConfigPath ?? join(process.env.USERPROFILE ?? process.env.HOME ?? ".", ".openclaw", "openclaw.json");
	try {
		const raw = readFileSync(path, "utf8");
		if ((JSON.parse(raw).plugins?.entries?.["openclaw-weixin"])?.enabled === true) problems.push(`OpenClaw 的 openclaw-weixin 仍为 enabled: true（${path}）`);
	} catch (error) {
		if (error.code !== "ENOENT") problems.push(`OpenClaw 配置无法解析（${path}）：${error instanceof Error ? error.message : String(error)}`);
	}
	problems.push(...scan_openclaw_gateway_processes());
	const result = {
		ok: problems.length === 0,
		problems
	};
	probe("lifecycle", "preflight.openclaw", {
		ok: result.ok,
		problems
	});
	return result;
}
/**
* 扫描正在运行的 OpenClaw 微信 gateway 进程（Windows）。
*
* 只读配置文件是不够的：进程可能正持有同一微信通道，此时两个消费者会互相抢消息
* （表现为回复丢一半、或收到别人的回复）。因此必须真的查进程。
*
* 判定：命令行同时包含 openclaw + gateway + (weixin|channel)。
* 查询本身失败时 **fail-closed**（报问题），不能因为查不到就当作"没有 OpenClaw"。
*/
function scan_openclaw_gateway_processes() {
	if (process.platform !== "win32") return [];
	const script = "Get-CimInstance Win32_Process | Select-Object ProcessId,CommandLine | ConvertTo-Json -Compress";
	try {
		const stdout = execFileSync("powershell.exe", [
			"-NoLogo",
			"-NonInteractive",
			"-ExecutionPolicy",
			"Bypass",
			"-Command",
			script
		], {
			encoding: "utf8",
			timeout: 2e4,
			windowsHide: true
		});
		const parsed = JSON.parse(stdout.trim() === "" ? "[]" : stdout);
		const rows = Array.isArray(parsed) ? parsed : [parsed];
		const found = [];
		for (const row of rows) {
			const command = (row.CommandLine ?? "").toLowerCase();
			if (!command.includes("openclaw")) continue;
			if (!command.includes("gateway")) continue;
			if (!command.includes("weixin") && !command.includes("channel")) continue;
			found.push(`检测到正在运行的 OpenClaw 微信 gateway（pid=${row.ProcessId ?? "?"}）`);
		}
		return found;
	} catch (error) {
		return ["无法确认 OpenClaw gateway 是否在运行（进程扫描失败，按 fail-closed 处理）：" + (error instanceof Error ? error.message : String(error))];
	}
}
//#endregion
//#region lib/types/reply/replyAggregator.js
/**
* 回复聚合器：只负责"把一个 turn 的流式文本切成可发送段落"，不做任何发送。
*
* 重构设计文档 §7.2：保留文本分段与代码围栏处理，删除独立发送语义。
* 发送顺序、重试、持久化全部交给 OutboundCoordinator。
*
* 同时持有本轮累计的思考摘要（reasoning），供"摘要即心跳"的进度推送使用。
* @module dsh-webhook-weixin/reply-aggregator
*/
/**
* 触发分段的句子标点。
*
* 只按**句末标点**切分，不再按逗号/分号切：
* iLink 对突发发送会限流（实测连发第 4 条起全部 `prepare failed`），
* 而中文逗号在正常回复里极密集，会把一条回复切成十几条独立消息。
* ASCII 句点同样不切（避免拆坏版本号/文件名）。
*/
const SENTENCE_BOUNDARIES = new Set([
	"。",
	"！",
	"!",
	"？",
	"?"
]);
/** 分段后保留的标点。 */
const KEPT = new Set([
	"！",
	"!",
	"？",
	"?"
]);
/**
* 一段至少要有这么多字符才值得单独发一条。
* 太短的（如"好呀。"）会与后续内容合并，避免把回复打碎成很多条。
*/
const MIN_SEGMENT_CHARS = 40;
/**
* 单轮最多切成几条。超出后不再切分，剩余内容并入尾段一起发。
* 这是对"限流"的硬保护：一轮回复最多产生这么多条出站消息。
*/
const MAX_SEGMENTS_PER_TURN = 4;
/** 模型泄漏到正文里的思考标签；回传前剥掉。 */
const THINK_TAG = /<\/?think[^<>]*>/gi;
/** 被分片切断、还没等到 `>` 的思考标签，先攒着。 */
const THINK_TAG_TAIL = /<\/?think[^<>]*$/i;
/**
* 识别"工具调用外壳"文本（模型把工具调用当普通文本输出）。
* 不能见到 `{`/`[` 就吞——正常 JSON 回答会被误吞，所以要求同时出现工具特征键。
*/
function looks_like_tool_call(text) {
	const trimmed = text.trimStart();
	if (/FunctionCall(Begin|End)?/.test(trimmed)) return true;
	if (!trimmed.startsWith("{") && !trimmed.startsWith("[")) return false;
	return /"(name|arguments|parameters|tool_call|tool_calls|function)"\s*:/.test(trimmed);
}
/** 单轮回复的文本聚合器。 */
var ReplyAggregator = class {
	pending = "";
	reasoning = "";
	suppressed = false;
	finished = false;
	/** 本轮已经切出去的段数（用于 MAX_SEGMENTS_PER_TURN 限流保护）。 */
	emitted = 0;
	/**
	* 接收一个正文分片，返回应立刻发送的段落列表。
	* 命中"工具调用外壳"时整轮静默，避免把内部结构泄露给用户。
	*/
	accept_chunk(text) {
		if (this.finished || !text) return [];
		if (this.suppressed || looks_like_tool_call(this.pending + text)) {
			this.suppressed = true;
			this.pending = "";
			return [];
		}
		this.pending = (this.pending + text).replace(THINK_TAG, "");
		if (THINK_TAG_TAIL.test(this.pending)) return [];
		if (this.emitted >= MAX_SEGMENTS_PER_TURN) return [];
		return this.extract();
	}
	/** 累计思考摘要（不发送）。 */
	accept_reasoning(text) {
		this.reasoning = (this.reasoning + text).slice(-4e3);
	}
	/**
	* 取出一段思考摘要并清空。
	* 摘要推送后应调用 `mark_visible()`，因为它本身就是一次"用户可见输出"。
	*/
	take_reasoning_excerpt(limit) {
		const text = this.reasoning.replace(/\s+/g, " ").trim();
		if (!text) return void 0;
		this.reasoning = "";
		return text.length > limit ? "…" + text.slice(-limit) : text;
	}
	/** 结束本轮，返回尚未发送的尾段。 */
	finish() {
		if (this.finished) return void 0;
		this.finished = true;
		if (this.suppressed) return void 0;
		const tail = this.pending.trim();
		this.pending = "";
		return tail || void 0;
	}
	/** 是否已被判定为工具调用外壳（整轮静默）。 */
	get is_suppressed() {
		return this.suppressed;
	}
	/** 按句子标点切出可发送段落。 */
	extract() {
		const segments = [];
		let start = 0;
		let index = 0;
		let in_code = false;
		while (index < this.pending.length) {
			if (this.pending.startsWith("```", index)) {
				in_code = !in_code;
				index += 3;
				continue;
			}
			if (!in_code && SENTENCE_BOUNDARIES.has(this.pending[index])) {
				let end = index + 1;
				let kept = "";
				if (KEPT.has(this.pending[index])) {
					while (end < this.pending.length && KEPT.has(this.pending[end])) end += 1;
					kept = this.pending.slice(index, end);
				}
				const segment = (this.pending.slice(start, index).trim() + kept).trim();
				if (segment.length >= MIN_SEGMENT_CHARS && this.emitted < MAX_SEGMENTS_PER_TURN) {
					segments.push(segment);
					this.emitted += 1;
					start = end;
					index = end;
					continue;
				}
				index = end;
				continue;
			}
			index += 1;
		}
		this.pending = this.pending.slice(start);
		return segments;
	}
};
//#endregion
//#region lib/types/session/weixinSessionHost.js
/**
* Harness Agent 宿主接口 + 单一所有权实现。
*
* 重构设计文档 §8.1：必须选定一种所有权模型，禁止"连接器 handles"与"Harness owner effect"
* 同时认为自己是最终 owner。此处采用 **gateway 统一持有** 模型：
* - 本类是唯一保存 `AgentHandle` 的地方；
* - Cordis teardown 只调用本类的 `dispose_agents()` 一次；
* - `dispose` 在 stop / 插件卸载 / 重复 stop / 创建中止四种情况下都幂等。
*
* 同时 §8.2 要求：preset / agent-loop / sessionPersistence 缺失必须在**连接器启动时**失败，
* 而不是第一条消息到达时才失败（由 `assert_capabilities()` 完成）。
* @module dsh-webhook-weixin/session-host
*/
/** 唯一的 Agent 所有权实现。 */
var HarnessSessionHost = class {
	ctx;
	options;
	handles = /* @__PURE__ */ new Map();
	disposed = false;
	composition;
	/** 绑定 Cordis 上下文与组合参数。 */
	constructor(ctx, options) {
		this.ctx = ctx;
		this.options = options;
	}
	/**
	* 启动时装配检查（设计文档 §8.2 / §9.3）：缺任何一个必需服务都立即失败。
	* @throws 缺少 agents / agent-loop / agentPresets / sessionPersistence 时抛出。
	*/
	assert_capabilities() {
		const get = (name) => this.ctx.get(name);
		const missing = [];
		if (get("agents") === void 0) missing.push("agents");
		if (get("agentPresets") === void 0) missing.push("agentPresets");
		if (get("sessionPersistence") === void 0) missing.push("sessionPersistence");
		if (get("llm") === void 0) missing.push("llm");
		if (this.options.agent_preset !== void 0 && get("agentPresets") !== void 0) probe("lifecycle", "capability.preset_requested", { preset: this.options.agent_preset });
		if (missing.length > 0) throw new Error(`webhook-weixin: 装配检查失败，缺少服务 ${missing.join(", ")}。连接器拒绝启动（避免出现"能启动但发消息才发现没有工具/没有持久化"的假成功）。`);
		probe("lifecycle", "capability.ok", {
			preset: this.options.agent_preset ?? "(default)",
			provider: this.options.provider,
			model: this.options.model
		});
	}
	/**
	* 忘掉已解析的 preset 组合（挂载/卸载工具后调用）。
	*
	* `compose()` 是**按宿主实例记忆**的：不重置的话，改完 preset 之后
	* 新会话仍然拿到旧工具集，用户会以为"勾选了没生效"。
	* 重置后下一次创建 Agent 会重新解析 preset（已有 Agent 不受影响，
	* 需要新会话或清空上下文才会换上新工具）。
	*/
	reset_composition() {
		this.composition = void 0;
		probe("lifecycle", "composition.reset");
	}
	/** 查找在线 Agent。 */
	get_agent(session_id) {
		return this.ctx.agents.get(session_id);
	}
	/** 创建 Agent；Session 已存在时自动恢复。 */
	async create_agent(session_id) {
		this.assert_not_disposed();
		const composition = await this.compose();
		try {
			const handle = await this.ctx.agents.create({
				sessionId: session_id,
				meta: {
					cwd: this.options.workspace_path,
					...composition.agentPreset === void 0 ? {} : { agentPreset: composition.agentPreset },
					...this.options.provider === void 0 ? {} : { provider: this.options.provider },
					...this.options.model === void 0 ? {} : { model: this.options.model }
				},
				...this.agent_options() === void 0 ? {} : { agentOptions: this.agent_options() },
				...composition.setup === void 0 ? {} : { setup: composition.setup }
			});
			this.handles.set(String(handle.agent.id), handle);
			probe("lifecycle", "agent.created", { session_id: String(handle.agent.id) });
			return handle.agent;
		} catch (error) {
			if (!is_session_already_exists(error)) throw error;
			probe("lifecycle", "agent.create_conflict_resume", { session_id: String(session_id) });
			return this.resume_agent(session_id);
		}
	}
	/** 恢复持久化 Session。 */
	async resume_agent(session_id) {
		this.assert_not_disposed();
		const composition = await this.compose();
		const handle = await this.ctx.agents.resume({
			resumeSessionId: session_id,
			...this.agent_options() === void 0 ? {} : { agentOptions: this.agent_options() },
			...composition.setup === void 0 ? {} : { setup: composition.setup }
		});
		this.handles.set(String(handle.agent.id), handle);
		probe("lifecycle", "agent.resumed", { session_id: String(handle.agent.id) });
		return handle.agent;
	}
	/**
	* 释放**单个** Agent（清空某个用户的上下文用），返回是否真的释放了。
	*
	* 必须是 owner 来释放：只有 dispose 才会关掉会话日志的写句柄。
	* 否则外部删掉 session 文件后，进程内残留的句柄会在下次 append 时
	* **新建一个没有 header 的坏文件**（实测的坑），那个会话就再也恢复不起来了。
	*/
	async dispose_agent(session_id) {
		const key = String(session_id);
		const handle = this.handles.get(key);
		if (handle === void 0) return false;
		this.handles.delete(key);
		try {
			await handle.dispose();
			probe("lifecycle", "agent.disposed_one", { session_id: key });
			return true;
		} catch (error) {
			probe("lifecycle", "agent.dispose_failed", {
				session_id: key,
				error: String(error)
			});
			return false;
		}
	}
	/**
	* 释放本宿主创建的全部 Agent；**幂等**（重复调用、卸载后调用都安全）。
	* 这是唯一的 owner 释放点。
	*/
	async dispose_agents() {
		if (this.disposed) return;
		this.disposed = true;
		const entries = [...this.handles.values()];
		this.handles.clear();
		for (const handle of entries) try {
			await handle.dispose();
		} catch (error) {
			probe("lifecycle", "agent.dispose_failed", { error: String(error) });
		}
		probe("lifecycle", "agents.disposed", { count: entries.length });
	}
	/** 组装 preset（解析失败必须抛错，禁止静默降级成"无工具 Agent"）。 */
	async compose() {
		if (this.composition !== void 0) return this.composition;
		const presets = this.ctx.get("agentPresets");
		if (presets === void 0) throw new Error("webhook-weixin: agentPresets 服务缺失，无法挂载 Agent preset（会导致 Agent 没有任何工具）。");
		try {
			const resolvedId = (await presets.resolve(this.options.agent_preset)).id;
			const composition = {
				agentPreset: resolvedId,
				setup: async (agentCtx) => {
					await presets.mount(agentCtx, resolvedId);
				}
			};
			this.composition = composition;
			probe("lifecycle", "preset.resolved", {
				requested: this.options.agent_preset ?? "(default)",
				resolved: resolvedId
			});
			return composition;
		} catch (error) {
			const detail = error instanceof Error ? error.message : String(error);
			throw new Error(`webhook-weixin: 无法解析 Agent preset "${this.options.agent_preset ?? "(default)"}"，拒绝以"无工具 Agent"继续运行：${detail}`);
		}
	}
	/**
	* 组装 Agent 运行选项。
	*
	* provider/model 必须传给 `ctx.agents.create/resume`：preset 的 persona 前缀里有
	* `{{model}}` 这类提示变量，不传会导致 Agent 启动即报
	* `prompt variable "{{model}}" has no value for this assembly`，
	* 表现为"消息已注入但永远没有回复"（探针里能看到 agent.error）。
	*/
	agent_options() {
		const provider = this.options.provider;
		const model = this.options.model;
		if (provider === void 0 || model === void 0) {
			probe("lifecycle", "agent_options.missing_model", {
				provider,
				model
			});
			return;
		}
		const level = this.options.reasoning_effort;
		if (level === void 0) return {
			provider,
			model
		};
		return {
			provider,
			model,
			reasoningEffort: level
		};
	}
	/** 卸载后禁止再创建。 */
	assert_not_disposed() {
		if (this.disposed) throw new Error("webhook-weixin: 宿主已释放，不能再创建 Agent");
	}
};
/** 判断错误是否为"Session 已存在"。 */
function is_session_already_exists(error) {
	const message = error instanceof Error ? error.message : String(error);
	return /already exists|duplicate|EEXIST|session_exists/i.test(message);
}
/** 可选的调试日志写入器；仅在设置 DSH_WEIXIN_DEBUG_LOG 时生效。 */
async function trace_to_file(line) {
	const path = process.env.DSH_WEIXIN_DEBUG_LOG;
	if (!path) return;
	try {
		await appendFile(path, `${(/* @__PURE__ */ new Date()).toISOString()} [weixin] ${line}\n`, "utf8");
	} catch {}
}
//#endregion
//#region lib/types/diagnostics/injectServer.js
/**
* 本机调试注入口 + 管理口：把一条"用户消息"直接塞进真实的入站管线，
* 并提供几个**只有本进程能做**的维护动作（清空记忆、看记忆体量）。
*
* 存在的理由：微信入站只能靠扫码后的 iLink 长轮询，开发和回归测试时每验证一次
* 都要用真手机发一条微信，效率极低。这个本地端口让调用方（脚本/自动化）绕过 iLink，
* 但仍走**同一条**下游链路：
*
*   delivery 登记 → InboundCoordinator → agent loop → outbox → 微信发送
*
* 所以它验证的是真实行为，而不是一个旁路替身。
*
* 为什么清空记忆必须走这个进程：`~/.dsh/timeline`、`~/.dsh/graph` 有内存缓存、
* 会话上下文还被 agent 句柄持有（删文件前必须先 dispose）。外部进程直接删文件
* 要么留下脏缓存、要么写出坏会话文件——所以这些动作只能由持有它们的进程执行。
*
* 安全约束（三道）：
* 1. 只在显式设置 `DSH_WEIXIN_INJECT_PORT` 时启用，默认完全关闭；
* 2. 只绑定 `127.0.0.1`，不对外网暴露；
* 3. 只接受 POST，且请求体必须是 JSON。
*
* @module dsh-webhook-weixin/inject-server
*/
/** 请求体体积上限：调试用，超长直接拒绝，避免把内存和日志拖死。 */
const MAX_BODY_BYTES = 64 * 1024;
/**
* 启动注入口；返回的 Server 需要由调用方在卸载时 close。
*
* @param options - 端口与注入/管理实现。
* @returns 已开始监听的 HTTP server。
*/
function start_inject_server(options) {
	const server = createServer((request, response) => {
		const reply = (status, payload) => {
			const body = JSON.stringify(payload);
			response.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
			response.end(body);
		};
		const path = (request.url ?? "").split("?")[0] ?? "";
		const is_inject = path === "/inject";
		const admin_match = /^\/admin\/([a-z][a-z0-9-]*)$/.exec(path);
		if (request.method !== "POST" || !is_inject && admin_match === null) {
			reply(404, {
				ok: false,
				error: "only POST /inject and POST /admin/<action> are supported"
			});
			return;
		}
		if (admin_match !== null && options.admin === void 0) {
			reply(501, {
				ok: false,
				error: "admin actions are not wired in this build"
			});
			return;
		}
		const chunks = [];
		let size = 0;
		request.on("data", (chunk) => {
			size += chunk.length;
			if (size > MAX_BODY_BYTES) {
				reply(413, {
					ok: false,
					error: "body too large"
				});
				request.destroy();
				return;
			}
			chunks.push(chunk);
		});
		request.on("end", () => {
			let parsed;
			try {
				const text = Buffer.concat(chunks).toString("utf8");
				parsed = text.trim() === "" ? {} : JSON.parse(text);
			} catch {
				reply(400, {
					ok: false,
					error: "body must be JSON"
				});
				return;
			}
			if (admin_match !== null) {
				const action = admin_match[1];
				options.admin(action, parsed).then((result) => reply(200, {
					ok: true,
					...result
				}), (error) => {
					probe("lifecycle", "admin.failed", {
						action,
						error: String(error)
					});
					reply(500, {
						ok: false,
						error: error instanceof Error ? error.message : String(error)
					});
				});
				return;
			}
			const text = typeof parsed.text === "string" ? parsed.text : "";
			if (text.trim() === "") {
				reply(400, {
					ok: false,
					error: "text is required"
				});
				return;
			}
			const input = {
				text,
				...typeof parsed.user_id === "string" ? { user_id: parsed.user_id } : {}
			};
			options.inject(input).then((result) => reply(200, {
				ok: true,
				...result
			}), (error) => {
				probe("inbound", "inject.failed", { error: String(error) });
				reply(500, {
					ok: false,
					error: error instanceof Error ? error.message : String(error)
				});
			});
		});
	});
	server.listen(options.port, "127.0.0.1");
	probe("lifecycle", "inject_server.listening", {
		port: options.port,
		host: "127.0.0.1"
	});
	return server;
}
//#endregion
//#region lib/types/session/sessionFiles.js
/**
* 会话历史的落盘位置，以及"清空上下文"时的**尽力回收**。
*
* ## 为什么要自己推导路径
*
* harness 的 `sessionPersistence` 能力只有 `create/open/flush/stat/list`——
* **没有 delete/clear**（见 `packages/session/session-persistence/src/index.ts:135`）。
* 所以"清空某个用户的对话上下文"只能由两件事配合完成：
*
* 1. **换会话 id（epoch）** ← 真正保证上下文干净的一步（见 `weixinStateStore.context_epoch`）；
* 2. **顺手删掉旧会话目录** ← 回收磁盘，纯尽力而为。
*
* 第 2 步的路径规则照抄 `@deepseek-ai/dsh-session-persistence-jsonl` 的 `format.ts`
* （`encodeSegment` / `projectKey` / `sessionDir`）。因为只是"尽力回收"，
* 推导不出、文件不存在、名字对不上时**一律跳过**——绝不能因为路径算错就删到别的会话。
*
* ## 顺序要求（实测踩过的坑）
*
* 必须**先 `dispose` 掉 agent**（关掉会话日志的写句柄）再删文件。否则残留句柄会在
* 下一次 append 时用 `open(path,'a')` 新建一个**只有事件、没有 header** 的文件，
* 那个会话下次恢复直接报 corrupt。
*
* @module dsh-webhook-weixin/session-files
*/
/** 会话格式代次对应的文件名前缀（写死成 v3 之外还要兼容将来）。 */
const LOG_FILE_PATTERN = /^session\.v\d+\.jsonl(\..+)?$/;
/**
* 把一个任意字符串编码成**单个安全路径段**（与 harness 的 `encodeSegment` 等价）。
*
* 安全字符 `[A-Za-z0-9._-]` 原样保留，其余（含 `~`）编码成 `~XXXX`（大写十六进制）。
* 微信 user_id 里的 `@` 就是这样变成 `~0040` 的。
*/
function encode_segment(raw) {
	if (raw.length === 0) throw new Error("cannot encode an empty path segment");
	if (raw === ".") return "~002E";
	if (raw === "..") return "~002E~002E";
	let out = "";
	for (let index = 0; index < raw.length; index += 1) {
		const code = raw.charCodeAt(index);
		const ch = String.fromCharCode(code);
		if (ch !== "~" && /^[A-Za-z0-9._-]$/.test(ch)) out += ch;
		else out += `~${code.toString(16).toUpperCase().padStart(4, "0")}`;
	}
	return out;
}
/** 工程目录名（与 harness 的 `projectKey` 等价）：分隔符压成 `-`，外裹 `--…--`。 */
function project_key(cwd) {
	if (cwd.length === 0) throw new Error("cannot encode an empty project path");
	let readable = "";
	let separator_run = false;
	for (let index = 0; index < cwd.length; index += 1) {
		const code = cwd.charCodeAt(index);
		const ch = String.fromCharCode(code);
		if (ch === "/" || ch === "\\" || ch === ":") {
			if (!separator_run) readable += "-";
			separator_run = true;
		} else if (ch !== "~" && /^[A-Za-z0-9._-]$/.test(ch)) {
			readable += ch;
			separator_run = false;
		} else {
			readable += `~${code.toString(16).toUpperCase().padStart(4, "0")}`;
			separator_run = false;
		}
	}
	return `--${(readable.replace(/^-+/, "") || "root").slice(0, 251)}--`;
}
/** 会话根目录（`DSH_HOME/sessions`，默认 `~/.dsh/sessions`）。 */
function sessions_root(dsh_home) {
	return join(dsh_home ?? process.env.DSH_HOME ?? join(homedir(), ".dsh"), "sessions");
}
/** 某个会话独占的目录：`<root>/<projectKey(cwd)>/<encodeSegment(id)>`。 */
function session_dir_path(root, cwd, id) {
	return join(root, cwd === void 0 ? "_no-cwd" : project_key(cwd), encode_segment(id));
}
/**
* 清空一个目录的**内容**（保留目录本身）。
*
* 用于"清空时间记忆库 / 知识图谱"：目录是存储层的工作目录，删掉再建反而多一次竞态，
* 所以只清内容。全部是尽力而为——单个文件删不掉就跳过并计数，不抛错。
*/
function clear_directory_contents(dir) {
	if (!existsSync(dir)) {
		mkdirSync(dir, { recursive: true });
		return {
			files: 0,
			bytes: 0,
			dir
		};
	}
	let files = 0;
	let bytes = 0;
	for (const name of readdirSync(dir)) {
		const path = join(dir, name);
		try {
			const stat = statSync(path);
			if (stat.isFile()) {
				bytes += stat.size;
				files += 1;
			} else {
				bytes += directory_bytes(path);
				files += 1;
			}
			rmSync(path, {
				recursive: true,
				force: true,
				maxRetries: 3,
				retryDelay: 100
			});
		} catch {}
	}
	return {
		files,
		bytes,
		dir
	};
}
/** 递归求目录体积（只为汇报，best effort）。 */
function directory_bytes(dir) {
	let total = 0;
	let names;
	try {
		names = readdirSync(dir);
	} catch {
		return 0;
	}
	for (const name of names) {
		const path = join(dir, name);
		try {
			const stat = statSync(path);
			total += stat.isDirectory() ? directory_bytes(path) : stat.size;
		} catch {}
	}
	return total;
}
/**
* 读某个会话目录里日志文件的体量（不存在返回 undefined）。
*
* 用"目录 + 文件名模式"而不是写死 `session.v3.jsonl.zstd`：格式代次或压缩方式
* 变了这里也能跟上（`v4`、`.jsonl.gz` 都认）。
*/
function session_log_stats(dir) {
	if (!existsSync(dir)) return void 0;
	let bytes = 0;
	let modified_at = 0;
	let files = 0;
	for (const name of readdirSync(dir)) {
		if (!LOG_FILE_PATTERN.test(name)) continue;
		try {
			const stat = statSync(join(dir, name));
			bytes += stat.size;
			modified_at = Math.max(modified_at, stat.mtimeMs);
			files += 1;
		} catch {}
	}
	return files === 0 ? void 0 : {
		bytes,
		modified_at,
		files
	};
}
/**
* 尽力删除一个会话的落盘目录。
*
* 三重防误删：目录必须存在、必须是"会话目录"（含 `session.vN.jsonl*` 文件）、
* 且路径由 `root + cwd + id` 完整推导而来（不接外部传入的路径）。
* 任何一步不符就跳过并说明原因。
*/
function delete_session_artifacts(input) {
	let dir = "";
	try {
		dir = session_dir_path(input.root, input.cwd, input.id);
	} catch (error) {
		return {
			deleted: false,
			dir: "",
			reason: `路径推导失败：${String(error)}`
		};
	}
	if (!existsSync(dir)) return {
		deleted: false,
		dir,
		reason: "目录不存在（可能还没落盘）"
	};
	let entries;
	try {
		entries = readdirSync(dir);
	} catch (error) {
		return {
			deleted: false,
			dir,
			reason: `读目录失败：${String(error)}`
		};
	}
	if (!entries.some((name) => LOG_FILE_PATTERN.test(name))) return {
		deleted: false,
		dir,
		reason: "目录里没有会话日志文件，不像会话目录，已跳过"
	};
	try {
		rmSync(dir, {
			recursive: true,
			force: true,
			maxRetries: 3,
			retryDelay: 100
		});
		return {
			deleted: true,
			dir
		};
	} catch (error) {
		return {
			deleted: false,
			dir,
			reason: `删除失败：${String(error)}`
		};
	}
}
//#endregion
//#region lib/types/coordination/codexBridge.js
/**
* Codex 远程桥：让用户用微信直接遥控本机 Codex，并把 Codex 的输出主动推回微信。
*
* 思路沿用作者早前的 Telegram 版本桥接实践（本仓库不含那份实现）。
* 本模块把它搬到微信链路上，并复用我们已有的 durable outbox 做推送：
*   - 入站：以 `/codex` 开头的消息不走大模型，直接交给 Codex 执行；
*   - 出站：Codex 的每一步事件（思考/命令/结果）都作为消息推进 outbox，按节流发出；
*   - 会话：记住 thread_id，后续 `/codex` 默认续用同一线程（可用 `/codex new` 强制新开）；
*   - 控制：`/codex stop` 终止当前任务、`/codex status` 查看状态。
*
* 用 `codex exec --json`：它按行输出结构化事件（thread.started / item.completed / turn.completed），
* 比 app-server 的 JSON-RPC 简单得多，足够做"边跑边推"。
* @module dsh-webhook-weixin/codex-bridge
*/
/** 默认工作区（欣爱的专属工作区）。 */
/** Codex 远程任务的默认工作目录：环境变量优先，否则当前工作目录（不写死本机路径）。 */
const DEFAULT_WORKSPACE$1 = process.env.DSH_CODEX_WORKSPACE?.trim() || process.cwd();
/** 单次 Codex 任务超时（毫秒）。 */
const RUN_TIMEOUT_MS$1 = Number(process.env.DSH_CODEX_TIMEOUT_MS ?? String(30 * 6e4));
/** 解析 Codex 可执行文件。 */
function resolve_codex_bin() {
	const configured = process.env.CODEX_BIN;
	if (configured !== void 0 && configured.trim() && existsSync(configured.trim())) return configured.trim();
	const fallback = join(process.env.LOCALAPPDATA ?? "", "OpenAI", "Codex", "bin", "codex.exe");
	if (existsSync(fallback)) return fallback;
	return "codex";
}
/** 识别 `/codex` 指令；不是则返回 undefined。 */
function parse_codex_command(text) {
	const trimmed = text.trim();
	if (!trimmed.toLowerCase().startsWith("/codex")) return void 0;
	const rest = trimmed.slice(6).trim();
	if (rest === "") return {
		action: "help",
		text: ""
	};
	const [head, ...tail] = rest.split(/\s+/);
	const keyword = (head ?? "").toLowerCase();
	const body = tail.join(" ").trim();
	if (keyword === "stop" || keyword === "停止") return {
		action: "stop",
		text: body
	};
	if (keyword === "status" || keyword === "状态") return {
		action: "status",
		text: body
	};
	if (keyword === "new" || keyword === "新开") return {
		action: "new",
		text: body
	};
	if (keyword === "help" || keyword === "帮助") return {
		action: "help",
		text: body
	};
	return {
		action: "run",
		text: rest
	};
}
/** 微信远程 Codex 桥。 */
var CodexBridge = class {
	store;
	push;
	workspace;
	tasks = /* @__PURE__ */ new Map();
	seq = /* @__PURE__ */ new Map();
	/** 绑定状态库与推送实现。 */
	constructor(store, push, workspace = DEFAULT_WORKSPACE$1) {
		this.store = store;
		this.push = push;
		this.workspace = workspace;
	}
	/** 处理一条 `/codex` 指令。 */
	async handle(user_id, session_id, command, context_token) {
		if (command.action === "help") {
			this.push(user_id, session_id, [
				"这里是 Codex 远程控制，用法：",
				"- /codex <任务>     交给 Codex 执行（默认续用上次线程）",
				"- /codex new <任务> 新开一个 Codex 线程",
				"- /codex status     查看当前状态",
				"- /codex stop       终止当前任务",
				`默认工作区：${this.workspace}`
			].join("\n"), "assistant", context_token);
			return;
		}
		if (command.action === "status") {
			const task = this.tasks.get(user_id);
			const thread = this.thread_for(user_id);
			const text = task === void 0 ? `（当前没有正在运行的 Codex 任务${thread === void 0 ? "" : `；上次线程 ${thread.slice(0, 8)}`}）` : `（Codex 正在运行：第 ${task.turn} 轮，已 ${Math.round((Date.now() - task.started_at) / 1e3)} 秒，线程 ${task.thread_id?.slice(0, 8) ?? "初始化中"}）`;
			this.push(user_id, session_id, text, "assistant", context_token);
			return;
		}
		if (command.action === "stop") {
			const stopped = this.stop(user_id);
			this.push(user_id, session_id, stopped ? "（已终止当前的 Codex 任务）" : "（当前没有正在运行的 Codex 任务）", "assistant", context_token);
			return;
		}
		if (command.action === "new") this.store.set_meta("codex_thread:" + user_id, "");
		if (this.tasks.has(user_id)) {
			this.push(user_id, session_id, "（已经有一个 Codex 任务在跑，等它结束或先发 /codex stop）", "error", context_token);
			return;
		}
		if (!command.text.trim()) {
			this.push(user_id, session_id, "（/codex 后面要跟具体任务）", "error", context_token);
			return;
		}
		await this.run(user_id, session_id, command.text.trim(), context_token);
	}
	/** 终止某用户当前任务；返回是否真的终止了。 */
	stop(user_id) {
		const task = this.tasks.get(user_id);
		if (task === void 0) return false;
		probe("codex", "stop", {
			user_id,
			pid: task.child.pid
		});
		task.child.kill();
		this.tasks.delete(user_id);
		return true;
	}
	/** 当前用户是否有任务在跑。 */
	is_running(user_id) {
		return this.tasks.has(user_id);
	}
	/** 读取该用户记住的 Codex 线程 id。 */
	thread_for(user_id) {
		const value = this.store.get_meta("codex_thread:" + user_id);
		return value !== void 0 && value.length > 0 ? value : void 0;
	}
	/** 执行一次 Codex 任务，边跑边把事件推给用户，结束后发最终结果。 */
	async run(user_id, session_id, task, context_token) {
		const thread = this.thread_for(user_id);
		const bin = resolve_codex_bin();
		const args = thread === void 0 ? [
			"exec",
			"--json",
			"--skip-git-repo-check",
			"--dangerously-bypass-approvals-and-sandbox",
			"--color",
			"never",
			"-C",
			this.workspace,
			task
		] : [
			"exec",
			"resume",
			thread,
			"--json",
			"--skip-git-repo-check",
			"--dangerously-bypass-approvals-and-sandbox",
			"--color",
			"never",
			task
		];
		const turn = (this.seq.get(user_id) ?? 0) + 1;
		this.seq.set(user_id, turn);
		probe("codex", "run.start", {
			user_id,
			turn,
			resume: thread !== void 0,
			workspace: this.workspace,
			task_len: task.length
		});
		const child = spawn(bin, args, {
			cwd: this.workspace,
			windowsHide: true,
			stdio: [
				"ignore",
				"pipe",
				"pipe"
			]
		});
		const running = {
			child,
			turn,
			started_at: Date.now(),
			...thread === void 0 ? {} : { thread_id: thread }
		};
		this.tasks.set(user_id, running);
		let stdout = "";
		let stderr = "";
		child.stdout?.on("data", (data) => {
			stdout += String(data);
		});
		child.stderr?.on("data", (data) => {
			stderr += String(data);
		});
		const timer = setTimeout(() => {
			probe("codex", "run.timeout", {
				user_id,
				turn
			});
			child.kill();
		}, RUN_TIMEOUT_MS$1);
		const pushed = /* @__PURE__ */ new Set();
		let last_push_at = 0;
		child.stdout;
		await new Promise((resolve) => {
			let buffer = "";
			const flush_lines = () => {
				const lines = buffer.split(/\r?\n/);
				buffer = lines.pop() ?? "";
				for (const line of lines) {
					const text = line.trim();
					if (!text.startsWith("{")) continue;
					let event;
					try {
						event = JSON.parse(text);
					} catch {
						continue;
					}
					this.consume_event(user_id, session_id, event, pushed, () => last_push_at, (at) => {
						last_push_at = at;
					}, context_token);
				}
			};
			child.stdout?.on("data", flush_lines);
			child.on("close", (code) => {
				flush_lines();
				clearTimeout(timer);
				this.tasks.delete(user_id);
				const ok = code === 0;
				probe("codex", "run.close", {
					user_id,
					turn,
					code,
					pushed: pushed.size
				});
				if (!ok) {
					const detail = stderr.split("\n").filter(Boolean).slice(-6).join(" ");
					this.push(user_id, session_id, `（Codex 退出码 ${code}${detail ? "：" + detail : ""}）`, "error", context_token);
				}
				resolve();
			});
		});
	}
	/** 解析单个 Codex JSON 事件并（必要时）推送给用户。 */
	consume_event(user_id, session_id, event, pushed, _get_last, set_last, context_token) {
		const type = String(event.type ?? "");
		if (type === "thread.started") {
			const thread_id = String(event.thread_id ?? "");
			if (thread_id) {
				this.store.set_meta("codex_thread:" + user_id, thread_id);
				const running = this.tasks.get(user_id);
				if (running !== void 0) running.thread_id = thread_id;
			}
			return;
		}
		if (type === "turn.completed") {
			const usage = event.usage;
			probe("codex", "turn.completed", {
				user_id,
				usage
			});
			return;
		}
		if (type === "item.completed" || type === "item.started" || type === "item.updated") {
			const item = event.item;
			if (item === void 0) return;
			const item_id = String(item.id ?? "");
			const item_type = String(item.type ?? "");
			const key = `${type}:${item_id}:${item_type}`;
			if (pushed.has(key)) return;
			let text;
			if (item_type === "agent_message" && typeof item.text === "string" && type === "item.completed") text = item.text;
			if (text === void 0 || text.trim() === "") return;
			pushed.add(key);
			set_last(Date.now());
			this.push(user_id, session_id, text.trim(), item_type === "agent_message" ? "assistant" : "progress", context_token);
		}
	}
};
//#endregion
//#region lib/types/coordination/codexWatcher.js
/**
* Codex 任务完成监视器：被动盯着本机 Codex，任务一结束就主动把结果推到微信。
*
* 一个容易走错的思路是"用 Codex 的 `notify` 钩子让独立进程直接推送"。
* 微信链路里不能那么做：独立进程直接调 iLink 会变成**第二个消费者**，
* 与 harness 抢同一个微信账号（我们为此吃过亏）。所以这里在 harness 内部做轮询式监视，
* 推送复用同一条 durable outbox —— 不新增消费者、不新增发送路径。
*
* 数据来源（与 Telegram 版一致，已验证可行）：
*   - `~/.codex/state_5.sqlite` 的 `threads` 表 → 最近的会话及其 rollout 路径/工作区/标题；
*   - rollout JSONL 尾部 → 生命周期事件 `task_started` / `task_complete` / `turn_aborted`；
*   - rollout 里的 `response_item(role=assistant)` → 最终回答文本。
*
* 关键行为：**首次扫描只记录基线、不通知**（否则重启会把历史任务全推一遍）。
* @module dsh-webhook-weixin/codex-watcher
*/
/** Codex 任务完成监视器。 */
var CodexWatcher = class {
	store;
	push;
	poll_ms;
	codex_home;
	summary_limit;
	timer;
	sweeping = false;
	/** 是否已完成首轮基线扫描（首轮不通知）。 */
	seeded = false;
	/** 绑定状态库与推送实现。 */
	constructor(options) {
		this.store = options.store;
		this.push = options.push;
		this.poll_ms = options.poll_ms ?? Number(process.env.DSH_CODEX_WATCH_MS ?? "8000");
		this.codex_home = options.codex_home ?? process.env.CODEX_HOME ?? join(process.env.USERPROFILE ?? process.env.HOME ?? ".", ".codex");
		this.summary_limit = options.summary_limit ?? Number(process.env.DSH_CODEX_SUMMARY_LIMIT ?? "1800");
	}
	/** 开始监视；幂等。 */
	start() {
		if (this.timer !== void 0) return;
		const timer = setInterval(() => {
			this.sweep();
		}, this.poll_ms);
		timer.unref?.();
		this.timer = timer;
		probe("codex-watch", "start", {
			poll_ms: this.poll_ms,
			codex_home: this.codex_home
		});
		this.sweep();
	}
	/** 停止监视；幂等。 */
	stop() {
		if (this.timer !== void 0) {
			clearInterval(this.timer);
			this.timer = void 0;
		}
		probe("codex-watch", "stop");
	}
	/** 扫一轮：读取最近的 Codex 会话状态，检测"刚完成"并推送。 */
	async sweep() {
		if (this.sweeping) return;
		this.sweeping = true;
		try {
			const statuses = this.read_statuses();
			if (statuses.length === 0) return;
			for (const item of statuses) {
				const key = "codex_watch:" + item.thread_id;
				const signature = `${item.status}|${item.event_at ?? ""}`;
				if (this.store.get_meta(key) === signature) continue;
				this.store.set_meta(key, signature);
				if (!this.seeded) continue;
				if (item.status !== "已完成" && item.status !== "已中止") continue;
				const text = this.compose(item);
				probe("codex-watch", "notify", {
					thread_id: item.thread_id,
					status: item.status,
					event_at: item.event_at
				});
				for (const user_id of this.store.list_users()) this.push(user_id, text);
			}
			this.seeded = true;
		} catch (error) {
			probe("codex-watch", "sweep_failed", { error: String(error) });
		} finally {
			this.sweeping = false;
		}
	}
	/** 组装通知文本。 */
	compose(item) {
		const summary = item.status === "已中止" ? "（任务被中止）" : this.latest_assistant_text(item.rollout_path).slice(0, this.summary_limit);
		const name = item.workspace.split(/[\\/]/).filter(Boolean).pop() ?? item.workspace;
		return [
			`Codex 任务${item.status}`,
			`项目：${name}`,
			`任务：${item.title.slice(0, 80)}`,
			"",
			"结果：",
			summary || "（没有取到结果文本）"
		].join("\n");
	}
	/** 读取最近的非归档 Codex 会话状态（只读，不打扰 Codex）。 */
	read_statuses(limit = 8) {
		const state_path = join(this.codex_home, "state_5.sqlite");
		if (!existsSync(state_path)) return [];
		let db;
		try {
			db = new DatabaseSync(state_path, { readOnly: true });
			return db.prepare("SELECT id, rollout_path, cwd, title, updated_at FROM threads WHERE archived = 0 ORDER BY updated_at DESC LIMIT ?").all(limit).map((row) => {
				const rollout = String(row.rollout_path ?? "");
				const { status, event_at } = this.read_lifecycle(rollout);
				return {
					thread_id: String(row.id),
					rollout_path: rollout,
					workspace: normalize_path$1(String(row.cwd ?? "")),
					title: this.latest_user_message(rollout) || String(row.title ?? "未命名任务"),
					status,
					...event_at === void 0 ? {} : { event_at }
				};
			});
		} catch (error) {
			probe("codex-watch", "state_db_failed", { error: String(error) });
			return [];
		} finally {
			try {
				db?.close();
			} catch {}
		}
	}
	/** 从 rollout 尾部找最近的生命周期事件（倒序扫描，避免读整个大文件）。 */
	read_lifecycle(rollout_path) {
		if (!rollout_path || !existsSync(rollout_path)) return { status: "记录缺失" };
		const patterns = [
			["\"type\":\"task_complete\"", "已完成"],
			["\"type\":\"turn_aborted\"", "已中止"],
			["\"type\":\"task_started\"", "运行中"]
		];
		try {
			const size = statSync(rollout_path).size;
			const lines = readFileSync(rollout_path).subarray(Math.max(0, size - 512 * 1024)).toString("utf8").split("\n");
			for (let i = lines.length - 1; i >= 0; i -= 1) {
				const line = lines[i];
				for (const [marker, status] of patterns) {
					if (!line.includes(marker)) continue;
					const at = line.match(/"timestamp":"([^"]+)"/);
					return {
						status,
						...at === null ? {} : { event_at: at[1] }
					};
				}
			}
		} catch (error) {
			probe("codex-watch", "rollout_read_failed", {
				error: String(error),
				rollout_path
			});
		}
		return { status: "等待首次运行" };
	}
	/** 取最近一条真实用户消息作为任务标题。 */
	latest_user_message(rollout_path) {
		return this.scan_tail(rollout_path, (line) => {
			if (!line.includes("\"type\":\"user_message\"")) return void 0;
			try {
				const parsed = JSON.parse(line);
				return clean_user_message$1(String(parsed.payload?.message ?? "").trim()) || void 0;
			} catch {
				return;
			}
		}) ?? "";
	}
	/** 取最近一条助手回答文本。 */
	latest_assistant_text(rollout_path) {
		let latest = "";
		try {
			const text = readFileSync(rollout_path, "utf8");
			for (const line of text.split("\n")) {
				if (!line.includes("\"role\":\"assistant\"")) continue;
				try {
					const parsed = JSON.parse(line);
					const payload = parsed.payload;
					if (parsed.type !== "response_item" || payload?.type !== "message" || payload.role !== "assistant") continue;
					const parts = (payload.content ?? []).map((part) => part.text ?? part.output_text ?? "").filter((item) => item.length > 0);
					if (parts.length > 0) latest = parts.join("\n").trim();
				} catch {
					continue;
				}
			}
		} catch (error) {
			probe("codex-watch", "assistant_read_failed", { error: String(error) });
		}
		return latest;
	}
	/** 从文件尾部往前找第一个满足条件的行（避免全文件解析）。 */
	scan_tail(rollout_path, pick) {
		if (!existsSync(rollout_path)) return void 0;
		try {
			const size = statSync(rollout_path).size;
			const lines = readFileSync(rollout_path).subarray(Math.max(0, size - 1024 * 1024)).toString("utf8").split("\n");
			for (let i = lines.length - 1; i >= 0; i -= 1) {
				const value = pick(lines[i]);
				if (value !== void 0) return value;
			}
		} catch {
			return;
		}
	}
};
/** 去掉 Codex 包装、只留用户真实请求。 */
function clean_user_message$1(message) {
	const marker = "## My request for Codex:";
	let text = message;
	if (text.includes(marker)) text = text.slice(text.indexOf(marker) + 24);
	text = text.trim();
	if (text.startsWith("<environment_context>") || text.startsWith("<permissions instructions>")) return "";
	return text;
}
/** 去掉 Windows 长路径前缀。 */
function normalize_path$1(path) {
	return path.startsWith("\\\\?\\") ? path.slice(4) : path;
}
//#endregion
//#region lib/types/coordination/opencodeBridge.js
/**
* opencode 远程桥：用微信遥控本机 opencode，并把它的输出主动推回微信。
*
* 与 codexBridge 同构（`/opencode` 指令），但驱动的是 opencode CLI。
* 关键工程点（都是踩过来的坑）：
* - 必须 `--attach <server>`：脱离 server 续会话会无限阻塞；服务器由启动器保证在跑。
* - 必须关掉 stdin：opencode 会读管道 stdin，开着就永不返回。
* - **必须有硬超时并杀掉**：opencode 的 provider 可能长时间 stream error 重试
*   （实测 `opencode-go` 报 stream error 时 run 会卡住十几分钟），不能让它拖死微信链路。
* - 可用 `DSH_OPENCODE_MODEL` 覆盖模型，绕开出故障的默认 provider。
* @module dsh-webhook-weixin/opencode-bridge
*/
/** 默认工作区（欣爱的专属工作区）。 */
/** opencode 远程任务的默认工作目录：环境变量优先，否则当前工作目录（不写死本机路径）。 */
const DEFAULT_WORKSPACE = process.env.DSH_OPENCODE_WORKSPACE?.trim() || process.cwd();
/** 单次任务硬超时（毫秒）；默认 10 分钟。 */
/**
* 单次 opencode 任务的硬超时。
*
* 默认 30 分钟（与 codex 对齐）。原来给 10 分钟，实测"排查整条消息链路并修复"
* 这类任务会连续两次超时被终止——超时后任务其实没完成，用户还得重来一遍，
* 反而更浪费。宁可给足时间；真要中断用户可以发 `/opencode stop`。
* 可用 DSH_OPENCODE_TIMEOUT_MS 调整。
*/
const RUN_TIMEOUT_MS = Number(process.env.DSH_OPENCODE_TIMEOUT_MS ?? String(30 * 6e4));
/** 推送时的最小间隔（毫秒），避免把一堆行刷成很多条微信。 */
/**
* 终止进程树。
*
* Windows 上 opencode 会拉起子进程（server / 工具进程），只 kill 父进程会留孤儿，
* 所以统一用 `taskkill /T /F`；非 Windows 回退到普通 kill。
*/
function kill_tree(child) {
	const pid = child.pid;
	if (pid === void 0) {
		child.kill();
		return;
	}
	if (process.platform === "win32") {
		spawn("taskkill", [
			"/pid",
			String(pid),
			"/T",
			"/F"
		], {
			stdio: "ignore",
			windowsHide: true
		});
		return;
	}
	child.kill("SIGKILL");
}
/** 解析 opencode 可执行文件。 */
function resolve_bin() {
	const configured = process.env.OPENCODE_BIN;
	if (configured !== void 0 && configured.trim() && existsSync(configured.trim())) return configured.trim();
	const fallback = "opencode";
	return existsSync(fallback) ? fallback : "opencode";
}
/** 去掉 ANSI 控制序列。 */
function strip_ansi(text) {
	return text.replace(/\u001B\[[0-9;?]*[A-Za-z]/g, "").replace(/\r\n/g, "\n").replace(/\r/g, "");
}
/**
* 模型优先级链：opencode-go → 大黄蜂 → 火山。
*
* 降级规则（用户明确要求）：
* - 前两级可以**自动**降级（都在允许范围内）；
* - 到第三级（火山）**不能自动跳**，必须先向用户报告并征得同意；
*   用户回复 `/opencode no` 就停下，什么都不做。
*/
const DEFAULT_MODEL_CHAIN = [
	"opencode-go/deepseek-v4.1-flash",
	"dahuangfen/gpt-5.6-sol",
	"volcengine/deepseek-v4-flash-ga-260731"
];
/** 连通性故障的特征（决定是否降级；任务本身的失败不降级）。 */
const CONNECTIVITY_FAILURE = /Cannot connect|ProviderHeaderTimeoutError|AI_APICallError|not supported by any configured account|ECONNREFUSED|ETIMEDOUT|ENOTFOUND|fetch failed|connect timeout|socket hang up|socket connection unexpectedly closed|stream error|ProviderError/i;
/** 用户同意切换到火山的有效期（毫秒）。 */
const APPROVAL_TTL_MS = Number(process.env.DSH_OPENCODE_APPROVAL_TTL_MS ?? String(30 * 6e4));
/** 识别 `/opencode`（或 `/oc`）指令。 */
function parse_opencode_command(text) {
	const trimmed = text.trim();
	const lower = trimmed.toLowerCase();
	let rest;
	if (lower.startsWith("/opencode")) rest = trimmed.slice(9).trim();
	else if (lower.startsWith("/oc ")) rest = trimmed.slice(3).trim();
	else if (lower === "/oc") rest = "";
	if (rest === void 0) return void 0;
	if (rest === "") return {
		action: "help",
		text: ""
	};
	const [head, ...tail] = rest.split(/\s+/);
	const keyword = (head ?? "").toLowerCase();
	const body = tail.join(" ").trim();
	if (keyword === "stop" || keyword === "停止") return {
		action: "stop",
		text: body
	};
	if (keyword === "status" || keyword === "状态") return {
		action: "status",
		text: body
	};
	if (keyword === "new" || keyword === "新开") return {
		action: "new",
		text: body
	};
	if (keyword === "help" || keyword === "帮助") return {
		action: "help",
		text: body
	};
	if (keyword === "yes" || keyword === "同意" || keyword === "ok") return {
		action: "yes",
		text: body
	};
	if (keyword === "no" || keyword === "不同意" || keyword === "不用" || keyword === "取消") return {
		action: "no",
		text: body
	};
	return {
		action: "run",
		text: rest
	};
}
/** 微信遥控 opencode 的桥。 */
var OpencodeBridge = class {
	store;
	push;
	server_url;
	workspace;
	tasks = /* @__PURE__ */ new Map();
	/** 等用户确认"切火山"的挂起任务。 */
	pending = /* @__PURE__ */ new Map();
	/** 用户已同意切火山；在 TTL 内有效。 */
	approvals = /* @__PURE__ */ new Map();
	chain;
	/** 绑定状态库、推送回调、server 地址与工作区。 */
	constructor(store, push, server_url = process.env.OPENCODE_SERVER_URL ?? "http://127.0.0.1:4096", workspace = process.env.DSH_OPENCODE_WORKSPACE ?? DEFAULT_WORKSPACE) {
		this.store = store;
		this.push = push;
		this.server_url = server_url;
		this.workspace = workspace;
		const configured = process.env.DSH_OPENCODE_MODEL_CHAIN;
		this.chain = configured !== void 0 && configured.trim() !== "" ? configured.split(",").map((item) => item.trim()).filter(Boolean) : DEFAULT_MODEL_CHAIN;
	}
	/** 当前是否在有效期内的"已同意切火山"。 */
	has_volcengine_approval(user_id) {
		const at = this.approvals.get(user_id);
		if (at === void 0) return false;
		if (Date.now() - at > APPROVAL_TTL_MS) {
			this.approvals.delete(user_id);
			return false;
		}
		return true;
	}
	/** 处理一条 `/opencode` 指令。 */
	async handle(user_id, session_id, command, context_token) {
		if (command.action === "help") {
			this.push(user_id, session_id, [
				"这里是 opencode 远程控制，用法：",
				"- /opencode <任务>     交给 opencode 执行",
				"- /opencode status     查看状态",
				"- /opencode stop       终止当前任务",
				"- /opencode yes|no     确认/拒绝\"切到火山模型\"",
				`默认工作区：${this.workspace}`,
				`模型优先级：${this.chain.join(" → ")}`
			].join("\n"), "assistant", context_token);
			return;
		}
		if (command.action === "yes") {
			const waiting = this.pending.get(user_id);
			if (waiting === void 0) {
				this.push(user_id, session_id, "（当前没有待确认的切换请求）", "error", context_token);
				return;
			}
			this.pending.delete(user_id);
			this.approvals.set(user_id, Date.now());
			const volcanic = this.chain[2] ?? this.chain[this.chain.length - 1];
			this.push(user_id, session_id, `（好，这次用火山模型 ${volcanic} 继续）`, "progress", context_token);
			await this.run(user_id, session_id, waiting.text, context_token, volcanic);
			return;
		}
		if (command.action === "no") {
			if (this.pending.get(user_id) === void 0) {
				this.push(user_id, session_id, "（当前没有待确认的切换请求）", "error", context_token);
				return;
			}
			this.pending.delete(user_id);
			this.approvals.delete(user_id);
			this.push(user_id, session_id, "（好，那我停下了；opencode 和大黄蜂都不通，等你处理网络或用别的办法）", "assistant", context_token);
			return;
		}
		if (command.action === "status") {
			const task = this.tasks.get(user_id);
			this.push(user_id, session_id, task === void 0 ? "（当前没有正在运行的 opencode 任务）" : `（opencode 正在运行：已 ${Math.round((Date.now() - task.started_at) / 1e3)} 秒）`, "assistant", context_token);
			return;
		}
		if (command.action === "stop") {
			const stopped = this.stop(user_id);
			this.push(user_id, session_id, stopped ? "（已终止当前的 opencode 任务）" : "（当前没有正在运行的 opencode 任务）", "assistant", context_token);
			return;
		}
		if (this.tasks.has(user_id)) {
			this.push(user_id, session_id, "（已经有一个 opencode 任务在跑，等它结束或先发 /opencode stop）", "error", context_token);
			return;
		}
		if (!command.text.trim()) {
			this.push(user_id, session_id, "（/opencode 后面要跟具体任务）", "error", context_token);
			return;
		}
		const task = command.text.trim();
		if (this.has_volcengine_approval(user_id) && this.chain.length >= 3) {
			await this.run(user_id, session_id, task, context_token, this.chain[2]);
			return;
		}
		if (await this.try_chain(user_id, session_id, task, context_token, this.chain.slice(0, 2)) === "connectivity_failed") {
			const volcanic = this.chain[2];
			if (volcanic === void 0) {
				this.push(user_id, session_id, "（opencode 和大黄蜂都连不上，且没有配置第三级模型）", "error", context_token);
				return;
			}
			this.pending.set(user_id, {
				text: task,
				...context_token === void 0 ? {} : { context_token },
				asked_at: Date.now()
			});
			this.push(user_id, session_id, [
				"opencode（首选）和大黄蜂（次选）都连不通。",
				`现在只剩火山可用：${volcanic}`,
				"要我切到火山继续做这个任务吗？",
				"回复 `/opencode yes` 同意，或 `/opencode no` 取消（我就停在这里，不会自己跳过去）。"
			].join("\n"), "assistant", context_token);
		}
	}
	/**
	* 依次用给定模型尝试任务。
	* @returns 'ok' 表示成功；'connectivity_failed' 表示全部因连通性失败；'failed' 表示任务本身失败。
	*/
	async try_chain(user_id, session_id, task, context_token, models) {
		for (const model of models) {
			const result = await this.run(user_id, session_id, task, context_token, model);
			if (result.ok) return "ok";
			if (!result.connectivity) return "failed";
			probe("opencode-bridge", "chain.fallback", {
				user_id,
				failed_model: model
			});
		}
		return "connectivity_failed";
	}
	/** 终止当前任务；返回是否真的终止。 */
	stop(user_id) {
		const task = this.tasks.get(user_id);
		if (task === void 0) return false;
		probe("opencode-bridge", "stop", {
			user_id,
			pid: task.child.pid
		});
		kill_tree(task.child);
		this.tasks.delete(user_id);
		return true;
	}
	/**
	* 执行一次任务：边跑边把有意义的输出行推给用户，结束后给结论。
	* @returns ok=成功；connectivity=连通性失败（调用方据此降级）；否则是任务本身失败。
	*/
	async run(user_id, session_id, task_text, context_token, model_override) {
		const bin = resolve_bin();
		const model = model_override ?? process.env.DSH_OPENCODE_MODEL?.trim();
		const args = [
			"run",
			"--auto",
			"--attach",
			this.server_url,
			"--dir",
			this.workspace,
			...model === void 0 || model === "" ? [] : ["-m", model],
			task_text
		];
		probe("opencode-bridge", "run.start", {
			user_id,
			workspace: this.workspace,
			model: model ?? "(default)",
			attach: this.server_url,
			task_len: task_text.length
		});
		const window_start = Date.now();
		this.store.set_meta("oc_bridge_window", JSON.stringify({
			start: window_start,
			end: 0
		}));
		this.push(user_id, session_id, `（好呀，我让 opencode 开始做了～${model === void 0 ? "" : "模型 " + model}）`, "progress", context_token);
		const child = spawn(bin, args, {
			cwd: this.workspace,
			windowsHide: true,
			stdio: [
				"ignore",
				"pipe",
				"pipe"
			]
		});
		this.tasks.set(user_id, {
			child,
			started_at: Date.now(),
			model: model ?? "(default)"
		});
		const timer = setTimeout(() => {
			probe("opencode-bridge", "run.timeout", {
				user_id,
				timeout_ms: RUN_TIMEOUT_MS
			});
			kill_tree(child);
		}, RUN_TIMEOUT_MS);
		let stderr = "";
		let buffer = "";
		const tail = [];
		const pushed = 0;
		const handle_lines = (raw) => {
			buffer += raw;
			const lines = buffer.split(/\r?\n/);
			buffer = lines.pop() ?? "";
			for (const raw_line of lines) {
				const line = strip_ansi(raw_line).trim();
				if (line === "" || line.length < 2) continue;
				tail.push(line);
				if (tail.length > 40) tail.shift();
			}
		};
		child.stdout?.on("data", (data) => handle_lines(String(data)));
		child.stderr?.on("data", (data) => {
			stderr += String(data);
		});
		return await new Promise((resolve) => {
			let settled = false;
			const finish = (result) => {
				if (settled) return;
				settled = true;
				clearTimeout(timer);
				this.tasks.delete(user_id);
				resolve(result);
			};
			child.on("close", (code) => {
				handle_lines(buffer);
				this.store.set_meta("oc_bridge_window", JSON.stringify({
					start: window_start,
					end: Date.now()
				}));
				const detail = strip_ansi(stderr).split("\n").filter(Boolean).slice(-3).join(" ").slice(0, 300);
				const connectivity = CONNECTIVITY_FAILURE.test(detail);
				probe("opencode-bridge", "run.close", {
					user_id,
					code,
					model,
					pushed_lines: pushed,
					connectivity
				});
				const summary = tail.slice(-15).join("\n").trim();
				if (code === 0) {
					this.push(user_id, session_id, summary === "" ? "（opencode 任务完成，没有输出内容）" : summary, "assistant", context_token);
					finish({
						ok: true,
						connectivity: false
					});
					return;
				}
				if (!connectivity) this.push(user_id, session_id, `（opencode 退出码 ${code}${detail ? "：" + detail : ""}）` + (summary === "" ? "" : `\n最近输出：\n${summary}`), "error", context_token);
				finish({
					ok: false,
					connectivity
				});
			});
			child.on("error", (error) => {
				probe("opencode-bridge", "run.error", {
					user_id,
					error: error.message
				});
				this.push(user_id, session_id, `（opencode 进程启动失败：${error.message}）`, "error", context_token);
				finish({
					ok: false,
					connectivity: false
				});
			});
		});
	}
};
//#endregion
//#region lib/types/coordination/opencodeWatcher.js
/**
* opencode 完成监视器：盯着本机 opencode，任务结束就把结果推到微信。
*
* **与微信桥的分工（防止重复汇报）**：
* - 通过微信 `/opencode` 发起的任务 → 由 OpencodeBridge 自己的闭环汇报（含实时输出与完成通知）；
* - 这个监视器只负责**外部发起**的任务（opencode 桌面/CLI 里自己跑的）。
* 为区分二者，OpencodeBridge 每次运行都会把时间窗写进 meta（`oc_bridge_window`），
* 监视器跳过落在窗口内的会话，避免同一任务被报两次。
*
* 数据来源：`~/.local/share/opencode/opencode.db`（只读）
*   - `session`：id / directory / title / time_created / time_updated / model
*   - `message`：session_id + data(JSON, role)
*   - `part`：message_id + data(JSON, type=text/tool/step-finish)
* @module dsh-webhook-weixin/opencode-watcher
*/
/** opencode 完成监视器。 */
var OpencodeWatcher = class {
	store;
	push;
	poll_ms;
	data_dir;
	summary_limit;
	idle_ms;
	timer;
	sweeping = false;
	seeded = false;
	/** 绑定状态库与推送实现。 */
	constructor(options) {
		this.store = options.store;
		this.push = options.push;
		this.poll_ms = options.poll_ms ?? Number(process.env.DSH_OPENCODE_WATCH_MS ?? "10000");
		this.data_dir = options.data_dir ?? process.env.OPENCODE_DATA_DIR ?? join(process.env.USERPROFILE ?? process.env.HOME ?? ".", ".local", "share", "opencode");
		this.summary_limit = options.summary_limit ?? Number(process.env.DSH_OPENCODE_SUMMARY_LIMIT ?? "1800");
		this.idle_ms = options.idle_ms ?? Number(process.env.DSH_OPENCODE_IDLE_MS ?? "8000");
	}
	/** 开始监视；幂等。 */
	start() {
		if (this.timer !== void 0) return;
		const timer = setInterval(() => {
			this.sweep();
		}, this.poll_ms);
		timer.unref?.();
		this.timer = timer;
		probe("opencode-watch", "start", {
			poll_ms: this.poll_ms,
			data_dir: this.data_dir
		});
		this.sweep();
	}
	/** 停止监视；幂等。 */
	stop() {
		if (this.timer !== void 0) {
			clearInterval(this.timer);
			this.timer = void 0;
		}
		probe("opencode-watch", "stop");
	}
	/** 手动扫一轮（供测试与诊断直接调用）。 */
	async sweep_once() {
		await this.sweep();
	}
	/** 扫一轮：找出"刚完成"的外部 opencode 会话并推送。 */
	async sweep() {
		if (this.sweeping) return;
		this.sweeping = true;
		try {
			const db_path = join(this.data_dir, "opencode.db");
			if (!existsSync(db_path)) return;
			const sessions = this.read_recent_sessions(db_path);
			for (const session of sessions) {
				if (Date.now() - session.time_updated < this.idle_ms) continue;
				if (this.within_bridge_window(session.time_created)) {
					probe("opencode-watch", "skip_bridge_session", { session_id: session.id });
					continue;
				}
				const last = this.last_assistant_message(db_path, session.id);
				if (last === void 0) continue;
				const key = "oc_done:" + session.id;
				if (this.store.get_meta(key) === last.message_id) continue;
				this.store.set_meta(key, last.message_id);
				if (!this.seeded) continue;
				const text = last.text;
				if (text.trim() === "") continue;
				const task = this.last_user_text(db_path, session.id) || session.title;
				const body = [
					"opencode 任务完成",
					`项目：${session.directory.split(/[\\/]/).filter(Boolean).pop() ?? session.directory}`,
					`任务：${task.slice(0, 80)}`,
					"",
					"结果：",
					text.slice(0, this.summary_limit)
				].join("\n");
				probe("opencode-watch", "notify", { session_id: session.id });
				for (const user_id of this.store.list_users()) this.push(user_id, body);
			}
			this.seeded = true;
		} catch (error) {
			probe("opencode-watch", "sweep_failed", { error: String(error) });
		} finally {
			this.sweeping = false;
		}
	}
	/** 该会话是否落在微信桥的运行窗口内（防重复汇报）。 */
	within_bridge_window(created_at) {
		const raw = this.store.get_meta("oc_bridge_window");
		if (raw === void 0 || raw === "") return false;
		try {
			const window = JSON.parse(raw);
			const start = window.start ?? 0;
			const end = window.end ?? 0;
			return created_at >= start - 2e3 && (end === 0 || created_at <= end + 15e3);
		} catch {
			return false;
		}
	}
	/** 读取最近更新的会话。 */
	read_recent_sessions(db_path, limit = 10) {
		let db;
		try {
			db = new DatabaseSync(db_path, { readOnly: true });
			return db.prepare("SELECT id, directory, title, time_created, time_updated FROM session WHERE time_archived IS NULL ORDER BY time_updated DESC LIMIT ?").all(limit);
		} catch (error) {
			probe("opencode-watch", "db_failed", { error: String(error) });
			return [];
		} finally {
			try {
				db?.close();
			} catch {}
		}
	}
	/**
	* 取会话**最后一条助手消息**的 id 与其文本。
	*
	* 先定位最后一条 assistant message（按 message.rowid 倒序），再取该消息的 text 分片——
	* 这样得到的 message_id 就是稳定的"完成指纹"，同一轮完成不会被重复汇报。
	*/
	last_assistant_message(db_path, session_id) {
		let db;
		try {
			db = new DatabaseSync(db_path, { readOnly: true });
			const message = db.prepare("SELECT id FROM message WHERE session_id = ? AND data LIKE '%\"role\":\"assistant\"%' ORDER BY rowid DESC LIMIT 1").get(session_id);
			if (message === void 0) return void 0;
			const rows = db.prepare("SELECT data FROM part WHERE message_id = ? ORDER BY rowid ASC").all(message.id);
			const texts = [];
			for (const row of rows) try {
				const part = JSON.parse(row.data);
				if (part.type === "text" && typeof part.text === "string" && part.text.trim() !== "") {
					texts.push(part.text.trim());
					if (texts.join("\n").length > this.summary_limit) break;
				}
			} catch {}
			if (texts.length === 0) return void 0;
			return {
				message_id: message.id,
				text: texts.join("\n")
			};
		} catch (error) {
			probe("opencode-watch", "assistant_read_failed", { error: String(error) });
			return;
		} finally {
			try {
				db?.close();
			} catch {}
		}
	}
	/** 取会话最后一条用户消息文本（作为任务标题）。 */
	last_user_text(db_path, session_id) {
		let db;
		try {
			db = new DatabaseSync(db_path, { readOnly: true });
			const rows = db.prepare("SELECT p.data FROM part p JOIN message m ON p.message_id = m.id WHERE p.session_id = ? AND m.data LIKE '%\"role\":\"user\"%' ORDER BY p.rowid DESC LIMIT 20").all(session_id);
			for (const row of rows) try {
				const part = JSON.parse(row.data);
				if (part.type === "text" && typeof part.text === "string" && part.text.trim() !== "") return part.text.trim();
			} catch {}
		} catch {
			return "";
		} finally {
			try {
				db?.close();
			} catch {}
		}
		return "";
	}
};
//#endregion
//#region lib/types/memory/fileLock.js
/**
* 跨进程文件锁（时间线 / 知识图谱的写保护）。
*
* 为什么需要它：这两个存储都是"读完整文件 → 改 → 整体替换"的写法。
* 两个进程同时写同一天时，后写的会覆盖先写的（实测 40 条只留下 20 条），
* 而且 Windows 上并发 rename 还会直接报 `EPERM`。
*
* 实现：用 `openSync(path, 'wx')`（CREATE_NEW，**原子**）抢占一个 `.lock` 文件。
* - 抢不到就小睡重试，直到超时；
* - 持锁进程崩了会留下锁文件，所以带**陈旧锁接管**：看 mtime 是否超时，并检查 pid 是否还活着；
* - 释放时先 close 再 unlink（Windows 上持有句柄时删不掉）。
*
* 只保护**同机多进程**；跨机器共享目录不在范围内（我们也不做）。
*
* @module dsh-webhook-weixin/file-lock
*/
/** 进程是否还活着（同机）。 */
function pid_alive(pid) {
	if (!Number.isInteger(pid) || pid <= 0) return false;
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		return error.code === "EPERM";
	}
}
/** 同步小睡（这些存储都是同步 API，锁也必须同步等）。 */
function sleep_ms(ms) {
	const shared = new SharedArrayBuffer(4);
	Atomics.wait(new Int32Array(shared), 0, 0, ms);
}
/**
* 在锁保护下执行一段同步逻辑。
*
* @param target_path - 被保护的文件路径（锁文件是它 + `.lock`）。
* @param fn - 临界区逻辑。
* @param options - 超时与陈旧判定。
* @returns `fn` 的返回值。
* @throws 等不到锁时抛错——**宁可失败也不能覆盖别人的合法数据**。
*/
function with_file_lock(target_path, fn, options) {
	const timeout_ms = options?.timeout_ms ?? 1e4;
	const stale_ms = options?.stale_ms ?? 3e4;
	const retry_ms = options?.retry_ms ?? 20;
	const lock_path = `${target_path}.lock`;
	const deadline = Date.now() + timeout_ms;
	let handle;
	while (handle === void 0) try {
		handle = openSync(lock_path, "wx");
		writeSync(handle, JSON.stringify({
			pid: process.pid,
			at: Date.now()
		}));
	} catch (error) {
		if (error.code !== "EEXIST") throw error;
		let stale = false;
		try {
			const info = JSON.parse(readFileSync(lock_path, "utf8"));
			stale = Date.now() - (info.at ?? 0) > stale_ms || !pid_alive(info.pid);
		} catch {
			try {
				stale = Date.now() - statSync(lock_path).mtimeMs > stale_ms;
			} catch {
				stale = true;
			}
		}
		if (stale) {
			probe("lock", "stale_taken_over", { lock_path });
			try {
				unlinkSync(lock_path);
			} catch {}
			continue;
		}
		if (Date.now() > deadline) {
			probe("lock", "timeout", {
				lock_path,
				timeout_ms
			});
			throw new Error(`等锁超时（${timeout_ms}ms）：${lock_path}。为避免覆盖其它进程的合法写入，本次写入放弃。`);
		}
		sleep_ms(retry_ms);
	}
	try {
		return fn();
	} finally {
		try {
			closeSync(handle);
		} catch {}
		try {
			unlinkSync(lock_path);
		} catch {}
	}
}
/** 带重试的原子替换：Windows 上并发 rename 偶发 EPERM，重试几次即可。 */
function rename_with_retry(from, to, attempts = 5) {
	for (let attempt = 0;; attempt += 1) try {
		renameSync(from, to);
		return;
	} catch (error) {
		const code = error.code;
		if (attempt >= attempts - 1 || code !== "EPERM" && code !== "EBUSY") throw error;
		sleep_ms(15 * (attempt + 1));
	}
}
//#endregion
//#region lib/types/memory/timelineStore.js
/**
* 时间线存储（长期记忆）：一天一个 node，node 内 = 摘要 + 切实聊天记录。
*
* 目录结构：
* ```
* <dir>/
*   index.json                 # 日期索引：node 元数据，供快速查询/统计
*   days/
*     2026-09-20.jsonl         # 权威记录（append-only，每行一条 TimelineEntry）
*     2026-09-20.summary.md    # 权威摘要（可人工编辑）
*     2026-09-20.md            # 人类可读视图（由上面两者生成，勿手改）
* ```
*
* 为什么把 jsonl 和 md 分开：
* - `jsonl` 是**权威**记录，解析稳定、追加安全，适合程序读写；
* - `md` 是给人（也给人格化 agent）读的视图，可随时从权威数据重建；
* - 摘要单独放 `summary.md`，这样"重新生成视图"不会覆盖人工编辑过的摘要。
*
* 所有写操作都走"临时文件 + rename"的原子替换，避免断电/杀进程留下半个文件。
*
* @module dsh-webhook-weixin/timeline-store
*/
const DEFAULT_TIMEZONE = "Asia/Shanghai";
const INDEX_VERSION = 1;
/** 时间线存储：读取与更新长期记忆。 */
var TimelineStore = class {
	dir;
	days_dir;
	timezone;
	persist_reasoning;
	idle_ms;
	/**
	* 已写入记录 id 的内存缓存（按天）。
	*
	* 用途：append 的幂等校验本来要读整天文件（O(n)），1000 条时是 O(n²)。
	* 用 `文件大小:mtime` 当缓存键——文件没变就直接复用，append 变成 O(1)。
	* 别的进程改了文件，键会变，缓存自动失效（配合 file lock 保证一致）。
	*/
	id_cache = /* @__PURE__ */ new Map();
	/** 视图（.md）待重建的日期；惰性重建，避免每次 append 都重写 Markdown。 */
	dirty_views = /* @__PURE__ */ new Set();
	/** 临时文件序号（同进程同毫秒多次写不撞名）。 */
	tmp_seq = 0;
	/** 正在重建索引：防止 load_index ↔ rebuild_index 互相递归。 */
	rebuilding_index = false;
	/** 每日元数据的内存缓存（追加时增量更新，避免每次重算整天）。 */
	meta_cache = /* @__PURE__ */ new Map();
	constructor(options) {
		this.dir = options.dir;
		this.days_dir = join(this.dir, "days");
		this.timezone = options.timezone ?? DEFAULT_TIMEZONE;
		this.persist_reasoning = options.persist_reasoning ?? process.env.DSH_TIMELINE_PERSIST_REASONING === "1";
		this.idle_ms = options.idle_ms ?? Math.max(1, Number(process.env.DSH_TIMELINE_IDLE_HOURS ?? "3")) * 36e5;
		this.ensure_dirs();
	}
	/**
	* 追加一条记录（幂等：同一 id 重复追加会被忽略）。
	*
	* @returns `true` 表示真的写入；`false` 表示这条 id 已存在。
	*/
	/** 存储目录（清空记忆时需要，避免调用方另算一遍路径）。 */
	dir_path() {
		return this.dir;
	}
	/**
	* 确保存储目录存在（含 `days/` 子目录）。
	*
	* **清空记忆之后必须调用**：清空会连 `days/` 一起删掉，而 store 实例是长驻的，
	* 不会自己重建——下一次 append 就会 ENOENT（实测 2026-09-21 23:46 那轮就撞上了）。
	* 构造函数与 append 也会调用它，所以外部删目录同样能自愈。
	*/
	ensure_dirs() {
		mkdirSync(this.days_dir, { recursive: true });
	}
	/**
	* 丢掉全部内存缓存。
	*
	* **外部清空目录之后必须调用**：`meta_cache` 是按天缓存的，文件被删掉了它却还在，
	* 于是 `list_days()` 会继续列出早已不存在的天。id 缓存虽按 `size:mtime` 校验，
	* 但也一并清掉更省心。
	*/
	forget_caches() {
		this.id_cache.clear();
		this.meta_cache.clear();
		probe("timeline", "caches.forgotten", {});
	}
	append(entry) {
		this.ensure_dirs();
		const date = this.day_key(entry.ts);
		const path = this.entries_path(date);
		const record = this.persist_reasoning ? entry : {
			...entry,
			reasoning: void 0
		};
		const line = JSON.stringify(strip_undefined$1(record));
		const written = with_file_lock(path, () => {
			const ids = this.known_ids(date);
			if (ids.has(entry.id)) {
				probe("timeline", "append.duplicate", {
					date,
					id: entry.id
				});
				return false;
			}
			const base = this.base_meta(date);
			appendFileSync(path, `${line}\n`, "utf8");
			ids.add(entry.id);
			this.set_meta(date, increment_meta(base, entry));
			this.dirty_views.add(date);
			return true;
		});
		if (written) {
			this.regenerate_view_if_due(date);
			probe("timeline", "append.ok", {
				date,
				role: entry.role,
				id: entry.id,
				chars: entry.text.length
			});
		}
		return written;
	}
	/**
	* 该日期已知的记录 id 集合（带 mtime 缓存）。
	*
	* 文件被别的进程改过（大小/mtime 变化）时会重新读，保证跨进程也准确。
	*/
	known_ids(date) {
		const path = this.entries_path(date);
		const stat = existsSync(path) ? statSync(path) : void 0;
		const key = stat === void 0 ? "missing" : `${stat.size}:${stat.mtimeMs}`;
		const cached = this.id_cache.get(date);
		if (cached !== void 0 && cached.key === key) return cached.ids;
		const ids = new Set(this.read_entries(date).map((item) => item.id));
		this.id_cache.set(date, {
			key,
			ids
		});
		return ids;
	}
	/**
	* 取某日"当前基数"（追加前）。
	*
	* 优先用内存缓存（热路径 O(1)）；缓存冷时**从记录文件算**，而不是信索引——
	* 索引只是缓存，记录文件才是权威。
	*/
	base_meta(date) {
		const cached = this.meta_cache.get(date);
		if (cached !== void 0) return cached;
		const entries = this.read_entries(date);
		const meta = this.meta_from_entries(date, entries);
		this.meta_cache.set(date, meta);
		return meta;
	}
	/** 写入某日的元数据（覆盖，不累加）。 */
	set_meta(date, meta) {
		this.meta_cache.set(date, meta);
		const index = this.load_index();
		this.save_index({
			...index,
			days: {
				...index.days,
				[date]: meta
			}
		});
	}
	/** 由记录列表直接算元数据（不读索引，避免递归/双重计数）。 */
	meta_from_entries(date, entries) {
		const ts = entries.map((item) => item.ts);
		return strip_undefined$1({
			date,
			timezone: this.timezone,
			entries: entries.length,
			user_entries: entries.filter((item) => item.role === "user").length,
			assistant_entries: entries.filter((item) => item.role === "assistant").length,
			first_ts: ts.length === 0 ? void 0 : Math.min(...ts),
			last_ts: ts.length === 0 ? void 0 : Math.max(...ts),
			updated_at: Date.now()
		});
	}
	/** 视图惰性重建：同一日期最多每 5 秒重建一次，且文件缺失时必建。 */
	regenerate_view_if_due(date) {
		if (!this.dirty_views.has(date)) return;
		const path = this.view_path(date);
		const stat = existsSync(path) ? statSync(path) : void 0;
		if (stat !== void 0 && Date.now() - stat.mtimeMs < 5e3) return;
		this.regenerate_view(date);
		this.dirty_views.delete(date);
	}
	/** 强制把所有待重建的视图刷出来（调度器空闲时调用）。 */
	flush_views() {
		for (const date of [...this.dirty_views]) {
			this.regenerate_view(date);
			this.dirty_views.delete(date);
		}
	}
	/**
	* 批量追加；返回真正写入的条数。用于恢复/回填历史。
	*/
	append_many(entries) {
		let written = 0;
		for (const entry of entries) if (this.append(entry)) written += 1;
		return written;
	}
	/**
	* 给某条记录补写媒体引用/描述（记录本身是 append-only，这里做定点改写）。
	*
	* 为什么需要它：消息先落盘（拿到事实），媒体要等下载完才拿得到引用、
	* 描述更要等视觉模型跑完；这两件事都比"记下这句话"晚，所以必须能后补。
	* 改写是**整天重写 + 原子替换**（一天记录量很小），失败也不会留下半截文件。
	*
	* @returns 是否命中并改写。
	*/
	update_entry(date, entry_id, patch) {
		assert_day_key(date);
		const path = this.entries_path(date);
		return with_file_lock(path, () => {
			const entries = this.read_entries(date);
			const index = entries.findIndex((item) => item.id === entry_id);
			if (index < 0) {
				probe("timeline", "entry.update_miss", {
					date,
					id: entry_id
				});
				return false;
			}
			const current = entries[index];
			entries[index] = strip_undefined$1({
				...current,
				...patch.text === void 0 ? {} : { text: patch.text },
				...patch.attachments === void 0 ? {} : { attachments: patch.attachments }
			});
			this.write_atomic(path, `${entries.map((item) => JSON.stringify(strip_undefined$1(item))).join("\n")}\n`);
			this.refresh_meta(date);
			this.regenerate_view(date);
			this.dirty_views.delete(date);
			probe("timeline", "entry.updated", {
				date,
				id: entry_id,
				attachments: patch.attachments?.length
			});
			return true;
		});
	}
	/**
	* 给某条记录里的某个附件补上描述（视觉模型跑完后回填）。
	*
	* @returns 是否命中并写入。
	*/
	update_attachment_description(date, entry_id, attachment_id, description) {
		const entry = this.read_entries(date).find((item) => item.id === entry_id);
		if (entry?.attachments === void 0) return false;
		const next = entry.attachments.map((item) => item.id === attachment_id ? strip_undefined$1({
			...item,
			description
		}) : item);
		if (!next.some((item, i) => item.description !== entry.attachments?.[i]?.description)) return false;
		return this.update_entry(date, entry_id, { attachments: next });
	}
	/**
	* 更新某一日的摘要（权威来源是 `days/<date>.summary.md`，可人工编辑）。
	*
	* @param date - `YYYY-MM-DD`。
	* @param summary - 摘要正文；空字符串表示清空。
	* @param meta - 可选的生成信息（例如模型名），会写进索引。
	*/
	update_summary(date, summary, meta) {
		assert_day_key(date);
		this.write_atomic(this.summary_path(date), summary.trim() === "" ? "" : `${summary.trim()}\n`);
		const index = this.load_index();
		const current = index.days[date] ?? this.empty_meta(date);
		const next = strip_undefined$1({
			...current,
			summary_updated_at: Date.now(),
			summary_model: meta?.model ?? current.summary_model,
			updated_at: Date.now()
		});
		this.save_index({
			...index,
			days: {
				...index.days,
				[date]: next
			}
		});
		this.regenerate_view(date);
		this.dirty_views.delete(date);
		probe("timeline", "summary.updated", {
			date,
			chars: summary.length,
			model: meta?.model
		});
	}
	/** 从权威记录重新生成人类可读视图（`<date>.md`）；人工改过视图时用它复原。 */
	regenerate_view(date) {
		assert_day_key(date);
		const entries = this.read_entries(date);
		const meta = this.meta_for(date, entries);
		const summary = this.read_summary(date);
		this.write_atomic(this.view_path(date), render_view(meta, summary, entries));
	}
	/**
	* 重建索引：**直接扫 `days/*.jsonl`**（权威数据），不信任 index.json。
	*
	* 这是索引损坏时的自愈路径——审计复现过"损坏索引后旧日期彻底不可发现"，
	* 而记录文件其实还在。所以读取路径发现索引缺失/损坏时必须自动走这里。
	*/
	rebuild_index() {
		const dates = existsSync(this.days_dir) ? readdirSync(this.days_dir).filter((name) => name.endsWith(".jsonl")).map((name) => name.slice(0, -6)).sort() : [];
		const days = {};
		for (const date of dates) days[date] = this.meta_for(date, this.read_entries(date));
		this.save_index({
			version: INDEX_VERSION,
			timezone: this.timezone,
			updated_at: Date.now(),
			days
		});
		this.id_cache.clear();
		this.meta_cache.clear();
		for (const [date, meta] of Object.entries(days)) this.meta_cache.set(date, meta);
		probe("timeline", "index.rebuilt", { days: dates.length });
		return dates.length;
	}
	/**
	* 找出"该更新摘要"的日期。
	*
	* 判定两条同时成立：
	* 1. **已经空闲**：距该日最后一条记录 ≥ `idle_ms`（默认 3 小时）——
	*    说明这段对话讲完了，现在总结不会再反复重写；
	* 2. **有待总结的内容**：还没有摘要，或上次摘要之后又新增了记录。
	*
	* @param now - 当前时间（便于测试注入）。
	*/
	days_needing_summary(now = Date.now()) {
		const index = this.load_index();
		const out = [];
		for (const meta of Object.values(index.days)) {
			if (meta.entries === 0 || meta.last_ts === void 0) continue;
			if (now - meta.last_ts < this.idle_ms) continue;
			if ((meta.summary_updated_at ?? 0) >= meta.last_ts) continue;
			out.push(meta.date);
		}
		return out.sort();
	}
	/**
	* 用注入的摘要器更新指定日期的摘要。
	*
	* @param date - `YYYY-MM-DD`。
	* @param summarize - 摘要实现（本地 Gemma / 云端模型都行）。
	* @returns 摘要文本；该日没有记录时返回 undefined（不写空摘要）。
	*/
	async summarize_day(date, summarize) {
		assert_day_key(date);
		const entries = this.read_entries(date);
		if (entries.length === 0) return void 0;
		const previous = this.read_summary(date);
		const result = await summarize({
			date,
			timezone: this.timezone,
			entries,
			...previous === void 0 ? {} : { previous_summary: previous }
		});
		const text = result.text.trim();
		if (text === "") return void 0;
		this.update_summary(date, text, { ...result.model === void 0 ? {} : { model: result.model } });
		return text;
	}
	/** 扫描并按需更新所有到期摘要；返回更新过的日期。 */
	async summarize_due(summarize, now = Date.now()) {
		const done = [];
		for (const date of this.days_needing_summary(now)) try {
			if (await this.summarize_day(date, summarize) !== void 0) done.push(date);
		} catch (error) {
			probe("timeline", "summary.failed", {
				date,
				error: String(error)
			});
		}
		return done;
	}
	/** 读一天的 node（摘要 + 记录 + 元数据）；可用 filter 按人/会话过滤。 */
	read_day(date, filter) {
		assert_day_key(date);
		const all = this.read_entries(date);
		const entries = filter === void 0 ? all : all.filter((item) => match_filter(item, filter));
		const summary = this.read_summary(date);
		return {
			meta: this.meta_for(date, entries),
			...summary === void 0 ? {} : { summary },
			entries
		};
	}
	/**
	* 读一段日期区间（含两端）。
	*
	* @param from - 起始 `YYYY-MM-DD`。
	* @param to - 结束 `YYYY-MM-DD`；缺省等于 `from`。
	*/
	read_range(from, to) {
		const end = to ?? from;
		assert_day_key(from);
		assert_day_key(end);
		return this.list_days().map((meta) => meta.date).filter((date) => date >= from && date <= end).map((date) => this.read_day(date));
	}
	/** 列出所有 node 的元数据（按日期升序）。 */
	list_days() {
		const index = this.load_index();
		return Object.values(index.days).sort((a, b) => a.date.localeCompare(b.date));
	}
	/** 读原始摘要文本；没有则 undefined。 */
	read_summary(date) {
		const path = this.summary_path(date);
		if (!existsSync(path)) return void 0;
		const text = readFileSync(path, "utf8").trim();
		return text === "" ? void 0 : text;
	}
	/**
	* 跨天全文搜索（大小写不敏感的朴素子串匹配）。
	*
	* 长期记忆规模有限（一天一个文件），朴素扫描足够；命中按时间倒序返回。
	*/
	search(keyword, options) {
		const needle = keyword.trim().toLowerCase();
		if (needle === "") return [];
		const limit = options?.limit ?? 50;
		const filter = {
			...options?.user_id === void 0 ? {} : { user_id: options.user_id },
			...options?.session_id === void 0 ? {} : { session_id: options.session_id }
		};
		const hits = [];
		const dates = this.list_days().map((meta) => meta.date).reverse();
		for (const date of dates) {
			if (options?.from !== void 0 && date < options.from) continue;
			if (options?.to !== void 0 && date > options.to) continue;
			for (const entry of this.read_entries(date)) {
				if (!match_filter(entry, filter)) continue;
				if (entry.text.toLowerCase().includes(needle)) hits.push({
					date,
					entry
				});
				if (hits.length >= limit) return hits;
			}
		}
		return hits;
	}
	/** 全局统计。 */
	stats() {
		const days = this.list_days();
		return strip_undefined$1({
			days: days.length,
			entries: days.reduce((sum, meta) => sum + meta.entries, 0),
			first_date: days[0]?.date,
			last_date: days[days.length - 1]?.date
		});
	}
	/** 时间戳 → 时区内的日期键。 */
	day_key(ts) {
		return new Intl.DateTimeFormat("en-CA", {
			timeZone: this.timezone,
			year: "numeric",
			month: "2-digit",
			day: "2-digit"
		}).format(new Date(ts));
	}
	entries_path(date) {
		return join(this.days_dir, `${date}.jsonl`);
	}
	summary_path(date) {
		return join(this.days_dir, `${date}.summary.md`);
	}
	view_path(date) {
		return join(this.days_dir, `${date}.md`);
	}
	index_path() {
		return join(this.dir, "index.json");
	}
	read_entries(date) {
		const path = this.entries_path(date);
		if (!existsSync(path)) return [];
		const out = [];
		for (const line of readFileSync(path, "utf8").split("\n")) {
			const trimmed = line.trim();
			if (trimmed === "") continue;
			try {
				out.push(JSON.parse(trimmed));
			} catch {
				probe("timeline", "entries.bad_line", { date });
			}
		}
		return out;
	}
	meta_for(date, entries) {
		const stored = this.load_index().days[date];
		const ts = entries.map((entry) => entry.ts);
		return strip_undefined$1({
			date,
			timezone: this.timezone,
			entries: entries.length,
			user_entries: entries.filter((entry) => entry.role === "user").length,
			assistant_entries: entries.filter((entry) => entry.role === "assistant").length,
			first_ts: ts.length === 0 ? void 0 : Math.min(...ts),
			last_ts: ts.length === 0 ? void 0 : Math.max(...ts),
			updated_at: stored?.updated_at ?? Date.now(),
			summary_updated_at: stored?.summary_updated_at,
			summary_model: stored?.summary_model
		});
	}
	empty_meta(date) {
		return {
			date,
			timezone: this.timezone,
			entries: 0,
			user_entries: 0,
			assistant_entries: 0,
			updated_at: Date.now()
		};
	}
	refresh_meta(date) {
		const index = this.load_index();
		const entries = this.read_entries(date);
		const next = strip_undefined$1({
			...this.meta_from_entries(date, entries),
			summary_updated_at: index.days[date]?.summary_updated_at,
			summary_model: index.days[date]?.summary_model
		});
		this.meta_cache.set(date, next);
		this.save_index({
			...index,
			days: {
				...index.days,
				[date]: next
			}
		});
	}
	/**
	* 读索引；**缺失或损坏时自动从权威 JSONL 重建**。
	*
	* 审计复现过：索引坏掉后旧日期"数据还在但彻底不可发现"（list_days/search/摘要调度都看不到）。
	* 所以这里不能返回空索引了事——记录文件才是权威，索引只是缓存，坏了就重建。
	*/
	load_index() {
		const path = this.index_path();
		if (!existsSync(path)) return this.rebuild_from_disk();
		try {
			const parsed = JSON.parse(readFileSync(path, "utf8"));
			if (parsed.version !== INDEX_VERSION || typeof parsed.days !== "object" || parsed.days === null) {
				probe("timeline", "index.invalid_shape", { path });
				return this.rebuild_from_disk();
			}
			return parsed;
		} catch {
			probe("timeline", "index.corrupt", { path });
			return this.rebuild_from_disk();
		}
	}
	/** 从磁盘重建索引（带递归保护：重建期间再问索引就返回空表）。 */
	rebuild_from_disk() {
		const empty = {
			version: INDEX_VERSION,
			timezone: this.timezone,
			updated_at: Date.now(),
			days: {}
		};
		if (this.rebuilding_index) return empty;
		this.rebuilding_index = true;
		try {
			this.rebuild_index();
		} catch (error) {
			probe("timeline", "index.rebuild_failed", { error: String(error) });
			return empty;
		} finally {
			this.rebuilding_index = false;
		}
		try {
			return JSON.parse(readFileSync(this.index_path(), "utf8"));
		} catch {
			return empty;
		}
	}
	save_index(index) {
		this.write_atomic(this.index_path(), `${JSON.stringify({
			...index,
			updated_at: Date.now()
		}, null, 2)}\n`);
	}
	/**
	* 原子写：临时文件 + rename。
	*
	* 临时名带**进程内递增序号**：同一进程同一毫秒的两次写不会撞名
	* （只带 pid 会撞，因为 pid 相同）；rename 带重试，Windows 并发时偶发 EPERM。
	*/
	write_atomic(path, content) {
		this.tmp_seq += 1;
		const tmp = `${path}.tmp-${process.pid}-${this.tmp_seq}`;
		writeFileSync(tmp, content, "utf8");
		rename_with_retry(tmp, path);
	}
};
/** 在元数据上累加一条记录（追加时的增量更新）。 */
function increment_meta(base, entry) {
	return strip_undefined$1({
		...base,
		entries: base.entries + 1,
		user_entries: base.user_entries + (entry.role === "user" ? 1 : 0),
		assistant_entries: base.assistant_entries + (entry.role === "assistant" ? 1 : 0),
		first_ts: base.first_ts === void 0 ? entry.ts : Math.min(base.first_ts, entry.ts),
		last_ts: base.last_ts === void 0 ? entry.ts : Math.max(base.last_ts, entry.ts),
		updated_at: Date.now()
	});
}
/** 记录是否满足过滤条件；未指定的维度不参与过滤。 */
function match_filter(entry, filter) {
	if (filter.user_id !== void 0 && entry.user_id !== filter.user_id) return false;
	if (filter.session_id !== void 0 && entry.session_id !== filter.session_id) return false;
	return true;
}
/** 校验日期键格式，避免拼出奇怪的路径。 */
function assert_day_key(date) {
	if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) throw new Error(`非法的日期键：${date}（应为 YYYY-MM-DD）`);
}
/** 去掉值为 undefined 的键，兼容 exactOptionalPropertyTypes。 */
function strip_undefined$1(value) {
	const out = {};
	for (const [key, item] of Object.entries(value)) if (item !== void 0) out[key] = item;
	return out;
}
/** 渲染人类可读的一天视图。 */
function render_view(meta, summary, entries) {
	const lines = [];
	lines.push("---");
	lines.push(`date: ${meta.date}`);
	lines.push(`timezone: ${meta.timezone}`);
	lines.push(`entries: ${meta.entries}`);
	lines.push(`user_entries: ${meta.user_entries}`);
	lines.push(`assistant_entries: ${meta.assistant_entries}`);
	if (meta.first_ts !== void 0) lines.push(`first_at: ${iso(meta.first_ts, meta.timezone)}`);
	if (meta.last_ts !== void 0) lines.push(`last_at: ${iso(meta.last_ts, meta.timezone)}`);
	lines.push(`updated_at: ${iso(meta.updated_at, meta.timezone)}`);
	if (meta.summary_updated_at !== void 0) lines.push(`summary_updated_at: ${iso(meta.summary_updated_at, meta.timezone)}`);
	if (meta.summary_model !== void 0) lines.push(`summary_model: ${meta.summary_model}`);
	lines.push("---");
	lines.push("");
	lines.push(`# ${meta.date}`);
	lines.push("");
	lines.push("## 摘要");
	lines.push("");
	lines.push("<!-- 权威来源：同目录 <date>.summary.md；本文件是生成视图，勿手改。 -->");
	lines.push("");
	lines.push(summary ?? "（尚未生成）");
	lines.push("");
	lines.push("## 记录");
	lines.push("");
	lines.push("<!-- 权威来源：同目录 <date>.jsonl；本文件是生成视图，勿手改。 -->");
	lines.push("");
	for (const entry of entries) {
		lines.push(`### ${clock(entry.ts, meta.timezone)} · ${entry.role === "user" ? "用户" : "欣爱"}`);
		lines.push("");
		if (entry.text.trim() !== "") lines.push(entry.text.trim());
		for (const item of entry.attachments ?? []) {
			const size = item.bytes === void 0 ? "" : `（${Math.round(item.bytes / 1024)}KB）`;
			const dims = item.width !== void 0 && item.height !== void 0 ? ` ${item.width}×${item.height}` : "";
			lines.push(`- [${item.media_type}]${dims}${size} 附件 ${item.id}${item.name === void 0 ? "" : ` · ${item.name}`}`);
			if (item.description !== void 0) lines.push(`  - 内容：${item.description}`);
		}
		lines.push("");
	}
	return `${lines.join("\n").trimEnd()}\n`;
}
/** 时区内 HH:mm:ss。 */
function clock(ts, timezone) {
	return new Intl.DateTimeFormat("en-GB", {
		timeZone: timezone,
		hour: "2-digit",
		minute: "2-digit",
		second: "2-digit",
		hour12: false
	}).format(new Date(ts));
}
/** 时区内 ISO8601（含偏移）。 */
function iso(ts, timezone) {
	const parts = new Intl.DateTimeFormat("en-CA", {
		timeZone: timezone,
		year: "numeric",
		month: "2-digit",
		day: "2-digit",
		hour: "2-digit",
		minute: "2-digit",
		second: "2-digit",
		hour12: false
	}).format(new Date(ts));
	const offset = new Intl.DateTimeFormat("en-US", {
		timeZone: timezone,
		timeZoneName: "longOffset"
	}).formatToParts(new Date(ts)).find((part) => part.type === "timeZoneName")?.value ?? "GMT";
	const zone = offset === "GMT" ? "+00:00" : offset.replace("GMT", "");
	return `${parts.replace(", ", "T").replace(/\//g, "-")}${zone}`;
}
//#endregion
//#region lib/types/memory/MemoryToolRegistry.js
var MemoryToolRegistry = class {
	static bound = /* @__PURE__ */ new WeakMap();
	/** ctx 为作用域，key 为工具组，install 接收受事务保护的注册函数；成功后记录，卸载时移除。 */
	static register_tools(ctx, key, install) {
		const groups = this.bound.get(ctx) ?? /* @__PURE__ */ new Set();
		if (groups.has(key)) return;
		const releases = [];
		try {
			install((tool) => {
				releases.push(ctx.tools.register(tool));
			});
		} catch (error) {
			for (const release of releases.reverse()) release();
			throw error;
		}
		groups.add(key);
		this.bound.set(ctx, groups);
		ctx.effect(() => () => {
			for (const release of releases.reverse()) release();
			groups.delete(key);
		}, `shared-memory.${key}`);
	}
};
//#endregion
//#region lib/types/memory/timelineTools.js
/**
* 长期记忆（时间线）的三层工具接口。
*
* ## 三层架构（按"代价"从便宜到贵，检索顺序就是从 1 到 3）
*
* | 层 | 工具 | 数据 | 代价 |
* |---|---|---|---|
* | L1 索引 | `timeline_days` | 有哪些天、各多少条、那天摘要 | 最低：只读索引 |
* | L2 检索 | `timeline_search` | 关键词命中在**哪天**、谁说的 | 中：跨天扫记录 |
* | L3 明细 | `timeline_read` | 某天的摘要 + **逐句原文** | 最高：整段进上下文 |
*
* 设计意图：**先便宜地缩小范围，再决定要不要读原文**。
* 模型最容易犯的错是"一上来就读一整天的原文"，既浪费上下文又慢；
* 所以 L1/L2 的返回里都故意只给"日期 + 片段"，逼它先定位。
*
* 写入**不开放给模型**：记录由链路的连接器自动落盘（用户上行 + 最终回复），
* 避免模型把"自己以为发生过的事"写进长期记忆。
*
* @module dsh-webhook-weixin/timeline-tools
*/
/** 单次返回给模型的最大字符数，避免一次把上下文顶爆。 */
const MAX_CHARS$3 = 6e3;
/** 截断并标注。 */
function clip$3(text) {
	return text.length <= MAX_CHARS$3 ? text : `${text.slice(0, MAX_CHARS$3)}\n…（已截断，共 ${text.length} 字符；请缩小范围重查）`;
}
/** 校验 `YYYY-MM-DD`；不合法就返回 undefined。 */
function as_date(value) {
	return typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value.trim()) ? value.trim() : void 0;
}
/**
* 在某个 agent 作用域上注册三个时间线工具。
*
* @param agent_ctx - agent 自己的 ctx（用 `agent.ctx.effect` 保证卸载时自动清理）。
* @param store - 时间线存储。
*/
function register_timeline_tools(agent_ctx, store) {
	MemoryToolRegistry.register_tools(agent_ctx, "timeline", (register) => {
		register(defineTool({
			name: "timeline_days",
			description: [
				"查看\"长期记忆里有哪些天\"，以及每天的对话条数和当天摘要。这是最便宜的一层。",
				"",
				"什么时候该用：",
				"- 用户提到\"前几天/上次/最近\"这类模糊时间，你不确定具体是哪天；",
				"- 用户问\"这几天都聊了什么/最近怎么样\"；",
				"- 你想先看看记忆里有没有相关内容，再决定要不要细查。",
				"",
				"什么时候不该用：",
				"- 当前对话里已经有的信息，不要为了\"确认\"再查一遍；",
				"- 与用户当前请求无关时，不要为了\"更了解他\"而主动翻记忆。",
				"",
				"用法建议：先调用它拿到日期，再用 timeline_read 读某一天的细节。"
			].join("\n"),
			parameters: {
				limit: {
					type: "number",
					description: "最多列出最近多少天，默认 14。"
				},
				include_summary: {
					type: "boolean",
					description: "是否一并返回每天的摘要（默认 true；关掉只返回日期与条数，更省）。"
				}
			},
			output: {
				schema: {
					type: "object",
					additionalProperties: false,
					properties: {
						ok: {
							type: "boolean",
							required: true
						},
						text: {
							type: "string",
							required: true
						}
					}
				},
				render: (_args, value) => [{
					type: "text",
					text: String(value.text ?? "")
				}]
			},
			async execute(args) {
				const days = store.list_days();
				if (days.length === 0) return {
					ok: true,
					text: "长期记忆里还没有任何记录。"
				};
				const limit = Math.max(1, Math.min(120, Number(args.limit) || 14));
				const picked = days.slice(-limit).reverse();
				const with_summary = args.include_summary !== false;
				const lines = picked.map((meta) => {
					const head = `- ${meta.date}（${meta.entries} 条：用户 ${meta.user_entries} / 我 ${meta.assistant_entries}）`;
					if (!with_summary) return head;
					const summary = store.read_summary(meta.date);
					return summary === void 0 ? `${head}\n  摘要：（尚未生成）` : `${head}\n  摘要：${summary.replace(/\n/g, " / ")}`;
				});
				const stats = store.stats();
				return {
					ok: true,
					text: clip$3(`共 ${stats.days} 天记忆（${stats.first_date ?? "?"} ~ ${stats.last_date ?? "?"}），最近 ${picked.length} 天：\n\n` + lines.join("\n"))
				};
			}
		}));
		register(defineTool({
			name: "timeline_search",
			description: [
				"在长期记忆里按关键词搜索，返回**命中的日期 + 谁说的 + 片段**。中间层。",
				"",
				"什么时候该用：",
				"- 用户说\"我之前不是说过…/你还记得…吗\"，你要先确认有没有、在哪天；",
				"- 要回忆某个人名、地点、事项、偏好（例如\"房租\"\"复查\"\"妈妈\"）；",
				"- 想找某个话题最早/最近一次出现。",
				"",
				"什么时候不该用：",
				"- 别用它来\"遍历所有记忆\"：一次只搜一个具体关键词，不要拿\"的\"\"了\"这类常见字去搜；",
				"- 已知具体日期时直接用 timeline_read，不必搜。",
				"",
				"返回的只是片段；要看上下文，再用 timeline_read 读那一天。"
			].join("\n"),
			parameters: {
				keyword: {
					type: "string",
					required: true,
					description: "要搜索的关键词（例如\"房租\"、\"医院\"）。一次一个，尽量具体。"
				},
				from: {
					type: "string",
					description: "起始日期 YYYY-MM-DD（可选）。"
				},
				to: {
					type: "string",
					description: "结束日期 YYYY-MM-DD（可选）。"
				},
				limit: {
					type: "number",
					description: "最多返回多少条命中，默认 20。"
				}
			},
			output: {
				schema: {
					type: "object",
					additionalProperties: false,
					properties: {
						ok: {
							type: "boolean",
							required: true
						},
						text: {
							type: "string",
							required: true
						}
					}
				},
				render: (_args, value) => [{
					type: "text",
					text: String(value.text ?? "")
				}]
			},
			async execute(args) {
				const keyword = typeof args.keyword === "string" ? args.keyword.trim() : "";
				if (keyword === "") return {
					ok: false,
					text: "需要一个非空 keyword。"
				};
				const from = as_date(args.from);
				const to = as_date(args.to);
				const hits = store.search(keyword, {
					limit: Math.max(1, Math.min(100, Number(args.limit) || 20)),
					...from === void 0 ? {} : { from },
					...to === void 0 ? {} : { to }
				});
				if (hits.length === 0) return {
					ok: true,
					text: `没有找到包含"${keyword}"的记录。不要猜内容，如实告诉用户没找到。`
				};
				const lines = hits.map((hit) => `- ${hit.date} ${hit.entry.role === "user" ? "用户" : "我"}：${hit.entry.text.replace(/\n/g, " ")}`);
				return {
					ok: true,
					text: clip$3(`包含"${keyword}"的记录（${hits.length} 条，新的在前）：\n\n${lines.join("\n")}\n\n要看完整上下文，用 timeline_read 读对应日期。`)
				};
			}
		}));
		register(defineTool({
			name: "timeline_read",
			description: [
				"读某一天（或一段日期）的长期记忆：**当天的摘要 + 逐句原文**。最贵的一层，请最后用。",
				"",
				"什么时候该用：",
				"- 已经用 timeline_search / timeline_days 定位到某天，现在需要看当时到底说了什么；",
				"- 用户明确问\"X 月 X 日那天我们聊了什么\"；",
				"- 要恢复某个事项的完整背景（时间、数字、承诺）。",
				"",
				"什么时候不该用：",
				"- **不要**为了回答普通问题而一次读很多天；单次最多读 3 天，且优先只读摘要确认真有需要再看原文；",
				"- 不要凭记忆里的\"大概日期\"反复试错；不确定日期就先 timeline_days。",
				"",
				"注意：这些是用户说过的原话与你的历史回复，属于隐私内容，只用于回答用户本人。"
			].join("\n"),
			parameters: {
				date: {
					type: "string",
					required: true,
					description: "要读取的日期 YYYY-MM-DD。"
				},
				to: {
					type: "string",
					description: "结束日期 YYYY-MM-DD（可选）。给区间时会读多天，单次最多 3 天。"
				},
				include_entries: {
					type: "boolean",
					description: "是否包含逐句原文（默认 true）。只想看摘要时设 false，更省。"
				}
			},
			output: {
				schema: {
					type: "object",
					additionalProperties: false,
					properties: {
						ok: {
							type: "boolean",
							required: true
						},
						text: {
							type: "string",
							required: true
						}
					}
				},
				render: (_args, value) => [{
					type: "text",
					text: String(value.text ?? "")
				}]
			},
			async execute(args) {
				const date = as_date(args.date);
				if (date === void 0) return {
					ok: false,
					text: "date 必须是 YYYY-MM-DD 格式。"
				};
				const to = as_date(args.to) ?? date;
				const non_empty = store.read_range(date, to).filter((day) => day.entries.length > 0);
				if (non_empty.length === 0) return {
					ok: true,
					text: `${date}${to === date ? "" : " ~ " + to} 没有对话记录。不要编造内容。`
				};
				const with_entries = args.include_entries !== false;
				return {
					ok: true,
					text: clip$3(non_empty.map((day) => {
						const head = `## ${day.meta.date}（${day.meta.entries} 条）`;
						const summary = `摘要：${day.summary ?? "（尚未生成）"}`;
						if (!with_entries) return `${head}\n${summary}`;
						return `${head}\n${summary}\n\n${day.entries.map((item) => `[${item.role === "user" ? "用户" : "我"}] ${item.text}`).join("\n")}`;
					}).join("\n\n"))
				};
			}
		}));
	});
}
//#endregion
//#region lib/types/memory/graphTypes.js
/**
* 知识图谱（社交网络）的类型定义。
*
* 结构：**有向无环图**。
* - 节点 = 一个人物；
* - 有向边的语义是"**A 称呼 B 为什么**"（A→B 标 `母亲`，就说明 B 是 A 的母亲；
*   反过来 B→A 会标 `儿子`/`女儿`）。所以"关系"永远是从"谁在称呼"指向"被称呼的人"，
*   这样一条边就同时表达了两件事：称呼词 + 称呼者视角。
*
* 为什么用有向边而不是无向的"关系"：
* 无向边只能表达"两人有关系"，但有向边能表达"**谁怎么叫谁**"——
* 这正是家人之间最常出现的信息（"我妈"、"他叫你舅舅"），而且不会在性别/长幼上出错。
*
* @module dsh-webhook-weixin/graph-types
*/
/**
* 图谱的**绝对中心**：助手（欣爱）。
*
* 所有关系都从她的视角表达——她称呼谁、谁称呼她。查关系时不指明起点，
* 默认就从她出发（见 `graphTools.ts`）。
*/
const GRAPH_ASSISTANT_NAME = "欣爱";
/**
* 用户在图谱里的规范名。
*
* 用"主人"而不是"我"：图谱是**欣爱的视角**，她称呼用户为"主人"，
* 所以节点的正名就是他。真名（例如身份证上的名字）属于"逐步补充"的信息——
* 一旦从对话里得知，节点会**就地改名**并把"主人"保留为别名，
* 这样两个叫法都还能查到。
*/
const GRAPH_USER_NAME = "主人";
//#endregion
//#region lib/types/memory/graphStore.js
/**
* 知识图谱存储（社交网络）：人物节点 + "A 称呼 B 为 X"的有向边。
*
* 目录结构与时间线同构（**原始数据权威、视图可重建**）：
* ```
* <dir>/
*   nodes.jsonl    # 权威：一人一行
*   edges.jsonl    # 权威：一条称呼一行
*   index.json     # 元数据：id 计数器、更新时间
*   graph.md       # 人类可读视图（生成，勿手改）
* ```
*
* 关键设计：
* - **id 与姓名解耦**：改名不会产生新节点；查找支持姓名与别名。
* - **每条信息带证据**：`sources` 记录"哪天的哪句话"支撑它，便于纠错和取信。
* - **年龄不硬记**：能从生日算就算，避免模型写错后一直错。
* - **不删只标**：节点/边不物理删除（人不会因为一次聊天就说错而被抹掉），
*   需要"失效"时用状态字段（例如 `status: 'unknown'`）。
*
* @module dsh-webhook-weixin/graph-store
*/
/** 两个人之间最多找多深的链路（超过就没有解释价值了）。 */
const DEFAULT_MAX_DEPTH = 4;
/** 知识图谱存储。 */
var KnowledgeGraphStore = class {
	dir;
	/** 临时文件序号（同进程同毫秒多次写不撞名）。 */
	tmp_seq = 0;
	constructor(options) {
		this.dir = options.dir;
		mkdirSync(this.dir, { recursive: true });
	}
	/** 全部节点。 */
	nodes() {
		return read_jsonl(this.nodes_path());
	}
	/** 全部边。 */
	edges() {
		return read_jsonl(this.edges_path());
	}
	snapshot() {
		return {
			nodes: this.nodes(),
			edges: this.edges()
		};
	}
	/** 按 id 或姓名/别名找人（姓名匹配忽略大小写与空格）。 */
	find(name_or_id) {
		const needle = normalize(name_or_id);
		if (needle === "") return void 0;
		return this.nodes().find((node) => node.id === name_or_id || normalize(node.name) === needle || (node.aliases ?? []).some((alias) => normalize(alias) === needle));
	}
	/** 某人对外（他称呼别人）的边。 */
	edges_from(id) {
		return this.edges().filter((edge) => edge.from === id);
	}
	/** 别人对某人的边（别人怎么称呼他）。 */
	edges_to(id) {
		return this.edges().filter((edge) => edge.to === id);
	}
	/**
	* 查询 A 与 B 之间的链路（**支持"朋友的朋友"这种多跳**）。
	*
	* 从 `from` 出发按有向边广度优先，找出所有到 `to` 的简单路径（不重复经过同一节点），
	* 深度上限默认 4 跳。深度必须限制：再远的关系已经解释不动，而且搜索会爆炸。
	*
	* @returns 按跳数从短到长排序的链路；没有通路返回空数组。
	*/
	paths(from_name, to_name, options) {
		const start = this.find(from_name);
		const end = this.find(to_name);
		if (start === void 0 || end === void 0) return [];
		const max_depth = Math.max(1, Math.min(8, options?.max_depth ?? DEFAULT_MAX_DEPTH));
		const nodes = new Map(this.nodes().map((node) => [node.id, node]));
		const outgoing = /* @__PURE__ */ new Map();
		for (const edge of this.edges()) {
			const list = outgoing.get(edge.from) ?? [];
			list.push(edge);
			outgoing.set(edge.from, list);
		}
		const results = [];
		const walk = (current, visited, hops) => {
			if (current === end.id && hops.length > 0) {
				results.push(build_path(hops, nodes));
				return;
			}
			if (hops.length >= max_depth) return;
			for (const edge of outgoing.get(current) ?? []) {
				if (visited.has(edge.to)) continue;
				if (edge.to === end.id) {
					results.push(build_path([...hops, edge], nodes));
					continue;
				}
				visited.add(edge.to);
				walk(edge.to, visited, [...hops, edge]);
				visited.delete(edge.to);
			}
		};
		walk(start.id, new Set([start.id]), []);
		return results.sort((a, b) => a.hops.length - b.hops.length);
	}
	/**
	* 某人的关系网（向外 expand 多少跳），用于"他身边都有谁"这类问题。
	*/
	neighborhood(name, options) {
		const start = this.find(name);
		if (start === void 0) return [];
		const max_depth = Math.max(1, Math.min(6, options?.max_depth ?? 2));
		const nodes = new Map(this.nodes().map((node) => [node.id, node]));
		const outgoing = /* @__PURE__ */ new Map();
		for (const edge of this.edges()) {
			const list = outgoing.get(edge.from) ?? [];
			list.push(edge);
			outgoing.set(edge.from, list);
		}
		const seen = /* @__PURE__ */ new Map();
		let frontier = [start.id];
		for (let depth = 1; depth <= max_depth; depth += 1) {
			const next = [];
			for (const id of frontier) for (const edge of outgoing.get(id) ?? []) {
				if (edge.to === start.id || seen.has(edge.to)) continue;
				seen.set(edge.to, {
					name: nodes.get(edge.to)?.name ?? edge.to,
					label: edge.label,
					depth
				});
				next.push(edge.to);
			}
			frontier = next;
		}
		return [...seen.values()];
	}
	/** 人物属性（含由生日推算的年龄）。 */
	attributes(name) {
		const node = this.find(name);
		if (node === void 0) return void 0;
		const age = node.age ?? age_from_birthday(node.birthday);
		return age === void 0 ? node : {
			...node,
			age
		};
	}
	/**
	* 建立（或修复）图谱的中心——**欣爱是绝对中心**。
	*
	* 做三件事，全部幂等，所以启动时无脑调用即可：
	* 1. **只建两个中心节点**：`欣爱` 与 `主人`。除姓名外不预设任何属性——
	*    图谱应该是"聊到什么补什么"，凭空写一个"状态：unknown"只会变成噪音。
	* 2. **建立两人互相的称呼**：欣爱→主人 标 `主人`（她这么称呼他），
	*    主人→欣爱 标 `欣爱`。这两条边给了所有多跳查询一个共同起点。
	* 3. **迁移历史数据**：早期版本把用户节点叫"我"（还硬写了 `status: unknown`），
	*    这里就地改名为"主人"，并把"我"保留成别名——于是按"我""用户"仍然查得到，
	*    已有边因为存的是 id，完全不受影响。
	*/
	ensure_center() {
		const user_exists = this.find(GRAPH_USER_NAME) !== void 0;
		let changed = false;
		const migrated = this.nodes().map((node) => {
			let next = node;
			if (!user_exists && normalize(node.name) === "我") {
				next = strip_undefined({
					...next,
					name: GRAPH_USER_NAME,
					aliases: merge_list(next.aliases, ["我"])
				});
				probe("graph", "center.migrated_self", { id: node.id });
			}
			if (next.status === "unknown") next = strip_undefined({
				...next,
				status: void 0
			});
			if (next !== node) changed = true;
			return next;
		});
		if (changed) this.write_nodes(migrated);
		for (const name of [GRAPH_ASSISTANT_NAME, GRAPH_USER_NAME]) if (this.find(name) === void 0) {
			this.upsert_person({
				name,
				evidence: {
					date: today(),
					note: "图谱中心节点"
				}
			});
			probe("graph", "center.created", { name });
		}
		this.upsert_edge({
			from: GRAPH_ASSISTANT_NAME,
			to: GRAPH_USER_NAME,
			label: GRAPH_USER_NAME,
			kind: "address"
		});
		this.upsert_edge({
			from: GRAPH_USER_NAME,
			to: GRAPH_ASSISTANT_NAME,
			label: GRAPH_ASSISTANT_NAME,
			kind: "address"
		});
	}
	/**
	* 新增或更新一个人（按姓名/别名匹配已有节点；匹配不到就新建）。
	*
	* **改名只认显式 id**：只有调用方给出 `id`（说明它已经解析出"这是谁"）时，
	* `name` 才会被当成新正名。否则用别名调用（"妈妈"→已存在的"王丽"）会把正名
	* 覆盖成别名，节点从此在"王丽"名下查不到——这个坑实测踩过。
	*
	* @returns 落库后的节点。
	*/
	upsert_person(input) {
		return with_file_lock(this.nodes_path(), () => this.upsert_person_locked(input));
	}
	upsert_person_locked(input) {
		const existing = input.id === void 0 ? this.find(input.name) : this.nodes().find((n) => n.id === input.id);
		const nodes = this.nodes();
		const now = Date.now();
		if (existing === void 0) {
			const node = strip_undefined({
				id: this.next_id(),
				name: input.name.trim(),
				aliases: input.aliases,
				birthday: input.birthday,
				age: input.age,
				gender: input.gender,
				status: input.status,
				contacts: input.contacts,
				location: input.location,
				occupation: input.occupation,
				family_summary: input.family_summary,
				important_dates: input.important_dates,
				notes: input.notes,
				updated_at: now,
				sources: input.evidence === void 0 ? void 0 : [input.evidence]
			});
			nodes.push(node);
			this.write_nodes(nodes);
			probe("graph", "person.created", {
				id: node.id,
				name: node.name
			});
			return node;
		}
		const requested = input.name.trim();
		const renaming = input.id !== void 0 && requested !== "" && normalize(requested) !== normalize(existing.name);
		const merged = strip_undefined({
			...existing,
			name: input.id === void 0 || requested === "" ? existing.name : requested,
			aliases: merge_list(renaming ? merge_list(existing.aliases, [existing.name]) : existing.aliases, input.aliases),
			birthday: input.birthday ?? existing.birthday,
			age: input.age ?? existing.age,
			gender: input.gender ?? existing.gender,
			status: input.status ?? existing.status,
			contacts: merge_list(existing.contacts, input.contacts),
			location: input.location ?? existing.location,
			occupation: input.occupation ?? existing.occupation,
			family_summary: input.family_summary ?? existing.family_summary,
			important_dates: merge_dates(existing.important_dates, input.important_dates),
			notes: input.notes ?? existing.notes,
			updated_at: now,
			sources: merge_evidence(existing.sources, input.evidence)
		});
		this.write_nodes(nodes.map((node) => node.id === existing.id ? merged : node));
		probe("graph", "person.updated", {
			id: merged.id,
			name: merged.name
		});
		return merged;
	}
	/**
	* 新增或更新一条"称呼"边。
	*
	* 同一对人可以有多个称呼（`妈` / `母亲`），所以按 (from,to,label) 去重而不是按 (from,to)。
	*/
	upsert_edge(input) {
		return with_file_lock(this.edges_path(), () => this.upsert_edge_locked(input));
	}
	upsert_edge_locked(input) {
		const from = this.find(input.from);
		const to = this.find(input.to);
		if (from === void 0 || to === void 0) {
			probe("graph", "edge.missing_person", {
				from: input.from,
				to: input.to
			});
			return;
		}
		if (from.id === to.id) return void 0;
		const edges = this.edges();
		const label = input.label.trim();
		if (label === "") return void 0;
		const now = Date.now();
		const address = edges.find((edge) => edge.from === from.id && edge.to === to.id && edge.kind === "address");
		if (input.kind === "address" && address !== void 0 && address.label !== label) {
			const merged = strip_undefined({
				...address,
				label,
				previous_labels: [address.label, ...address.previous_labels ?? []].slice(0, 8),
				updated_at: now,
				sources: merge_evidence(address.sources, input.evidence)
			});
			this.write_edges(edges.map((edge) => edge === address ? merged : edge));
			probe("graph", "edge.address_updated", {
				from: from.name,
				to: to.name,
				before: address.label,
				after: label
			});
			return merged;
		}
		const existing = edges.find((edge) => edge.from === from.id && edge.to === to.id && edge.label === label);
		if (existing === void 0) {
			const edge = strip_undefined({
				from: from.id,
				to: to.id,
				label,
				kind: input.kind,
				updated_at: now,
				sources: input.evidence === void 0 ? void 0 : [input.evidence]
			});
			edges.push(edge);
			this.write_edges(edges);
			probe("graph", "edge.created", {
				from: from.name,
				to: to.name,
				label
			});
			return edge;
		}
		const merged = strip_undefined({
			...existing,
			kind: input.kind ?? existing.kind,
			updated_at: now,
			sources: merge_evidence(existing.sources, input.evidence)
		});
		this.write_edges(edges.map((edge) => edge === existing ? merged : edge));
		return merged;
	}
	/** 人类可读视图（生成）。 */
	to_markdown() {
		const nodes = this.nodes();
		const edges = this.edges();
		const by_id = new Map(nodes.map((node) => [node.id, node]));
		const lines = [
			"# 人物关系图",
			"",
			`节点 ${nodes.length} · 称呼 ${edges.length}`,
			""
		];
		for (const node of nodes) {
			lines.push(`## ${node.name}（${node.id}）`);
			lines.push("");
			lines.push(`- 别名：${(node.aliases ?? []).join("、") || "—"}`);
			lines.push(`- 生日：${node.birthday ?? "—"}　年龄：${node.age ?? age_from_birthday(node.birthday) ?? "—"}`);
			lines.push(`- 性别：${node.gender ?? "—"}　状态：${node.status ?? "unknown"}`);
			lines.push(`- 职业：${node.occupation ?? "—"}　所在地：${node.location ?? "—"}`);
			lines.push(`- 联系方式：${(node.contacts ?? []).join("、") || "—"}`);
			lines.push(`- 家庭情况：${node.family_summary ?? "—"}`);
			if ((node.important_dates ?? []).length > 0) lines.push(`- 重要日期：${(node.important_dates ?? []).map((d) => `${d.label} ${d.date}`).join("；")}`);
			if (node.notes !== void 0) lines.push(`- 备注：${node.notes}`);
			const out = edges.filter((edge) => edge.from === node.id);
			if (out.length > 0) {
				lines.push("");
				lines.push("他/她是这样称呼别人的：");
				for (const edge of out) lines.push(`- → ${by_id.get(edge.to)?.name ?? edge.to}：${edge.label}`);
			}
			lines.push("");
		}
		return lines.join("\n");
	}
	/** 重建人类可读视图。 */
	regenerate_view() {
		this.write_atomic(join(this.dir, "graph.md"), `${this.to_markdown()}\n`);
	}
	/** 统计。 */
	stats() {
		return {
			people: this.nodes().length,
			relations: this.edges().length
		};
	}
	/** 存储目录（清空图谱时需要，避免调用方另算一遍路径）。 */
	dir_path() {
		return this.dir;
	}
	/**
	* 把"消化到哪了"的水位清零。
	*
	* 清空图谱后要调它：否则水位还停在被清掉之前的位置，后续更新只会处理**新**记录，
	* 图谱会一直空着。归零之后，下一次空闲更新会把时间线里的历史重新消化一遍，
	* 图谱就能从已有记忆里逐步长回来。
	*/
	reset_watermark() {
		const { last_update_ms, last_update_ids, ...rest } = this.load_index();
		this.save_index(rest);
		probe("graph", "watermark.reset", { previous: last_update_ms ?? 0 });
	}
	nodes_path() {
		return join(this.dir, "nodes.jsonl");
	}
	edges_path() {
		return join(this.dir, "edges.jsonl");
	}
	index_path() {
		return join(this.dir, "index.json");
	}
	/**
	* 上次用聊天记录更新图谱的时间（毫秒）。
	*
	* 用它做**增量更新**：每次只处理比它更新的记录，避免重复消化同一批对话
	* （重复处理不仅浪费，还会让模型反复"确认"同一件事，容易把错误固化下来）。
	*/
	last_update_ms() {
		return this.load_index().last_update_ms;
	}
	/** 本次水位同时刻已处理过的记录 id（用于 (ts, id) 复合游标）。 */
	last_update_ids() {
		return this.load_index().last_update_ids ?? [];
	}
	/**
	* 记录本次更新覆盖到的水位：时间戳 + **该时间戳上已处理的记录 id**。
	*
	* 为什么要带 id：只存毫秒时间戳时，同一毫秒内后来新增的记录会被
	* `ts > since` 永久跳过（审计复现过）。带上 id 就能精确续上。
	*/
	set_last_update(ts, ids = []) {
		const index = this.load_index();
		const previous = index.last_update_ms ?? 0;
		if (ts < previous) return;
		const same_moment = ts === previous ? [...new Set([...index.last_update_ids ?? [], ...ids])] : [...new Set(ids)];
		this.save_index({
			...index,
			last_update_ms: ts,
			last_update_ids: same_moment.slice(-200)
		});
	}
	/**
	* 分配下一个节点 id。
	*
	* **不能只信索引**：审计复现过"索引损坏 → 计数器归零 → 新节点又拿到 p1"，
	* 重复 id 会让边指向错误的人。所以这里取"索引计数器"和"实际节点里最大编号"的较大值 + 1。
	* 万一算出已存在的 id，直接抛错（fail closed），绝不写重复 id。
	*/
	next_id() {
		const index = this.load_index();
		const from_nodes = this.max_seq_in_nodes();
		const next = Math.max(index.last_person_seq ?? 0, from_nodes) + 1;
		if (this.nodes().some((node) => node.id === `p${next}`)) throw new Error(`知识图谱 id 冲突：p${next} 已存在。拒绝写入以免边指向错误的人。`);
		this.save_index({
			...index,
			last_person_seq: next
		});
		return `p${next}`;
	}
	/** 扫节点文件取最大编号（`p12` → 12）；这是 id 分配的**权威兜底**。 */
	max_seq_in_nodes() {
		let max = 0;
		for (const node of this.nodes()) {
			const match = /^p(\d+)$/.exec(node.id);
			if (match !== null) max = Math.max(max, Number(match[1]));
		}
		return max;
	}
	load_index() {
		const path = this.index_path();
		if (!existsSync(path)) return {};
		try {
			return JSON.parse(readFileSync(path, "utf8"));
		} catch {
			const recovered = this.max_seq_in_nodes();
			probe("graph", "index.corrupt_recovered", { recovered_seq: recovered });
			return { last_person_seq: recovered };
		}
	}
	save_index(index) {
		this.write_atomic(this.index_path(), `${JSON.stringify({
			...index,
			updated_at: Date.now()
		}, null, 2)}\n`);
	}
	write_nodes(nodes) {
		this.write_atomic(this.nodes_path(), `${nodes.map((node) => JSON.stringify(strip_undefined(node))).join("\n")}\n`);
		this.regenerate_view();
	}
	write_edges(edges) {
		this.write_atomic(this.edges_path(), `${edges.map((edge) => JSON.stringify(strip_undefined(edge))).join("\n")}\n`);
		this.regenerate_view();
	}
	/** 原子写：临时文件（带进程内序号防撞名）+ rename（带重试，Windows 偶发 EPERM）。 */
	write_atomic(path, content) {
		mkdirSync(this.dir, { recursive: true });
		this.tmp_seq += 1;
		const tmp = `${path}.tmp-${process.pid}-${this.tmp_seq}`;
		writeFileSync(tmp, content, "utf8");
		rename_with_retry(tmp, path);
	}
};
/** 读 jsonl；坏行跳过（不让一行损坏毁掉整张图）。 */
function read_jsonl(path) {
	if (!existsSync(path)) return [];
	const out = [];
	for (const line of readFileSync(path, "utf8").split("\n")) {
		const trimmed = line.trim();
		if (trimmed === "") continue;
		try {
			out.push(JSON.parse(trimmed));
		} catch {
			probe("graph", "jsonl.bad_line", { path });
		}
	}
	return out;
}
/** 今天的日期（`YYYY-MM-DD`，本地时区），用作"中心节点"这类自生成证据的日期。 */
function today() {
	const now = /* @__PURE__ */ new Date();
	const pad = (value) => String(value).padStart(2, "0");
	return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`;
}
/** 姓名比较用的归一化。 */
function normalize(value) {
	return value.trim().toLowerCase().replace(/\s+/g, "");
}
/** 由生日推算年龄；只有拿到完整年份时才算。 */
function age_from_birthday(birthday) {
	if (birthday === void 0) return void 0;
	const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(birthday.trim());
	if (match === null) return void 0;
	const year = Number(match[1]);
	const month = Number(match[2]);
	const day = Number(match[3]);
	const now = /* @__PURE__ */ new Date();
	let age = now.getFullYear() - year;
	if (now.getMonth() + 1 < month || now.getMonth() + 1 === month && now.getDate() < day) age -= 1;
	return age >= 0 && age < 150 ? age : void 0;
}
/** 合并列表并去重（保持已有顺序在前）。 */
function merge_list(existing, added) {
	const out = [];
	for (const item of [...existing ?? [], ...added ?? []]) {
		const value = item.trim();
		if (value !== "" && !out.includes(value)) out.push(value);
	}
	return out.length === 0 ? void 0 : out;
}
/** 合并重要日期（按 label 去重，新值覆盖旧值）。 */
function merge_dates(existing, added) {
	const map = /* @__PURE__ */ new Map();
	for (const item of existing ?? []) map.set(item.label, item.date);
	for (const item of added ?? []) map.set(item.label, item.date);
	return map.size === 0 ? void 0 : [...map].map(([label, date]) => ({
		label,
		date
	}));
}
/** 追加证据并去重（同一天同一条只留一份）。 */
function merge_evidence(existing, added) {
	if (added === void 0) return existing === void 0 ? void 0 : [...existing];
	const out = [...existing ?? []];
	if (!out.some((item) => item.date === added.date && item.note === added.note)) out.push(added);
	return out.slice(-12);
}
/** 由跳链构造可读路径。 */
function build_path(hops, nodes) {
	const name = (id) => nodes.get(id)?.name ?? id;
	const node_ids = [hops[0].from, ...hops.map((edge) => edge.to)];
	const readable = hops.map((edge) => `${name(edge.from)}的${edge.label}是${name(edge.to)}`).join("，");
	return {
		node_ids,
		names: node_ids.map(name),
		hops: hops.map((edge) => ({
			from: name(edge.from),
			to: name(edge.to),
			label: edge.label
		})),
		readable
	};
}
/** 去掉 undefined 字段，兼容 exactOptionalPropertyTypes。 */
function strip_undefined(value) {
	const out = {};
	for (const [key, item] of Object.entries(value)) if (item !== void 0) out[key] = item;
	return out;
}
//#endregion
//#region lib/types/memory/graphTools.js
/**
* 知识图谱（社交网络）的查询工具。
*
* 只提供**一个**工具 `relation_query`，两种用法：
* - 给两个人名 → 查他们之间的称呼链路（**递归多跳**，能回答"朋友的朋友"这种关系）；
* - 只给一个人名 → 查这个人的属性 + 向外的关系网。
*
* 为什么合成一个工具：模型（尤其中小参数）在工具变多时容易选错。
* 一个入口 + 可选参数，比"person_info / relation_path"两个工具更不容易用错。
*
* 写入不开放给模型：图谱由链路在空闲时用 Gemma 分批更新，避免模型凭印象改事实。
*
* @module dsh-webhook-weixin/graph-tools
*/
/** 单次返回给模型的最大字符数。 */
const MAX_CHARS$2 = 5e3;
function clip$2(text) {
	return text.length <= MAX_CHARS$2 ? text : `${text.slice(0, MAX_CHARS$2)}\n…（已截断，请缩小范围）`;
}
/** 人物属性的可读片段。 */
function describe_person(node) {
	const bits = [];
	if (node.aliases !== void 0 && node.aliases.length > 0) bits.push(`别名 ${node.aliases.join("、")}`);
	if (node.gender !== void 0 && node.gender !== "unknown") bits.push(`性别 ${{
		male: "男",
		female: "女",
		other: "其他"
	}[node.gender] ?? node.gender}`);
	if (node.birthday !== void 0) bits.push(`生日 ${node.birthday}`);
	if (node.age !== void 0) bits.push(`年龄 ${node.age}`);
	if (node.status !== void 0 && node.status !== "unknown") bits.push(`状态 ${{
		alive: "在世",
		deceased: "已故",
		lost_contact: "失联"
	}[node.status] ?? node.status}`);
	if (node.occupation !== void 0) bits.push(`职业 ${node.occupation}`);
	if (node.location !== void 0) bits.push(`所在地 ${node.location}`);
	if (node.contacts !== void 0 && node.contacts.length > 0) bits.push(`联系方式 ${node.contacts.join("、")}`);
	if (node.family_summary !== void 0) bits.push(`家庭情况 ${node.family_summary}`);
	if (node.important_dates !== void 0 && node.important_dates.length > 0) bits.push(`重要日期 ${node.important_dates.map((item) => `${item.label} ${item.date}`).join("；")}`);
	if (node.notes !== void 0) bits.push(`备注 ${node.notes}`);
	return bits.length === 0 ? "（暂无属性）" : bits.join("；");
}
/** 某人对外一层的称呼（"称呼 X 为 Y"），作为属性之外的补充。 */
function relation_lines(graph, id) {
	const out = graph.edges_from(id);
	if (out.length === 0) return [];
	const by_id = new Map(graph.nodes().map((node) => [node.id, node.name]));
	return [`　他/她称呼：${out.map((edge) => `${by_id.get(edge.to) ?? edge.to}为「${edge.label}」`).join("；")}`];
}
/**
* 在 agent 作用域注册图谱查询工具。
*
* @param agent_ctx - agent 自己的 ctx。
* @param graph - 知识图谱存储。
*/
function register_graph_tools(agent_ctx, graph) {
	MemoryToolRegistry.register_tools(agent_ctx, "graph", (register) => {
		register(defineTool({
			name: "relation_query",
			description: [
				"查询家人/朋友关系（知识图谱）。这张图**以「欣爱」（你自己）为中心**：",
				"不填 from 就默认从欣爱出发，所以\"主人和 XX 是什么关系\"直接给 to 即可。",
				"图谱里的两个固定节点：`欣爱`（你）与 `主人`（用户本人，真名可能在他的别名里）。",
				"",
				"两种用法：",
				"- 只给 to：查\"欣爱 → to\"的链路，也就是这个人和这个家是什么关系；",
				"- 只给 from：查这个人的资料 + 他身边都有谁（向外 2 层）；",
				"- 同时给 from 和 to：查两人的关系链路，**会自动递归多跳**，",
				"  例如\"小红是小明的朋友，小红的朋友是老王\"。",
				"",
				"什么时候该用：",
				"- 用户问\"XX 是谁/XX 的电话/XX 在哪/XX 是我什么人\"（\"我\"就是 `主人`）；",
				"- 用户提到一个你不认识的人名，需要先查一下再回答；",
				"- 需要理清\"谁的谁\"这种链路（亲戚、朋友的朋友）。",
				"",
				"什么时候不该用：",
				"- 当前对话里已经说清楚的信息，不要为了确认再查；",
				"- 用户只是闲聊、不涉及具体人物时不要调用。",
				"",
				"返回的是图谱里**已记录**的内容；如果没这个人或没有通路，会如实告诉你，",
				"不要据此编造关系或属性。"
			].join("\n"),
			parameters: {
				from: {
					type: "string",
					description: "起点人物姓名（或已知别名）；不填默认「欣爱」。"
				},
				to: {
					type: "string",
					description: "终点人物姓名；给了就查两人之间的关系链路。"
				},
				max_depth: {
					type: "number",
					description: "链路最多几跳，默认 4。"
				}
			},
			output: {
				schema: {
					type: "object",
					additionalProperties: false,
					properties: {
						ok: {
							type: "boolean",
							required: true
						},
						text: {
							type: "string",
							required: true
						}
					}
				},
				render: (_args, value) => [{
					type: "text",
					text: String(value.text ?? "")
				}]
			},
			async execute(args) {
				const from = typeof args.from === "string" ? args.from.trim() : "";
				const to = typeof args.to === "string" ? args.to.trim() : "";
				if (from === "" && to === "") return {
					ok: false,
					text: "至少要给 from 或 to 一个人名。"
				};
				const origin = from === "" ? GRAPH_ASSISTANT_NAME : from;
				const start = graph.find(origin);
				if (start === void 0) {
					const known = graph.nodes().map((node) => node.name).slice(0, 30);
					return {
						ok: true,
						text: known.length === 0 ? "知识图谱里还没有任何人物记录。" : `没有找到"${origin}"。目前记录在册的人有：${known.join("、")}。请确认名字，或如实告诉用户你还不认识这个人。`
					};
				}
				const max_depth = Number(args.max_depth) || 4;
				if (to === "") {
					const attributes = graph.attributes(start.name);
					const ring = graph.neighborhood(start.name, { max_depth: 2 });
					const lines = [`【${start.name}】${describe_person(attributes ?? start)}`];
					if (ring.length > 0) {
						lines.push("");
						lines.push("关系网（他/她怎么称呼别人）：");
						for (const item of ring) lines.push(`- ${item.label}：${item.name}${item.depth > 1 ? `（${item.depth} 层关系）` : ""}`);
					} else lines.push("", "（图谱里还没有这个人的关系记录）");
					return {
						ok: true,
						text: clip$2(lines.join("\n"))
					};
				}
				const end = graph.find(to);
				if (end === void 0) return {
					ok: true,
					text: `没有找到"${to}"。目前记录在册的人有：${graph.nodes().map((node) => node.name).slice(0, 30).join("、")}。`
				};
				const paths = graph.paths(start.name, end.name, { max_depth });
				if (paths.length === 0) return {
					ok: true,
					text: `图谱里没有从"${start.name}"到"${end.name}"的关系链路（最多找了 ${max_depth} 跳）。不要据此编造关系；如果用户说了他们的关系，可以在回复里请用户确认。`
				};
				const lines = [
					`【${start.name}】${describe_person(graph.attributes(start.name) ?? start)}`,
					...relation_lines(graph, start.id),
					`【${end.name}】${describe_person(graph.attributes(end.name) ?? end)}`,
					...relation_lines(graph, end.id),
					"",
					`找到 ${paths.length} 条关系链路：`
				];
				for (const path of paths.slice(0, 5)) lines.push(`- ${path.readable}（${path.hops.length} 跳）`);
				return {
					ok: true,
					text: clip$2(lines.join("\n"))
				};
			}
		}));
	});
}
//#endregion
//#region lib/types/memory/graphProtocol.js
/**
* 图谱更新用的**中文短协议**（不是 JSON）。
*
* 为什么不用 JSON：本地 Gemma 是小模型，让它输出 JSON 时经常出现
* 引号不配对、尾逗号、字段名漂移；一旦解析失败整批就白跑。
* 改成"一行一条、竖线分隔"的短协议后：格式约束极弱、容错极高，
* 坏行可以直接跳过而不影响其它行——这是我们在"中文短协议风格"上的一贯取舍。
*
* 协议（每行一个，`|` 分隔，行首是标签）：
* ```
* 人物|姓名|别名1,别名2
* 属性|姓名|字段|值
* 关系|称呼者|被称呼者|称呼
* 事件|姓名|日期|描述
* 未绑定|指代词|原文片段
* ```
* 无内容时输出 `无`。除这些之外的内容一律忽略。
*
* @module dsh-webhook-weixin/graph-protocol
*/
/** 协议里"属性"字段名 → 节点字段。用中文名是为了让模型不用翻译。 */
const ATTRIBUTE_FIELDS = {
	姓名: "name",
	别名: "aliases",
	出生日期: "birthday",
	生日: "birthday",
	年龄: "age",
	性别: "gender",
	状态: "status",
	联系方式: "contacts",
	电话: "contacts",
	所在地: "location",
	职业: "occupation",
	家庭情况: "family_summary",
	重要日期: "important_dates",
	备注: "notes"
};
/** 把中文字段名映射成节点字段；不认识就原样返回（由调用方决定是否忽略）。 */
function map_attribute_field(field) {
	const key = field.trim();
	return ATTRIBUTE_FIELDS[key] ?? key;
}
/** 该字段是否是图谱认可的属性字段。 */
function is_known_attribute_field(field) {
	return ATTRIBUTE_FIELDS[field.trim()] !== void 0;
}
/**
* 解析模型输出的短协议文本。
*
* 容错优先：只认行首标签，字段数不对或值空白的行进 `bad_lines`，绝不抛错。
*/
function parse_facts(text) {
	const facts = [];
	const bad_lines = [];
	for (const raw of text.split("\n")) {
		const line = raw.trim().replace(/^[-*]\s*/, "");
		if (line === "" || line === "无" || line.startsWith("#")) continue;
		const parts = line.split("|").map((item) => item.trim());
		const at = (index) => parts[index] ?? "";
		switch (at(0)) {
			case "人物": {
				if (at(1) === "") {
					bad_lines.push(line);
					break;
				}
				const aliases = at(2).split(/[,，、]/).map((item) => item.trim()).filter((item) => item !== "");
				facts.push({
					kind: "person",
					name: at(1),
					...aliases.length === 0 ? {} : { aliases }
				});
				break;
			}
			case "属性":
				if (at(1) === "" || at(2) === "" || at(3) === "") {
					bad_lines.push(line);
					break;
				}
				facts.push({
					kind: "attribute",
					name: at(1),
					field: at(2),
					value: at(3)
				});
				break;
			case "关系":
				if (at(1) === "" || at(2) === "" || at(3) === "") {
					bad_lines.push(line);
					break;
				}
				facts.push({
					kind: "relation",
					from: at(1),
					to: at(2),
					label: at(3)
				});
				break;
			case "事件":
				if (at(1) === "" || at(3) === "") {
					bad_lines.push(line);
					break;
				}
				facts.push({
					kind: "event",
					name: at(1),
					date: at(2),
					description: at(3)
				});
				break;
			case "未绑定":
				if (at(1) === "") {
					bad_lines.push(line);
					break;
				}
				facts.push({
					kind: "unresolved",
					reference: at(1),
					quote: at(3) === "" ? at(2) : at(3)
				});
				break;
			case "上文": {
				const text = parts.slice(1).join("|").trim();
				if (text === "") {
					bad_lines.push(line);
					break;
				}
				facts.push({
					kind: "gist",
					text
				});
				break;
			}
			default: bad_lines.push(line);
		}
	}
	return {
		facts,
		bad_lines
	};
}
/**
* 渲染"指代候选"清单——**这是解决"之前那个人"的关键**。
*
* 小模型靠长上下文解指代既慢又吃显存；改成给它一份**很小的候选名单**
* （最近提到过的人 + 图谱里已有的人 + 上一块提到的最后几个人），
* 让它做"指代词 → 名单里哪一个"的选择题，比自由回忆稳得多。
*
* @param people - 候选人物（姓名 + 可选的关系提示，例如"小李（同事）"）。
* @param limit - 最多列几个，防止名单本身变长。
*/
function render_candidates(people, limit = 30) {
	const unique = [];
	for (const item of people) {
		const value = item.trim();
		if (value !== "" && !unique.includes(value)) unique.push(value);
	}
	if (unique.length === 0) return "（暂无已知人物）";
	return unique.slice(-limit).join("、");
}
/** 把待解指代的片段渲染进下一次调用的上下文（有界，避免无限累积）。 */
function render_pending(pending, limit = 5) {
	if (pending.length === 0) return "";
	return pending.slice(-limit).map((item) => `- "${item.reference}" → 可能指：${item.quote}`).join("\n");
}
//#endregion
//#region lib/types/memory/graphUpdater.js
/**
* 用本地 Gemma 从聊天记录里更新知识图谱。
*
* ## 为什么要分成"小块 + 多次小调用"，而不是"一次读一大段"
*
* 显存只有 8GB（模型+视觉投影器已占约 5GB），**长上下文就是最贵的资源**。
* 而"之前那个人"这类指代又天然需要上下文——两者直接冲突。
* 解法不是把上下文变长，而是**把上下文变窄 + 用一份很小的候选名单把指代变成选择题**：
*
* 1. **分块**：记录按 ~2.5k 字符切块，每块单独调用，输入永远很短；
* 2. **滚动指代候选**：每次调用都带上"最近提到过的人 + 图谱已有的人"（几十个 token），
*    让模型做"`那个人` → 名单里哪一个"的选择，而不是自由回忆；
* 3. **跨块续接**：上一块末尾几行 + 未解开的指代片段带进下一块（有界），
*    这样跨块的"之前说的那个人"也能接上；
* 4. **先抽取、后落库**：模型只输出**中文短协议事实**（一行一条），
*    由代码解析并写库——模型不直接改图，格式错一行也只丢一行；
* 5. **逐节点审核**：只对"这批记录里被提到的人"再各调一次，
*    且只喂**与这个人有关的事实**（几十到几百 token），这就是"递归遍历节点"，
*    但代价不随图谱规模线性膨胀（可选 `full_scan` 强制全量遍历）。
*
* 时间不敏感（都在空闲时跑），所以"多调用、每次很小"是明确更优的取舍。
*
* @module dsh-webhook-weixin/graph-updater
*/
/**
* 说话人别名 → 图谱里的规范名。
*
* 实测教训：模型会把对话里的说话人标签（"用户""助手"）当成人名建节点，
* 还顺势编出"职业=技术支持"这种属性。所以这里做**代码级兜底**——
* 不指望提示词能 100% 拦住，落库前统一归一化。
*
* 对话记录里的说话人标签直接用图谱里的规范名（`[主人]` 与 `[欣爱]`），
* 这样模型看到的就是要输出的名字，少一层映射。
*/
const SELF_ALIASES = [
	"我",
	"用户",
	"主人",
	"本人",
	"自己"
];
const ASSISTANT_ALIASES = [
	"欣爱",
	"助手",
	"小爱",
	"你"
];
/** 把说话人别名归一到规范名；不是别名就原样返回。 */
function canonical_person_name(name) {
	const value = name.trim();
	if (SELF_ALIASES.includes(value)) return GRAPH_USER_NAME;
	if (ASSISTANT_ALIASES.includes(value)) return GRAPH_ASSISTANT_NAME;
	return value;
}
/**
* 抽取用提示词（中文短协议）。
*
* 格式说明**刻意不写"可照抄的示例行"**：实测写成 `属性|姓名|字段|值` 时，
* 模型会把这行当成一条数据原样输出（4 次里 2 次），污染事实。
* 改成"标签 + 竖线 + 字段名"的**散文式描述**后，照抄现象消失（4 次里 0 次），
* 同一份测试的召回率也从 1.25/3 提升到 2.25/3。
*/
const EXTRACT_RULES = [
	"你在把聊天记录整理成\"人物事实\"，用于维护一份家人/朋友关系图。",
	"",
	"【输出格式】每行一条，各欄用竖线分隔。行首标签只允许这几种：",
	"第一种是\"人物\"，后面依次跟：人名、别名（多个用逗号隔开，没有就留空）。",
	"第二种是\"属性\"，后面依次跟：人名、字段名、值。",
	"第三种是\"关系\"，后面依次跟：称呼者、被称呼者、称呼。",
	"第四种是\"事件\"，后面依次跟：人名、日期、描述。",
	"第五种是\"未绑定\"，后面依次跟：指代词、原文片段。",
	"第六种是\"上文\"，后面跟一句话，概括这一块里与人物有关的要点（供下一块判断指代用）。",
	"（不要把上面这些说明当成内容抄下来；只输出真实提取到的行。）",
	"没有内容就输出：无",
	"",
	"【字段名只能用这些】姓名/别名/出生日期/年龄/性别/状态/联系方式/所在地/职业/家庭情况/重要日期/备注",
	"",
	"【硬性规则】",
	"0. 这张图以\"欣爱\"为中心。记录里的两个说话人已经有名字，直接用：",
	"   \"[主人]\"是主人本人（图谱里的规范名就是\"主人\"），\"[欣爱]\"是你自己（规范名\"欣爱\"）。",
	"   凡是没有指明\"谁在称呼\"的亲属关系，都是主人的关系，称呼者一律写\"主人\"。",
	"0.1 \"主人\"只是**称呼**，不是主人的姓名——**他的姓名还不知道**。",
	"   除非记录里明确说出姓名（\"我叫张三\"），否则不要写 属性|主人|姓名|…，也不要猜。",
	"   如果记录里说了以后怎么称呼（\"以后叫我老板\"），写成：关系|欣爱|主人|老板。",
	"1. 只写记录里明确出现的信息。禁止推断、禁止补常识、禁止编造。",
	"1.1 \"用户\"\"助手\"这类词是说话人标记，**不是人名**，绝不要写成 人物 行，也不要据此编造职业/身份。",
	"2. \"关系\"的方向是**称呼者怎么称呼被称呼者**：记录里说\"我妈王丽\"，就写：关系|主人|王丽|妈妈。",
	"3. 指代词（他/她/那个人/之前那个/我同事）**必须换成名单里的具体姓名**才能写成 人物/属性/关系。",
	"   换不出来就写 未绑定|指代词|原文片段，不要猜。",
	"4. 不要把寒暄、情绪、语气词写成事实。",
	"5. 不确定的宁可不写。"
].join("\n");
/**
* 合并压缩用提示词。
*
* 触发场景：已经攒了压缩上下文，再带上当前块就超限了。
* 这时**不是丢掉旧提要**，而是把"旧压缩块 + 当前块"一起再压成一个新块继续跑——
* 信息被压缩而不是被丢弃，这是与"直接截断"的本质区别。
*/
const MERGE_RULES = [
	"你在压缩一份\"人物事实\"的上下文，压缩后会继续用来处理后面的聊天记录。",
	"输入有三部分：旧的上下文提要、旧的事实清单、新的一段聊天记录。",
	"",
	"【输出格式】每行一条，用竖线分隔：",
	"提要行：行首写\"上文\"，后面跟一句话概括要点（最多 10 行；务必保留还没解开的指代）。",
	"事实行：行首写\"属性\"或\"关系\"或\"人物\"，后面按原来的字段顺序跟内容（去重，最多 60 行）。",
	"",
	"【硬性规则】",
	"1. **事实不能因为压缩而丢**：人名、称呼、关键属性必须保留下来。",
	"2. 新旧冲突时以新记录为准。",
	"3. 只输出上面两种行，不要解释、不要前言后语。"
].join("\n");
/** 逐节点审核用提示词。 */
const REVIEW_RULES = [
	"你在审核一个人物档案要不要更新。",
	"",
	"【输出格式】每行一条，只允许：",
	"更新|字段|值",
	"没有需要更新就输出：无",
	"",
	"【硬性规则】",
	"1. 只能根据下面给出的\"本次事实\"来更新，不能凭印象补充。",
	"2. 字段名只能用：姓名/别名/出生日期/年龄/性别/状态/联系方式/所在地/职业/家庭情况/重要日期/备注。",
	"3. 事实与现有档案冲突时，以本次事实为准并更新；没有提到的字段不要动。",
	"4. 不确定就不输出。"
].join("\n");
/** 新增人物用提示词。 */
const NEW_PERSON_RULES = [
	"你在决定要不要给关系图添加**新人物**。",
	"",
	"【输出格式】每行一条，只允许：",
	"人物|姓名|别名1,别名2",
	"关系|称呼者|被称呼者|称呼",
	"没有新人物就输出：无",
	"",
	"【硬性规则】",
	"1. 名单里已经有的人**不要**再输出 人物 行。",
	"2. 只添加记录里明确出现、且有名字的人。指代词换不出姓名的不要添加。",
	"3. 关系行的方向是\"称呼者怎么称呼被称呼者\"。",
	"4. 不确定就不输出。"
].join("\n");
/**
* 从一段聊天记录更新知识图谱。
*
* @param options - 图谱、模型调用、分块参数。
* @param records - 待处理的记录（按时间升序）。
* @returns 本次更新的统计。
*/
async function update_graph_from_records(options, records) {
	const graph = options.graph;
	const chunk_chars = options.chunk_chars ?? Number(process.env.DSH_GRAPH_CHUNK_CHARS ?? "8000");
	const max_prompt_chars = options.max_prompt_chars ?? Number(process.env.DSH_GRAPH_MAX_PROMPT_CHARS ?? "30000");
	graph.ensure_center();
	const chunks = chunk_records(records, chunk_chars);
	const all_facts = [];
	let bad_lines = 0;
	let unresolved_total = 0;
	let unresolved = [];
	const gists = [];
	const facts_carried = [];
	let trimmed_prompts = 0;
	let merged_count = 0;
	for (const [index, chunk] of chunks.entries()) {
		const date = chunk[chunk.length - 1]?.date ?? "";
		const build = (gist_list, fact_list) => [
			EXTRACT_RULES,
			"",
			"【已知人物名单（指代词只能从中选择，或用 未绑定）】",
			render_candidates(candidate_names(graph)),
			...carry_section(gist_list, fact_list, unresolved),
			...tail_of(records, chunk, 4) === "" ? [] : [
				"",
				"【本块之前的几句（帮助判断指代）】",
				tail_of(records, chunk, 4)
			],
			"",
			"【本块聊天记录】",
			chunk.map((item) => format_line(item)).join("\n")
		].join("\n");
		let prompt = build(gists, facts_carried);
		if (prompt.length > max_prompt_chars && (gists.length > 0 || facts_carried.length > 0)) {
			const merge_prompt = build_merge(gists, facts_carried, chunk);
			if (merge_prompt.length <= max_prompt_chars) {
				const merged = parse_facts(await options.complete(merge_prompt));
				bad_lines += merged.bad_lines.length;
				gists.length = 0;
				facts_carried.length = 0;
				for (const fact of merged.facts) {
					if (fact.kind === "unresolved") {
						unresolved.push({
							reference: fact.reference,
							quote: fact.quote
						});
						unresolved_total += 1;
						continue;
					}
					if (fact.kind === "gist") {
						gists.push(fact.text);
						continue;
					}
					const canonical = canonicalize_fact(fact);
					all_facts.push({
						fact: canonical,
						date,
						quote: quote_of(canonical)
					});
					facts_carried.push(render_fact(canonical));
				}
				merged_count += 1;
				probe("graph", "update.merged", {
					index: index + 1,
					gists: gists.length,
					facts: facts_carried.length,
					prompt_chars: merge_prompt.length
				});
				continue;
			}
			let gist_list = [...gists];
			let fact_list = [...facts_carried];
			while (merge_prompt.length > max_prompt_chars && (gist_list.length > 0 || fact_list.length > 0)) {
				if (gist_list.length > 0) gist_list = gist_list.slice(1);
				else fact_list = fact_list.slice(1);
				trimmed_prompts += 1;
				break;
			}
			gists.length = 0;
			gists.push(...gist_list);
			facts_carried.length = 0;
			facts_carried.push(...fact_list);
			prompt = build(gists, facts_carried);
		}
		const parsed = parse_facts(await options.complete(prompt));
		bad_lines += parsed.bad_lines.length;
		unresolved = [];
		for (const fact of parsed.facts) {
			if (fact.kind === "unresolved") {
				unresolved.push({
					reference: fact.reference,
					quote: fact.quote
				});
				unresolved_total += 1;
				continue;
			}
			if (fact.kind === "gist") {
				gists.push(fact.text);
				continue;
			}
			const canonical = canonicalize_fact(fact);
			all_facts.push({
				fact: canonical,
				date,
				quote: quote_of(canonical)
			});
			facts_carried.push(render_fact(canonical));
		}
		probe("graph", "update.chunk_done", {
			index: index + 1,
			chunks: chunks.length,
			facts: parsed.facts.length,
			bad: parsed.bad_lines.length,
			prompt_chars: prompt.length,
			trimmed: trimmed_prompts
		});
	}
	const created = [];
	const touched = /* @__PURE__ */ new Set();
	for (const item of all_facts) switch (item.fact.kind) {
		case "person": {
			const existed = graph.find(item.fact.name) !== void 0;
			graph.upsert_person({
				name: item.fact.name,
				...item.fact.aliases === void 0 ? {} : { aliases: item.fact.aliases },
				evidence: {
					date: item.date,
					note: item.quote
				}
			});
			if (!existed) created.push(item.fact.name);
			touched.add(item.fact.name);
			break;
		}
		case "attribute":
			touched.add(item.fact.name);
			break;
		case "relation":
			touched.add(item.fact.from);
			touched.add(item.fact.to);
			break;
		default: break;
	}
	const to_review = options.full_scan === true ? graph.nodes().map((node) => node.name) : [...touched];
	const updated = [];
	for (const name of to_review) {
		const node = graph.find(name);
		if (node === void 0) continue;
		const related = all_facts.filter((item) => item.fact.kind === "attribute" && item.fact.name === name || item.fact.kind === "person" && item.fact.name === name);
		if (related.length === 0 && options.full_scan !== true) continue;
		const prompt = [
			REVIEW_RULES,
			"",
			`【人物】${node.name}`,
			`【现有档案】${describe_current(node)}`,
			"",
			"【本次事实】",
			related.length === 0 ? "（无）" : related.map((item) => render_fact(item.fact)).join("\n")
		].join("\n");
		const parsed = parse_facts_review(await options.complete(prompt));
		bad_lines += parsed.bad_lines.length;
		if (parsed.updates.length === 0) continue;
		const patch = patch_from_updates(parsed.updates);
		if (Object.keys(patch).length === 0) continue;
		graph.upsert_person({
			id: node.id,
			name: node.name,
			...patch,
			evidence: {
				date: all_facts[0]?.date ?? "",
				note: "逐节点审核"
			}
		});
		updated.push(node.name);
		probe("graph", "update.node_reviewed", {
			name: node.name,
			fields: Object.keys(patch).length
		});
	}
	for (const item of all_facts) {
		if (item.fact.kind !== "attribute") continue;
		const patch = patch_from_updates([{
			field: item.fact.field,
			value: item.fact.value
		}]);
		if (Object.keys(patch).length === 0) continue;
		const existing = graph.find(item.fact.name);
		graph.upsert_person({
			name: item.fact.name,
			...patch,
			...existing !== void 0 && typeof patch.name === "string" ? { id: existing.id } : {},
			evidence: {
				date: item.date,
				note: item.quote
			}
		});
		touched.add(item.fact.name);
	}
	let relations_created = 0;
	for (const item of all_facts) {
		if (item.fact.kind !== "relation") continue;
		if (graph.upsert_edge({
			from: item.fact.from,
			to: item.fact.to,
			label: item.fact.label,
			...is_address_pair(item.fact.from, item.fact.to) ? { kind: "address" } : {},
			evidence: {
				date: item.date,
				note: item.quote
			}
		}) !== void 0) relations_created += 1;
	}
	const known = candidate_names(graph);
	const fact_lines = all_facts.map((item) => render_fact(item.fact));
	const build_new = (lines) => [
		NEW_PERSON_RULES,
		"",
		"【已知人物名单】",
		render_candidates(known, 60),
		"",
		"【本次整理出的事实】",
		lines.length === 0 ? "（无）" : lines.join("\n")
	].join("\n");
	let kept = fact_lines;
	let new_prompt = build_new(kept);
	while (new_prompt.length > max_prompt_chars && kept.length > 0) {
		kept = kept.slice(1);
		new_prompt = build_new(kept);
		trimmed_prompts += 1;
	}
	const new_parsed = parse_facts(await options.complete(new_prompt));
	bad_lines += new_parsed.bad_lines.length;
	for (const fact of new_parsed.facts) {
		if (fact.kind === "person" && graph.find(fact.name) === void 0) {
			graph.upsert_person({
				name: fact.name,
				...fact.aliases === void 0 ? {} : { aliases: fact.aliases },
				evidence: {
					date: all_facts[0]?.date ?? "",
					note: "新增人物审核"
				}
			});
			created.push(fact.name);
		}
		if (fact.kind === "relation") {
			if (graph.upsert_edge({
				from: fact.from,
				to: fact.to,
				label: fact.label,
				evidence: {
					date: all_facts[0]?.date ?? "",
					note: "新增人物审核"
				}
			}) !== void 0) relations_created += 1;
		}
	}
	const result = {
		chunks: chunks.length,
		facts: all_facts.length,
		bad_lines,
		people_created: created,
		people_updated: updated,
		relations_created,
		unresolved: unresolved_total,
		merged_blocks: merged_count,
		trimmed_prompts
	};
	probe("graph", "update.done", { ...result });
	return result;
}
/** 组装"合并压缩"提示词：旧压缩块 + 当前块 → 一个新块。 */
function build_merge(gists, facts, chunk) {
	return [
		MERGE_RULES,
		"",
		"【旧的上下文提要】",
		gists.length === 0 ? "（无）" : gists.map((item) => `- ${item}`).join("\n"),
		"",
		"【旧的事实清单】",
		facts.length === 0 ? "（无）" : facts.join("\n"),
		"",
		"【新的一段聊天记录】",
		chunk.map((item) => format_line(item)).join("\n")
	].join("\n");
}
/**
* 组装"前文提要"段（滚动压缩上下文）。
*
* 这是防上下文丢失的关键：分块后"之前那个人"可能落在上一块，
* 只带上一块原文会撑爆显存，所以带的是**压缩形式**——
* 每块一句提要 + 已提取的单行事实 + 未解指代，全部是有界的小文本。
*/
function carry_section(gists, facts, unresolved) {
	const out = [];
	if (gists.length > 0) {
		out.push("", "【前文提要（前面几块的压缩要点，帮助判断指代）】");
		for (const gist of gists.slice(-8)) out.push(`- ${gist}`);
	}
	if (facts.length > 0) {
		out.push("", "【前面已提取的事实（不要重复输出这些）】");
		for (const fact of facts.slice(-40)) out.push(fact);
	}
	if (unresolved.length > 0) out.push("", "【前面未解开的指代】", render_pending(unresolved));
	return out;
}
/** 一条关系是不是"当面称呼"（两个中心人物之间）。 */
function is_address_pair(from, to) {
	const centers = [GRAPH_ASSISTANT_NAME, GRAPH_USER_NAME];
	return from !== to && centers.includes(from) && centers.includes(to);
}
/** 候选人物名单：图谱里的姓名 + 别名（别名也列出来，方便指代匹配）。 */
function candidate_names(graph) {
	const out = [];
	for (const node of graph.nodes()) {
		out.push(node.name);
		for (const alias of node.aliases ?? []) out.push(alias);
	}
	return out;
}
/** 按字符预算切块。 */
function chunk_records(records, budget) {
	const chunks = [];
	let current = [];
	let size = 0;
	for (const record of records) {
		const cost = record.text.length + 8;
		if (current.length > 0 && size + cost > budget) {
			chunks.push(current);
			current = [];
			size = 0;
		}
		current.push(record);
		size += cost;
	}
	if (current.length > 0) chunks.push(current);
	return chunks;
}
/** 取本块之前的若干行，作为跨块指代的上下文（有界）。 */
function tail_of(records, chunk, lines) {
	const first = records.indexOf(chunk[0]);
	if (first <= 0) return "";
	return records.slice(Math.max(0, first - lines), first).map((item) => format_line(item)).join("\n");
}
/**
* 一行聊天记录：说话人标签直接用图谱规范名。
*
* 用 `[主人]`/`[欣爱]` 而不是 `[用户]`/`[助手]`，模型看到的就是该输出的名字，
* 少一层映射就少一类错误（把说话人标记当人名建节点）。
*/
function format_line(item) {
	return `[${item.role === "user" ? GRAPH_USER_NAME : GRAPH_ASSISTANT_NAME}] ${item.text}`;
}
/** 把事实里的人名归一化（说话人别名 → 规范名）。 */
function canonicalize_fact(fact) {
	switch (fact.kind) {
		case "person": return {
			...fact,
			name: canonical_person_name(fact.name)
		};
		case "attribute": return {
			...fact,
			name: canonical_person_name(fact.name)
		};
		case "relation": return {
			...fact,
			from: canonical_person_name(fact.from),
			to: canonical_person_name(fact.to)
		};
		case "event": return {
			...fact,
			name: canonical_person_name(fact.name)
		};
		default: return fact;
	}
}
/** 事实的一句话表示（既作为证据，也用于后续调用）。 */
function render_fact(fact) {
	switch (fact.kind) {
		case "person": return `人物|${fact.name}${fact.aliases === void 0 ? "" : "|" + fact.aliases.join(",")}`;
		case "attribute": return `属性|${fact.name}|${fact.field}|${fact.value}`;
		case "relation": return `关系|${fact.from}|${fact.to}|${fact.label}`;
		case "event": return `事件|${fact.name}|${fact.date}|${fact.description}`;
		case "unresolved": return `未绑定|${fact.reference}|${fact.quote}`;
		case "gist": return `上文|${fact.text}`;
	}
}
/** 证据里的短引用。 */
function quote_of(fact) {
	return render_fact(fact).slice(0, 80);
}
/** 现有档案的可读描述（喂给审核调用）。 */
function describe_current(node) {
	const bits = [];
	if (node.aliases !== void 0 && node.aliases.length > 0) bits.push(`别名=${node.aliases.join(",")}`);
	if (node.birthday !== void 0) bits.push(`出生日期=${node.birthday}`);
	if (node.age !== void 0) bits.push(`年龄=${node.age}`);
	if (node.gender !== void 0) bits.push(`性别=${node.gender}`);
	if (node.status !== void 0) bits.push(`状态=${node.status}`);
	if (node.contacts !== void 0 && node.contacts.length > 0) bits.push(`联系方式=${node.contacts.join(",")}`);
	if (node.location !== void 0) bits.push(`所在地=${node.location}`);
	if (node.occupation !== void 0) bits.push(`职业=${node.occupation}`);
	if (node.family_summary !== void 0) bits.push(`家庭情况=${node.family_summary}`);
	if (node.notes !== void 0) bits.push(`备注=${node.notes}`);
	return bits.length === 0 ? "（空）" : bits.join("；");
}
/** 解析审核输出（`更新|字段|值`）。 */
function parse_facts_review(text) {
	const updates = [];
	const bad_lines = [];
	for (const raw of text.split("\n")) {
		const line = raw.trim().replace(/^[-*]\s*/, "");
		if (line === "" || line === "无" || line.startsWith("#")) continue;
		const parts = line.split("|").map((item) => item.trim());
		const field = parts[1] ?? "";
		if (parts[0] !== "更新" || field === "" || parts.length < 3) {
			bad_lines.push(line);
			continue;
		}
		updates.push({
			field,
			value: parts.slice(2).join("|")
		});
	}
	return {
		updates,
		bad_lines
	};
}
/** 把 `字段|值` 转成节点补丁；不认识的字段直接忽略（宁可少写也不写错）。 */
function patch_from_updates(updates) {
	const patch = {};
	for (const { field, value } of updates) {
		if (!is_known_attribute_field(field)) continue;
		const key = map_attribute_field(field);
		switch (key) {
			case "age": {
				const age = Number(value.replace(/[^\d]/g, ""));
				if (Number.isFinite(age) && age > 0 && age < 150) patch.age = age;
				break;
			}
			case "contacts":
				patch.contacts = value.split(/[,，、;；]/).map((item) => item.trim()).filter((item) => item !== "");
				break;
			case "aliases":
				patch.aliases = value.split(/[,，、;；]/).map((item) => item.trim()).filter((item) => item !== "");
				break;
			case "status":
				patch.status = /已故|去世|过世/.test(value) ? "deceased" : /失联|联系不上/.test(value) ? "lost_contact" : /在世|健在/.test(value) ? "alive" : "unknown";
				break;
			case "gender":
				patch.gender = /男/.test(value) ? "male" : /女/.test(value) ? "female" : "unknown";
				break;
			case "important_dates": {
				const match = /^(.+?)\s*[:：]?\s*(\d{4}-\d{2}-\d{2}|\d{2}-\d{2})$/.exec(value.trim());
				if (match !== null) patch.important_dates = [{
					label: match[1].trim(),
					date: match[2]
				}];
				break;
			}
			default: if (value.trim() !== "") patch[key] = value.trim();
		}
	}
	return patch;
}
//#endregion
//#region lib/types/memory/graphScheduler.js
/**
* 知识图谱的**空闲增量更新**调度器。
*
* 触发条件与时间线摘要一致：**距最后一条对话已空闲 N 小时**（默认 3 小时）。
* 空闲才更新有两个好处：不打扰正在进行的对话；且这时模型会在被打断后
* 被再次唤醒（Gemma 是懒加载 + 空闲自动卸显存的）。
*
* 与时间线的分工：
* - 时间线 = 逐句事实（权威，按天存）；
* - 图谱 = 从时间线里**抽取出来的人物与称呼**（可被查询、可递归多跳）。
* 所以图谱更新**读的是时间线**，天然与时间线同步，不需要各写一份。
*
* 增量：只处理"比上次图谱更新更新的记录"，避免反复消化同一批对话。
*
* @module dsh-webhook-weixin/graph-scheduler
*/
/**
* 启动图谱空闲更新调度器。
*
* @param options - 图谱、时间线、模型入口与阈值。
* @returns 句柄；调用方负责 `stop()`。
*/
function start_graph_scheduler(options) {
	const idle_ms = options.idle_ms ?? Math.max(1, Number(process.env.DSH_TIMELINE_IDLE_HOURS ?? "3")) * 36e5;
	const interval_ms = options.interval_ms ?? Math.max(1, Number(process.env.DSH_TIMELINE_CHECK_MINUTES ?? "5")) * 6e4;
	const max_days = options.max_days ?? 7;
	let busy = false;
	let stopped = false;
	const run_once = async (now = Date.now()) => {
		if (busy) {
			probe("graph", "scheduler.skipped_busy");
			return;
		}
		busy = true;
		try {
			const since = options.graph.last_update_ms() ?? 0;
			const done_ids = new Set(options.graph.last_update_ids());
			const all = options.timeline.list_days().slice(-max_days).flatMap((day) => options.timeline.read_day(day.date).entries);
			const records = all.filter((item) => item.ts > since || item.ts === since && !done_ids.has(item.id)).sort((a, b) => a.ts - b.ts).map((item) => ({
				date: new Date(item.ts).toISOString().slice(0, 10),
				role: item.role,
				text: item.text
			}));
			if (records.length === 0) return void 0;
			const last_ts = Math.max(...all.map((item) => item.ts), since);
			const ids_at_watermark = all.filter((item) => item.ts === last_ts).map((item) => item.id);
			if (now - last_ts < idle_ms) return void 0;
			probe("graph", "scheduler.started_update", {
				records: records.length,
				since
			});
			const result = await update_graph_from_records({
				graph: options.graph,
				complete: options.complete
			}, records);
			options.graph.set_last_update(last_ts, ids_at_watermark);
			options.on_update?.(result);
			return result;
		} catch (error) {
			probe("graph", "scheduler.failed", { error: String(error) });
			return;
		} finally {
			busy = false;
		}
	};
	const timer = setInterval(() => {
		run_once();
	}, interval_ms);
	timer.unref?.();
	probe("graph", "scheduler.started", {
		interval_ms,
		idle_ms,
		max_days
	});
	return {
		run_once,
		stop() {
			if (stopped) return;
			stopped = true;
			clearInterval(timer);
			probe("graph", "scheduler.stopped");
		}
	};
}
//#endregion
//#region lib/types/memory/gpuMonitor.js
/**
* 显存监视：Gemma 行动前先确认显卡是空闲的。
*
* 为什么需要：本地 Gemma（Q4_K_M + 视觉投影器 + 32k 上下文）醒来后要占约 5.3GB，
* 而这张卡只有 8GB。如果用户正好在跑游戏/训练/另一个模型，硬启动会把两边都拖垮
* （甚至 OOM）。后台任务**不急**（摘要在空闲时才跑），所以正确做法是
* **等显存空出来再启动**，而不是抢。
*
* 判定用 `nvidia-smi`；查不到就直接放行（不能因为探测失败就永远不跑模型）。
*
* @module dsh-webhook-weixin/gpu-monitor
*/
/**
* 读一次显存占用。
*
* @returns 快照；没有 nvidia-smi 或查询失败时返回 undefined（调用方应据此放行）。
*/
function gpu_memory() {
	try {
		const parts = (execFileSync("nvidia-smi", ["--query-gpu=memory.used,memory.total", "--format=csv,noheader,nounits"], {
			encoding: "utf8",
			timeout: 15e3,
			windowsHide: true
		}).trim().split("\n")[0] ?? "").split(",").map((item) => Number(item.trim()));
		const used = parts[0];
		const total = parts[1];
		if (used === void 0 || total === void 0) return void 0;
		if (!Number.isFinite(used) || !Number.isFinite(total)) return void 0;
		return {
			used_mb: used,
			total_mb: total,
			free_mb: total - used
		};
	} catch {
		return;
	}
}
/**
* 等到显存足够空闲；超时返回 false（调用方应放弃本轮，而不是硬上）。
*
* 探测不到显存信息时返回 true（放行）。
*/
async function wait_for_free_vram(options) {
	const min_free_mb = options?.min_free_mb ?? 5200;
	const timeout_ms = options?.timeout_ms ?? 6e5;
	const poll_ms = options?.poll_ms ?? 3e4;
	const label = options?.label ?? "gemma";
	if (min_free_mb <= 0) return true;
	const deadline = Date.now() + timeout_ms;
	let waited = false;
	for (;;) {
		const memory = gpu_memory();
		if (memory === void 0) {
			if (waited) probe("gpu", "vram.wait_skipped", {
				label,
				reason: "no_nvidia_smi"
			});
			return true;
		}
		if (memory.free_mb >= min_free_mb) {
			if (waited) probe("gpu", "vram.free_now", {
				label,
				free_mb: memory.free_mb
			});
			return true;
		}
		if (Date.now() >= deadline) {
			probe("gpu", "vram.wait_timeout", {
				label,
				free_mb: memory.free_mb,
				need_mb: min_free_mb,
				timeout_ms
			});
			return false;
		}
		if (!waited) {
			waited = true;
			probe("gpu", "vram.busy_wait", {
				label,
				free_mb: memory.free_mb,
				need_mb: min_free_mb,
				total_mb: memory.total_mb
			});
		}
		await new Promise((resolve) => setTimeout(resolve, Math.min(poll_ms, Math.max(0, deadline - Date.now()))));
	}
}
//#endregion
//#region lib/types/memory/gemmaGate.js
/**
* 本地 Gemma 的**全局并发闸门**。
*
* 为什么需要：时间线摘要、图谱抽取、图片描述是三条独立的后台链路，
* 各自维护 busy 标志——它们可以**同时**唤醒模型（审计实测并发峰值 2）。
* 本地 llama-server 只有一份 8GB 显存：并发调用会争显存、触发重复冷启动、
* 让本可以串行完成的活互相拖慢。
*
* 所以把"调用模型"这件事收敛到一个有界闸门里：默认**并发 1**，
* 排队超过上限就直接拒绝（后台任务宁可这一轮跳过，也不要无限堆积把内存吃满）。
*
* @module dsh-webhook-weixin/gemma-gate
*/
/**
* 创建闸门。
*
* @param options - 并发与排队上限。
*/
function create_gemma_gate(options) {
	const concurrency = Math.max(1, options?.concurrency ?? 1);
	const max_queue = Math.max(0, options?.max_queue ?? 20);
	const wait_timeout_ms = options?.wait_timeout_ms ?? 6e5;
	let active = 0;
	const queue = [];
	const release = () => {
		active -= 1;
		const next = queue.shift();
		if (next !== void 0) next();
	};
	return {
		async run(label, fn) {
			if (!await wait_for_free_vram({
				min_free_mb: options?.min_free_vram_mb ?? 5200,
				timeout_ms: options?.vram_wait_ms ?? 6e5,
				poll_ms: options?.vram_poll_ms ?? 3e4,
				label
			})) throw new Error(`显存一直被占用，暂缓「${label}」（等不到至少 ${options?.min_free_vram_mb ?? 5200}MB 空闲）。稍后会自动重试。`);
			if (options?.ensure_ready !== void 0) await options.ensure_ready();
			if (active >= concurrency) {
				if (queue.length >= max_queue) {
					probe("gemma", "gate.rejected", {
						label,
						active,
						queued: queue.length
					});
					throw new Error(`Gemma 调用排队已满（${max_queue}），本次「${label}」跳过。`);
				}
				await new Promise((resolve, reject) => {
					const timer = setTimeout(() => {
						const at = queue.indexOf(entry);
						if (at >= 0) queue.splice(at, 1);
						reject(/* @__PURE__ */ new Error(`等待 Gemma 闸门超时（${wait_timeout_ms}ms）：${label}`));
					}, wait_timeout_ms);
					const entry = () => {
						clearTimeout(timer);
						resolve();
					};
					queue.push(entry);
				});
			}
			active += 1;
			probe("gemma", "gate.enter", {
				label,
				active,
				queued: queue.length
			});
			try {
				return await fn();
			} finally {
				release();
				probe("gemma", "gate.leave", {
					label,
					active,
					queued: queue.length
				});
			}
		},
		stats: () => ({
			active,
			queued: queue.length
		})
	};
}
//#endregion
//#region lib/types/coordination/notifySettings.js
/**
* 通知开关：控制 codex / opencode 的"任务完成汇报"是否推送到微信。
*
* 存在状态库的 `meta` 表里（`notify.codex` / `notify.opencode`，值为 `1`/`0`），
* 所以**重启后依然生效**，不需要额外的配置文件。
*
* 为什么需要它：完成汇报是"外部发起的任务结束了"这类被动通知，
* 有时候用户在专心聊天、不想被一堆任务汇报打断，就需要能关掉；
* 但真实对话回复**不受这个开关影响**（那个永远要发）。
*
* @module dsh-webhook-weixin/notify-settings
*/
/** meta 键名。 */
function meta_key(target) {
	return `notify.${target}`;
}
/**
* 基于状态库创建通知开关。
*
* @param store - 微信状态库（复用 meta 表，避免再引入一份配置）。
*/
function create_notify_settings(store) {
	return {
		is_enabled(target) {
			return store.get_meta(meta_key(target)) !== "0";
		},
		set_enabled(target, enabled) {
			store.set_meta(meta_key(target), enabled ? "1" : "0");
		},
		snapshot() {
			return {
				codex: store.get_meta(meta_key("codex")) !== "0",
				opencode: store.get_meta(meta_key("opencode")) !== "0"
			};
		}
	};
}
//#endregion
//#region lib/types/coordination/notifyTools.js
/**
* 通知开关工具：让欣爱自己查/改 codex 与 opencode 的完成汇报是否推送。
*
* 一个工具两个动作（而不是两个工具）：模型在工具变多时更容易选错，
* 而"查状态"和"改开关"本来就是一回事的两面，合成一个入口更不容易用错。
*
* 只影响**被动通知**（外部发起的任务完成汇报）；真实对话回复不受影响。
*
* @module dsh-webhook-weixin/notify-tools
*/
const TARGETS = ["codex", "opencode"];
/** 解析目标；'all'/'both' 表示两个都算。 */
function parse_targets(raw) {
	const value = typeof raw === "string" ? raw.trim().toLowerCase() : "";
	if (value === "" || value === "all" || value === "both" || value === "全部") return [...TARGETS];
	const matched = TARGETS.filter((target) => value === target || value.includes(target));
	return matched.length === 0 ? void 0 : matched;
}
/**
* 在 agent 作用域注册通知开关工具。
*
* @param agent_ctx - agent 自己的 ctx。
* @param settings - 通知开关（读写状态库 meta）。
*/
function register_notify_tools(agent_ctx, settings) {
	agent_ctx.tools.register(defineTool({
		name: "notify_control",
		description: [
			"查看或设置\"任务完成汇报\"的推送开关（codex 与 opencode 各自独立）。",
			"",
			"动作：",
			"- status：查看当前两个开关是开还是关。",
			"- set：设置开关，需要同时给 target（codex / opencode / all）和 enabled（true/false）。",
			"",
			"什么时候该用：",
			"- 用户说\"别老是汇报了/关掉那个通知/太吵了\" → action=set, target=all, enabled=false；",
			"- 用户说\"把通知打开/汇报恢复一下\" → action=set, target=all, enabled=true；",
			"- 用户问\"通知开着吗\" → action=status。",
			"",
			"什么时候不该用：",
			"- 不要因为\"觉得吵\"就自己关掉通知——必须用户明确要求才改；",
			"- 这个开关只影响\"外部任务的完成汇报\"，不影响你正常的对话回复。"
		].join("\n"),
		parameters: {
			action: {
				type: "string",
				required: true,
				description: "status（查看）或 set（设置）。"
			},
			target: {
				type: "string",
				description: "set 时必填：codex / opencode / all。"
			},
			enabled: {
				type: "boolean",
				description: "set 时必填：true 打开、false 关闭。"
			}
		},
		output: {
			schema: {
				type: "object",
				additionalProperties: false,
				properties: {
					ok: {
						type: "boolean",
						required: true
					},
					text: {
						type: "string",
						required: true
					}
				}
			},
			render: (_args, value) => [{
				type: "text",
				text: String(value.text ?? "")
			}]
		},
		async execute(args) {
			const action = typeof args.action === "string" ? args.action.trim().toLowerCase() : "";
			const render = (state) => `codex 完成汇报：${state.codex ? "已开启" : "已关闭"}\nopencode 完成汇报：${state.opencode ? "已开启" : "已关闭"}`;
			if (action === "status" || action === "") return {
				ok: true,
				text: `当前通知开关：\n${render(settings.snapshot())}`
			};
			if (action !== "set") return {
				ok: false,
				text: `未知动作"${action}"；可用：status | set`
			};
			const targets = parse_targets(args.target);
			if (targets === void 0) return {
				ok: false,
				text: "target 必须是 codex / opencode / all 之一。"
			};
			if (typeof args.enabled !== "boolean") return {
				ok: false,
				text: "set 需要 enabled（true 打开、false 关闭）。"
			};
			for (const target of targets) settings.set_enabled(target, args.enabled);
			return {
				ok: true,
				text: `已${args.enabled ? "开启" : "关闭"}：${targets.join("、")}。\n${render(settings.snapshot())}`
			};
		}
	}));
}
//#endregion
//#region lib/types/memory/bridgeResultTools.js
/**
* codex-opencode 处理结果日志：按需取回"任务做完了但当时没能推给你"的结果。
*
* 为什么需要它：平台只允许在"用户刚发过消息"的窗口内发送（实测：旧 token / 无 token /
* 先调 notifystart 全都会 ret=-2）。所以当任务在用户不在微信时完成，结果推不出去；
* 与其反复重试或直接丢弃，不如**停放在队列里保留一段时间**，等用户主动问起再返回。
*
* 工具名用英文标识符（协议要求），但**说明里写明中文名**，界面与模型都能看懂。
*
* @module dsh-webhook-weixin/bridge-result-tools
*/
/** 单次返回给模型的最大字符数。 */
const MAX_CHARS$1 = 6e3;
function clip$1(text) {
	return text.length <= MAX_CHARS$1 ? text : `${text.slice(0, MAX_CHARS$1)}\n…（已截断，还有更多条，可再查）`;
}
/** 把时间戳转成"几分钟前"这种好读的说法。 */
function ago$1(ms) {
	const minutes = Math.round((Date.now() - ms) / 6e4);
	if (minutes < 1) return "刚刚";
	if (minutes < 60) return `${minutes} 分钟前`;
	return `${Math.round(minutes / 60)} 小时前`;
}
/**
* 注册"处理结果日志"工具。
*
* @param agent_ctx - agent 自己的 ctx。
* @param store - 微信状态库（停放结果就在 outbox 里）。
* @param retention_ms - 停放保留期，用于告诉模型结果会保留多久。
*/
function register_bridge_result_tools(agent_ctx, store, retention_ms = 6 * 36e5) {
	agent_ctx.tools.register(defineTool({
		name: "bridge_results",
		description: [
			"codex-opencode 处理结果日志：查看\"任务已完成、但当时没能发给你\"的处理结果。",
			"",
			"背景：微信只允许在你刚发过消息的那一小段时间内发送。所以你在忙别的事时，",
			`由 codex / opencode 跑完的任务结果会**停放在这里**（保留 ${Math.round(retention_ms / 36e5)} 小时），不会主动推给你。`,
			"",
			"什么时候该用：",
			"- 用户问\"刚才那个任务做完了吗 / 有什么结果 / 有没有什么要告诉我的\"；",
			"- 用户提到之前让 codex 或 opencode 做的事，而你不确定结果。",
			"",
			"什么时候不该用：",
			"- 用户没问、也没有相关话题时，不要主动去翻这个日志；",
			"- 这里只有\"结果\"，没有中间过程——中间过程不会推也不在这里。"
		].join("\n"),
		parameters: {
			limit: {
				type: "number",
				description: "最多返回多少条，默认 10。"
			},
			keyword: {
				type: "string",
				description: "可选：只返回包含该关键词的结果。"
			}
		},
		output: {
			schema: {
				type: "object",
				additionalProperties: false,
				properties: {
					ok: {
						type: "boolean",
						required: true
					},
					text: {
						type: "string",
						required: true
					}
				}
			},
			render: (_args, value) => [{
				type: "text",
				text: String(value.text ?? "")
			}]
		},
		async execute(args) {
			const limit = Math.max(1, Math.min(50, Number(args.limit) || 10));
			const keyword = typeof args.keyword === "string" ? args.keyword.trim() : "";
			let rows = store.parked_results(limit * 3);
			if (keyword !== "") rows = rows.filter((row) => row.text.includes(keyword));
			rows = rows.slice(0, limit);
			probe("bridge-results", "queried", {
				count: rows.length,
				keyword
			});
			if (rows.length === 0) return {
				ok: true,
				text: keyword === "" ? "处理结果日志里没有待取回的结果（说明该推的都已经发出去了）。" : `没有包含"${keyword}"的处理结果。`
			};
			const blocks = rows.map((row) => `### ${ago$1(row.created_at)}\n${row.text.trim()}`);
			return {
				ok: true,
				text: clip$1(`有 ${rows.length} 条待取回的处理结果：\n\n${blocks.join("\n\n")}`)
			};
		}
	}));
}
//#endregion
//#region lib/types/coordination/bridgeSessionTools.js
/**
* 工具 `bridge_sessions`：把本机 Codex / opencode 的**会话清单与聊天记录**摆给模型看。
*
* 存在的理由：用户会在微信里说"上次那个会话继续做""把那个项目的会话列出来"，
* 但模型此前只能看到"任务完成"的推送，看不到**会话本身**——于是没法远程指挥。
* 这个工具补上这一环：先列会话（带名字/目录/状态），再按 id 读聊天记录，
* 拿到 id 之后就能用 `codex` / `opencode` 工具续会话。
*
* 数据来源（都是**只读**打开，不会干扰正在跑的工具）：
* - Codex：`~/.codex/state_5.sqlite` 的 `threads` 表 + 每个会话的 rollout JSONL；
* - opencode：`~/.local/share/opencode/opencode.db` 的 `session` / `message` / `part` 三张表。
*
* @module dsh-webhook-weixin/bridge-session-tools
*/
/** 单次返回给模型的字符上限。 */
const MAX_CHARS = 6e3;
/** Codex home（与 codexWatcher 同一套解析顺序）。 */
function codex_home() {
	return process.env.CODEX_HOME ?? join(process.env.USERPROFILE ?? homedir(), ".codex");
}
/** opencode 数据目录（与 opencodeWatcher 同一套解析顺序）。 */
function opencode_data_dir() {
	return process.env.OPENCODE_DATA_DIR ?? join(process.env.USERPROFILE ?? homedir(), ".local", "share", "opencode");
}
/** 列出最近的 Codex 会话。 */
function list_codex_sessions(limit = 8) {
	const state_path = join(codex_home(), "state_5.sqlite");
	if (!existsSync(state_path)) return [];
	let db;
	try {
		db = new DatabaseSync(state_path, { readOnly: true });
		return db.prepare("SELECT id, rollout_path, cwd, title, updated_at FROM threads WHERE archived = 0 ORDER BY updated_at DESC LIMIT ?").all(limit).map((row) => {
			const rollout = String(row.rollout_path ?? "");
			const title = read_codex_messages(rollout, 12).filter((item) => item.role === "user").at(-1)?.text ?? String(row.title ?? "未命名任务");
			return {
				tool: "codex",
				id: String(row.id),
				title: title.slice(0, 80),
				workspace: normalize_path(String(row.cwd ?? "")),
				status: codex_lifecycle(rollout),
				updated_at: Number(row.updated_at ?? 0),
				rollout_path: rollout
			};
		});
	} catch (error) {
		probe("bridge-sessions", "codex_db_failed", { error: String(error) });
		return [];
	} finally {
		try {
			db?.close();
		} catch {}
	}
}
/** 从 rollout 尾部读最近的生命周期事件。 */
function codex_lifecycle(rollout_path) {
	if (rollout_path === "" || !existsSync(rollout_path)) return "记录缺失";
	const patterns = [
		["\"type\":\"task_complete\"", "已完成"],
		["\"type\":\"turn_aborted\"", "已中止"],
		["\"type\":\"task_started\"", "运行中"]
	];
	try {
		const size = statSync(rollout_path).size;
		const lines = readFileSync(rollout_path).subarray(Math.max(0, size - 512 * 1024)).toString("utf8").split("\n");
		for (let index = lines.length - 1; index >= 0; index -= 1) {
			const line = lines[index];
			for (const [marker, status] of patterns) if (line.includes(marker)) return status;
		}
	} catch {}
	return "未知";
}
/**
* 读一个 Codex 会话的聊天记录（按时间顺序返回最后 limit 条）。
*
* rollout 是 JSONL：用户与助手的真实消息都在 `response_item` / `message` 里，
* 其余是工具调用等内部事件，这里全部跳过（用户要看的是对话，不是内部轨迹）。
*/
function read_codex_messages(rollout_path, limit = 12) {
	if (rollout_path === "" || !existsSync(rollout_path)) return [];
	const out = [];
	try {
		const size = statSync(rollout_path).size;
		const text = readFileSync(rollout_path).subarray(Math.max(0, size - 4 * 1024 * 1024)).toString("utf8");
		for (const line of text.split("\n")) {
			if (!line.includes("\"role\":\"user\"") && !line.includes("\"role\":\"assistant\"")) continue;
			try {
				const parsed = JSON.parse(line);
				if (parsed.type !== "response_item" || parsed.payload?.type !== "message") continue;
				const role = parsed.payload.role;
				if (role !== "user" && role !== "assistant") continue;
				const body = (parsed.payload.content ?? []).map((part) => part.text ?? part.output_text ?? "").filter((item) => item.trim() !== "").join("\n").trim();
				if (body === "") continue;
				const clean = role === "user" ? clean_user_message(body) : body;
				if (clean === "") continue;
				out.push({
					role,
					text: clean,
					...parsed.timestamp === void 0 ? {} : { at: parsed.timestamp }
				});
			} catch {}
		}
	} catch (error) {
		probe("bridge-sessions", "codex_rollout_failed", { error: String(error) });
	}
	return out.slice(-limit);
}
/** 去掉 Windows 长路径前缀（`\\?\`）。 */
function normalize_path(path) {
	return path.startsWith("\\\\?\\") ? path.slice(4) : path;
}
/** 剥掉 Codex 给用户消息加的包装，只留真实请求。 */
function clean_user_message(message) {
	const marker = "## My request for Codex:";
	let text = message;
	if (text.includes(marker)) text = text.slice(text.indexOf(marker) + 24);
	text = text.trim();
	if (text.startsWith("<environment_context>") || text.startsWith("<permissions instructions>")) return "";
	return text;
}
/** 列出最近的 opencode 会话。 */
function list_opencode_sessions(limit = 8) {
	const db_path = join(opencode_data_dir(), "opencode.db");
	if (!existsSync(db_path)) return [];
	let db;
	try {
		db = new DatabaseSync(db_path, { readOnly: true });
		return db.prepare("SELECT id, directory, title, time_created, time_updated FROM session WHERE time_archived IS NULL ORDER BY time_updated DESC LIMIT ?").all(limit).map((row) => ({
			tool: "opencode",
			id: String(row.id),
			title: String(row.title ?? "").trim() || "(无标题)",
			workspace: String(row.directory ?? ""),
			status: "opencode 会话",
			updated_at: Number(row.time_updated ?? 0)
		}));
	} catch (error) {
		probe("bridge-sessions", "opencode_db_failed", { error: String(error) });
		return [];
	} finally {
		try {
			db?.close();
		} catch {}
	}
}
/** 读一个 opencode 会话的聊天记录（按时间顺序返回最后 limit 条）。 */
function read_opencode_messages(session_id, limit = 12) {
	const db_path = join(opencode_data_dir(), "opencode.db");
	if (session_id === "" || !existsSync(db_path)) return [];
	let db;
	const out = [];
	try {
		db = new DatabaseSync(db_path, { readOnly: true });
		const scan = Math.min(400, Math.max(limit * 8, 40));
		const messages = db.prepare("SELECT id, data FROM message WHERE session_id = ? ORDER BY rowid DESC LIMIT ?").all(session_id, scan);
		for (const message of messages.reverse()) {
			let role = "";
			try {
				const parsed = JSON.parse(message.data);
				role = String(parsed.role ?? "");
			} catch {}
			if (role !== "user" && role !== "assistant") continue;
			const parts = db.prepare("SELECT data FROM part WHERE message_id = ? ORDER BY rowid ASC").all(message.id);
			const texts = [];
			for (const part of parts) try {
				const parsed = JSON.parse(part.data);
				if (parsed.type === "text" && typeof parsed.text === "string" && parsed.text.trim() !== "") texts.push(parsed.text.trim());
			} catch {}
			if (texts.length === 0) continue;
			out.push({
				role,
				text: texts.join("\n")
			});
		}
	} catch (error) {
		probe("bridge-sessions", "opencode_messages_failed", { error: String(error) });
		return [];
	} finally {
		try {
			db?.close();
		} catch {}
	}
	return out.slice(-limit);
}
/** 相对时间（"12 分钟前"）。 */
function ago(ms) {
	if (!Number.isFinite(ms) || ms <= 0) return "时间未知";
	const minutes = Math.round((Date.now() - ms) / 6e4);
	if (minutes < 1) return "刚刚";
	if (minutes < 60) return `${minutes} 分钟前`;
	const hours = Math.round(minutes / 60);
	if (hours < 48) return `${hours} 小时前`;
	return `${Math.round(hours / 24)} 天前`;
}
function clip(text) {
	return text.length <= MAX_CHARS ? text : `${text.slice(0, MAX_CHARS)}\n…（已截断）`;
}
/** 列出会话的可读文本。 */
function render_sessions(sessions, tool) {
	const mine = sessions.filter((item) => item.tool === tool);
	if (mine.length === 0) return [`【${tool} 会话】没有找到会话记录。`];
	const lines = [`【${tool} 会话】最近 ${mine.length} 个：`];
	for (const item of mine) {
		const short = item.id.slice(0, 8);
		const name = item.workspace.split(/[\\/]/).filter(Boolean).at(-1) ?? "";
		lines.push(`- ${short}…（${item.id}）`);
		lines.push(`    任务：${item.title}`);
		lines.push(`    项目：${name || item.workspace}　状态：${item.status}　更新：${ago(item.updated_at)}`);
	}
	return lines;
}
/** 注册会话查询工具。 */
function register_bridge_session_tools(agent_ctx) {
	agent_ctx.tools.register(defineTool({
		name: "bridge_sessions",
		description: [
			"codex/opencode 会话记录：列出本机 Codex 与 opencode 的会话（带名字与项目），",
			"或读某个会话的聊天记录。用于**远程指挥**这些会话。",
			"",
			"两种用法：",
			"- 不填 session_id：列出两个工具的最近会话（含会话 id、任务名、项目、状态、更新时间）；",
			"- 填 session_id：读那个会话最近的聊天记录（用户说了什么、助手回了什么）。",
			"",
			"什么时候该用：",
			"- 用户问\"有哪些会话/上次那个任务叫什么/那个项目做到哪了\"；",
			"- 用户想继续某个任务，但没说清是哪个会话——先列出来让他确认；",
			"- 用户问\"你刚才在那个会话里说了什么/结果是什么\"。",
			"",
			"拿到 session_id 之后，可以用 `codex` / `opencode` 工具续那个会话（不要凭印象编造会话 id）。",
			"什么时候不该用：用户只是闲聊、或明确在说微信这边的对话时不要调用。"
		].join("\n"),
		parameters: {
			tool: {
				type: "string",
				description: "查哪个工具：codex | opencode | both（默认 both）。"
			},
			session_id: {
				type: "string",
				description: "要读聊天记录的会话 id（列表里那一长串）；不填就只列会话。"
			},
			limit: {
				type: "number",
				description: "最多几条，默认 8（列会话）/ 12（读记录）。"
			}
		},
		output: {
			schema: {
				type: "object",
				additionalProperties: false,
				properties: {
					ok: {
						type: "boolean",
						required: true
					},
					text: {
						type: "string",
						required: true
					}
				}
			},
			render: (_args, value) => [{
				type: "text",
				text: String(value.text ?? "")
			}]
		},
		async execute(args) {
			const tool = (args.tool ?? "both").trim().toLowerCase();
			const session_id = typeof args.session_id === "string" ? args.session_id.trim() : "";
			const limit = Number.isFinite(Number(args.limit)) && Number(args.limit) > 0 ? Math.min(30, Math.floor(Number(args.limit))) : session_id === "" ? 8 : 12;
			if (session_id !== "") {
				const codex_hit = list_codex_sessions(30).find((item) => item.id === session_id || item.id.startsWith(session_id));
				if (codex_hit !== void 0 && codex_hit.rollout_path !== void 0) return {
					ok: true,
					text: clip(render_messages("codex", codex_hit, read_codex_messages(codex_hit.rollout_path, limit)))
				};
				const opencode_hit = list_opencode_sessions(30).find((item) => item.id === session_id || item.id.startsWith(session_id));
				if (opencode_hit !== void 0) return {
					ok: true,
					text: clip(render_messages("opencode", opencode_hit, read_opencode_messages(opencode_hit.id, limit)))
				};
				return {
					ok: true,
					text: `没有找到会话"${session_id}"。可以不带 session_id 先列一遍（会话 id 很长，列表里给的是完整 id，请整串照抄）。`
				};
			}
			const sessions = [...tool === "opencode" ? [] : list_codex_sessions(limit), ...tool === "codex" ? [] : list_opencode_sessions(limit)];
			if (sessions.length === 0) return {
				ok: true,
				text: "本机没有找到 Codex / opencode 的会话记录。"
			};
			const lines = [];
			if (tool !== "opencode") lines.push(...render_sessions(sessions, "codex"));
			if (tool !== "codex") {
				if (lines.length > 0) lines.push("");
				lines.push(...render_sessions(sessions, "opencode"));
			}
			lines.push("", "（要看某个会话的聊天记录：把它的会话 id 传给本工具的 session_id。）");
			return {
				ok: true,
				text: clip(lines.join("\n"))
			};
		}
	}));
}
/** 渲染聊天记录。 */
function render_messages(tool, session, messages) {
	const name = session.workspace.split(/[\\/]/).filter(Boolean).at(-1) ?? session.workspace;
	const lines = [
		`【${tool} 会话 ${session.id}】`,
		`任务：${session.title}`,
		`项目：${name}　状态：${session.status}　更新：${ago(session.updated_at)}`,
		""
	];
	if (messages.length === 0) {
		lines.push("（这个会话里没有可读的对话内容）");
		return lines.join("\n");
	}
	for (const message of messages) {
		lines.push(`[${message.role === "user" ? "用户" : "助手"}] ${message.text}`);
		lines.push("");
	}
	return lines.join("\n").trimEnd();
}
//#endregion
//#region lib/types/coordination/externalRuns.js
/**
* 中断外部 agent 的**在跑进程**（opencode / codex）。
*
* ## 为什么需要
*
* 实测踩过：用户说"停止任务"，我们只 `agent.cancel()` 取消了模型那一轮，
* 但模型之前用 `opencode` 工具起的 `opencode run --auto` 子进程**继续跑了 22 分钟**——
* 工具调用因此一直不返回，那一轮永远收不了尾，用户只看到每 5 分钟一条"还在处理中"。
*
* 所以取消必须**连外部进程一起杀**：模型的 turn 是它的一部分，外部进程是另一部分。
*
* ## 为什么按进程名+命令行匹配，而不是记子进程句柄
*
* 子进程有两种来源：我们的桥（`/opencode` 聊天命令）和 harness 的 `opencode`/`codex`
* 工具包（模型自己调的）。后者不归我们管、也没把句柄交给我们，所以只能按
* "可执行文件名 + 命令行特征"去找。匹配规则刻意保守：
* - 必须有 `run` 子命令（`opencode run …` / `codex exec …`）；
* - **排除 `serve`**（那是常驻的 opencode server，杀了会把链路弄断）。
*
* @module dsh-webhook-weixin/external-runs
*/
/** 命令行特征：opencode/codex 的"跑一次任务"，排除常驻 server。 */
const RUN_PATTERN = "(opencode|codex).*(run|exec)\\b";
/**
* 中断所有正在跑的外部 agent 任务。
*
* @returns 杀掉的进程数与 pid；任何失败都只记在 `error` 里，不抛。
*/
function kill_external_runs() {
	if (process.platform !== "win32") return {
		killed: 0,
		pids: [],
		error: "非 Windows 平台未实现外部进程中断"
	};
	try {
		const pids = execFileSync("powershell", [
			"-NoProfile",
			"-Command",
			`Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match '${RUN_PATTERN}' -and \$_.Name -notmatch 'powershell|pwsh' } | ForEach-Object { \$_.ProcessId }`
		], {
			encoding: "utf8",
			windowsHide: true,
			timeout: 2e4
		}).split("\n").map((line) => Number(line.trim())).filter((value) => Number.isInteger(value) && value > 0);
		if (pids.length === 0) {
			probe("external-runs", "kill.none", {});
			return {
				killed: 0,
				pids: []
			};
		}
		for (const pid of pids) try {
			execFileSync("taskkill", [
				"/PID",
				String(pid),
				"/T",
				"/F"
			], {
				stdio: "ignore",
				windowsHide: true,
				timeout: 2e4
			});
		} catch {}
		probe("external-runs", "kill.done", { pids });
		return {
			killed: pids.length,
			pids
		};
	} catch (error) {
		probe("external-runs", "kill.failed", { error: String(error).slice(0, 160) });
		return {
			killed: 0,
			pids: [],
			error: String(error).slice(0, 160)
		};
	}
}
//#endregion
//#region lib/types/runtime/modelFailover.js
/**
* 模型故障转移：某个 API 失活（额度用完 / key 失效 / 模型下线）时，
* 自动换到系统里配置的**另一个** API 继续干活。
*
* ## 为什么需要
*
* 实测踩过：中转站的 key 用满 7 天额度后，每一次模型调用都 429，这一轮直接失败——
* 用户看到的是"一直处理中"然后什么都没有。系统里明明还配着豆包、千问的 key，
* 却只能靠人手动去控制台换。这个模块就是让"换"自动发生。
*
* ## 怎么换（不丢上下文、不重启）
*
* harness 的 `sessionController.selectModel({sessionId, provider, model})` 会把选择装到
* **下一个请求**上，并顺手保存成默认值——所以同一段对话可以无缝换模型。
* 本模块只负责"判断该不该换、换成谁、多久内别再试它"，副作用由连接器执行。
*
* ## 判定
*
* 只有**重试也没用**的错误才换：额度/限流、鉴权失败、模型不存在。
* 网络抖动、5xx 这类交给 harness 自己的重试策略（llm-retry），我们不要抢着换。
*
* @module dsh-webhook-weixin/model-failover
*/
/**
* 找到模型配置文件的真实路径。
*
* 它在**工作区的 `config/` 子目录**下（`<工作区>/config/modelConfig.json`）。
* 早先按 `<工作区>/modelConfig.json` 找过——读不到就静默变成空配置，
* 表现是"故障转移明明配了却说没有候选模型"（2026-09-22 实测踩过）。
*
* @param workspace - 工作区目录（连接器里的 PROJECT_CONFIG_DIR）。
* @returns 存在的那个路径；都不存在返回 undefined。
*/
function find_model_config(workspace) {
	return [join(workspace, "config", "modelConfig.json"), join(workspace, "modelConfig.json")].find((path) => existsSync(path));
}
/** 默认冷却：额度类 1 小时（日额度/滚动窗口有机会恢复），鉴权类 24 小时。 */
const DEFAULT_COOLDOWN_MS = 60 * 6e4;
const AUTH_COOLDOWN_MS = 1440 * 6e4;
/** 限流/并发类只冷几秒到几分钟——上游一空就能用，别把好模型长期晾着。 */
const RATE_LIMIT_COOLDOWN_MS = 3 * 6e4;
/** 这些 provider 是"本地/记忆"用途，不该被当成聊天模型的转移目标。 */
const NON_CHAT_PROVIDERS = new Set(["gemma-local"]);
/**
* 判断一条模型错误是不是"这个 API 不能用了"。
*
* @param message - 错误文本（含 HTTP 状态码与上游 message）。
* @returns 失活原因；不是致命错误就返回 undefined（交给 harness 重试）。
*/
function classify_llm_failure(message) {
	const text = message.toLowerCase();
	if (/限额|额度|quota|insufficient|欠费|余额不足/.test(text)) return "quota";
	if (/concurrent|too many requests|overload|temporarily unavailable|请稍后重试|并发|繁忙|(^|[^0-9])429([^0-9]|$)/.test(text)) return "rate_limit";
	if (/(^|[^0-9])40[13]([^0-9]|$)|unauthorized|forbidden|invalid.?api.?key|api.?key.*(invalid|无效|失效|不正确)|鉴权|认证失败|token.*(invalid|无效|过期)/.test(text)) return "auth";
	if (/(^|[^0-9])404([^0-9]|$)|model.*not.*found|no such model|does not exist|模型不存在|模型已下线/.test(text)) return "model_missing";
}
/** 冷却时间：限流最短、额度类用配置值、鉴权/模型缺失最长。 */
function cooldown_ms(reason, config) {
	if (reason === "rate_limit") return RATE_LIMIT_COOLDOWN_MS;
	return reason === "quota" ? config.cooldown_ms : AUTH_COOLDOWN_MS;
}
/** 从 modelConfig.json 的内容解析故障转移配置。 */
function parse_failover_config(config) {
	const raw = config.failover ?? {};
	const enabled = raw.enabled !== false;
	const cooldown = Number(raw.cooldownMs);
	const known = (provider, model) => {
		const entry = (config.providers ?? {})[provider];
		if (entry === void 0) return false;
		return (entry.models ?? []).some((item) => item.id === model);
	};
	const explicit = (raw.order ?? []).filter((item) => typeof item?.provider === "string" && typeof item?.model === "string" && item.provider !== "" && item.model !== "").map((item) => ({
		provider: String(item.provider),
		model: String(item.model)
	})).filter((item) => known(item.provider, item.model));
	const derived = [];
	const push = (candidate) => {
		if (derived.some((item) => item.provider === candidate.provider && item.model === candidate.model)) return;
		derived.push(candidate);
	};
	const default_model = config.defaultModel;
	if (default_model?.provider !== void 0 && default_model?.model !== void 0) push({
		provider: default_model.provider,
		model: default_model.model
	});
	for (const [provider, entry] of Object.entries(config.providers ?? {})) {
		if (NON_CHAT_PROVIDERS.has(provider)) continue;
		if (entry.apiKey === void 0 || entry.apiKey === "") continue;
		for (const model of entry.models ?? []) if (typeof model.id === "string" && model.id !== "") push({
			provider,
			model: model.id
		});
	}
	return {
		enabled,
		order: explicit.length > 0 ? explicit : derived,
		cooldown_ms: Number.isFinite(cooldown) && cooldown > 0 ? cooldown : DEFAULT_COOLDOWN_MS
	};
}
/** 下一个候选：从 `current` 之后顺着找，跳过失活的（不回头，避免来回横跳）。 */
function next_alive_candidate(order, current, is_dead) {
	const start = current === void 0 ? 0 : Math.max(0, order.findIndex((item) => item.provider === current.provider && item.model === current.model) + 1);
	for (let index = start; index < order.length; index += 1) {
		const candidate = order[index];
		if (!is_dead(candidate)) return candidate;
	}
	for (let index = 0; index < start && index < order.length; index += 1) {
		const candidate = order[index];
		if (!is_dead(candidate)) return candidate;
	}
}
/**
* 内存里的模型健康表（按 `provider/model` 记冷却）。
*
* 只放在内存：进程重启后重新试探是合理的（额度可能已经恢复），
* 也不需要把"某个 key 挂了"这种瞬时状态写进磁盘。
*/
var ModelHealth = class ModelHealth {
	now;
	dead = /* @__PURE__ */ new Map();
	constructor(now = () => Date.now()) {
		this.now = now;
	}
	static key(candidate) {
		return `${candidate.provider}/${candidate.model}`;
	}
	mark_dead(candidate, reason, detail, ms) {
		const entry = {
			until: this.now() + ms,
			reason,
			detail: detail.slice(0, 200),
			at: this.now()
		};
		this.dead.set(ModelHealth.key(candidate), entry);
		return entry;
	}
	mark_alive(candidate) {
		this.dead.delete(ModelHealth.key(candidate));
	}
	is_dead(candidate) {
		const entry = this.dead.get(ModelHealth.key(candidate));
		if (entry === void 0) return false;
		if (entry.until <= this.now()) {
			this.dead.delete(ModelHealth.key(candidate));
			return false;
		}
		return true;
	}
	/** 当前失活的模型（供控制台/诊断展示）。 */
	snapshot() {
		const out = [];
		for (const [key, entry] of this.dead) {
			if (entry.until <= this.now()) continue;
			const [provider, ...rest] = key.split("/");
			out.push({
				provider: provider ?? "",
				model: rest.join("/"),
				...entry
			});
		}
		return out;
	}
};
//#endregion
//#region lib/types/memory/gemmaSummarizer.js
/**
* 用本地 Gemma（llama.cpp llama-server）生成时间线摘要。
*
* 为什么是本地：摘要只需"读懂一天对话并写几条要点"，Gemma 实测一整天才 ~10 秒，
* 且免费、不占云端配额。缺点是上下文 32k、长输入会变概括，所以这里对超长的一天
* 自动走**分块 map-reduce**（分块小结 → 合并）。
*
* 显存是懒加载的：llama-server 用 `--sleep-idle-seconds` 启动，空闲后自动把模型
* 从显存卸下（实测 4.4GB → 0.8GB），下次请求自动唤醒（约 18 秒）。
* 因此这里的超时必须给足，覆盖"唤醒 + 生成"。
*
* @module dsh-webhook-weixin/gemma-summarizer
*/
/**
* 摘要提示词（A 段：规则；数据单独放在 B 段）。
*
* 这里是 A/B 实测出来的版本，改动都有依据：
* - **A/B 分离**（规则块 + 【数据】块）：混在一起时模型会把规则当数据读。
* - **给"完成"立唯一判据**：旧版把"已设置提醒"误判成"已完成"（实测 3 次里错 2 次），
*   所以现在显式列举什么算完成、并**明确否定**"设置提醒/安排时间/记下来"。
* - **完成标记只允许出现在"事实"行**：待办行禁止出现任何完成字样，
*   这样语义上不可能再把待办标成完成。
* 实测（同一份带陷阱的评测集，各跑 3 次）：误标 2/3 → 0/3；完成标记 1/3 → 3/3。
*/
const INSTRUCTIONS = [
	"【角色】你是长期记忆整理器。输入是某一天\"用户\"与\"助手\"的对话记录。",
	"【输出格式】",
	"- 中文，最多 6 条，每条一行，以 \"- \" 开头",
	"- 每行以类别开头，类别只能是：事实 / 待办 / 偏好",
	"- 只输出这些行，不要标题、前言、结语、解释",
	"【完成标记的唯一判据】",
	"A. 只有\"记录里明确说了这件事已经做完\"才能标\"（已完成）\"：助手的完成声明（改好了/已发出/测试通过/已关闭），或用户的完成声明（已经交了/已经弄完了/不用了）。",
	"B. \"设置提醒\"\"安排时间\"\"约定日期\"\"记下来\"一律**不是**完成，必须写成待办。",
	"C. 未完成的、正在进行中的、未来的事情，一律**不得**出现\"（已完成）\"三个字。",
	"【逐行规则】",
	"1. 已完成的事项写成：\"- 事实：<事项>（已完成）\"；完成标记**只允许出现在 事实 行**。",
	"2. 待办写成：\"- 待办：<事项>（<时间，若有>）\"，待办行**禁止**出现任何完成字样。",
	"3. 偏好写成：\"- 偏好：<内容>\"；用户发来的图片若写了内容描述，可写成 \"- 事实：用户发来一张照片（<描述要点>）\"。",
	"4. 每条必须能对应到记录原话；对应不上就不写。禁止推断、补常识、评价、编造。",
	"5. 时间口径保留记录原话；记录未给具体日期时不要自己换算。",
	"6. 不写寒暄、情绪、客套话。"
].join("\n");
/** 把一天的记录渲染成给模型看的文本（含媒体描述，让摘要能提到"发了什么图"）。 */
function render_entries(entries) {
	return entries.map((item) => {
		const head = `[${item.role === "user" ? "用户" : "助手"}] ${item.text.trim()}`;
		const media = (item.attachments ?? []).map((attach) => `  [图片${attach.name === void 0 ? "" : " " + attach.name}] ${attach.description ?? "(无描述)"}`).join("\n");
		return media === "" ? head : `${head}\n${media}`;
	}).join("\n");
}
/** 按字符预算把记录切成若干块（至少一块）。 */
function chunk_entries(entries, budget) {
	const chunks = [];
	let current = [];
	let size = 0;
	for (const item of entries) {
		const cost = item.text.length + 16;
		if (current.length > 0 && size + cost > budget) {
			chunks.push(current);
			current = [];
			size = 0;
		}
		current.push(item);
		size += cost;
	}
	if (current.length > 0) chunks.push(current);
	return chunks;
}
/**
* 创建 Gemma 摘要器。
*
* @param options - 端点与生成参数。
* @returns 可直接交给 `TimelineStore.summarize_due()` 的摘要器。
*/
function create_gemma_summarizer(options) {
	const base_url = (options?.base_url ?? process.env.GEMMA_LOCAL_BASE_URL ?? "http://127.0.0.1:8080/v1").replace(/\/$/, "");
	const model = options?.model ?? process.env.DSH_GEMMA_MODEL ?? "gemma-4-E4B-it-Q4_K_M";
	const max_tokens = options?.max_tokens ?? 600;
	const timeout_ms = options?.timeout_ms ?? Number(process.env.SYNLOVE_LLM_TIMEOUT_MS ?? "300000");
	const chunk_chars = options?.chunk_chars ?? 24e3;
	const fetch_impl = options?.fetch_impl ?? fetch;
	const complete = options?.complete ?? (async (prompt, tokens) => {
		const controller = new AbortController();
		const timer = setTimeout(() => controller.abort(), timeout_ms);
		try {
			const response = await fetch_impl(`${base_url}/chat/completions`, {
				method: "POST",
				redirect: "error",
				headers: {
					"Content-Type": "application/json",
					...process.env[process.env.SYNLOVE_LLM_API_KEY_ENV ?? "SYNLOVE_LLM_API_KEY"] ? { Authorization: `Bearer ${process.env[process.env.SYNLOVE_LLM_API_KEY_ENV ?? "SYNLOVE_LLM_API_KEY"]}` } : {}
				},
				body: JSON.stringify({
					model,
					messages: [{
						role: "user",
						content: prompt
					}],
					temperature: .3,
					max_tokens: tokens,
					stream: false
				}),
				signal: controller.signal
			});
			if (!response.ok) throw new Error(`gemma HTTP ${response.status}`);
			const parsed = await response.json();
			const text = parsed.choices?.[0]?.message?.content ?? "";
			probe("timeline", "gemma.completion", {
				model,
				prompt_tokens: parsed.usage?.prompt_tokens,
				completion_tokens: parsed.usage?.completion_tokens
			});
			return text.trim();
		} finally {
			clearTimeout(timer);
		}
	});
	return async (request) => {
		const rendered = render_entries(request.entries);
		const previous = "";
		if (rendered.length <= chunk_chars) return {
			text: await complete(`${INSTRUCTIONS}${previous}\n\n【数据】\n日期：${request.date}\n对话记录：\n${rendered}`, max_tokens),
			model
		};
		const chunks = chunk_entries(request.entries, chunk_chars);
		probe("timeline", "gemma.map_reduce", {
			date: request.date,
			chunks: chunks.length,
			chars: rendered.length
		});
		const partials = [];
		for (const [index, chunk] of chunks.entries()) {
			const text = await complete(`${INSTRUCTIONS}\n\n【数据】\n这是当天第 ${index + 1}/${chunks.length} 段记录，请只整理这一段：\n${render_entries(chunk)}`, 300);
			partials.push(`【第 ${index + 1} 段】\n${text}`);
		}
		return {
			text: await complete(`${INSTRUCTIONS}${previous}\n\n【数据】\n下面是同一天各段的小结，请合并成一份当天摘要（格式同上，不要照抄段落编号）：\n\n${partials.join("\n\n")}`, max_tokens),
			model
		};
	};
}
//#endregion
//#region lib/types/memory/memoryModel.js
/**
* 记忆更新的"模型客户端"：本地 Gemma 或云端模型，二者可切换。
*
* 为什么需要可切换：本地 Gemma 免费、但要占约 5.3GB 显存（这张卡只有 8GB）。
* 用户如果在跑游戏/训练，等显存空出来可能要等很久；而记忆更新**不是急事**，
* 但也不能永远不做。所以给一条"直接用云端模型"的路：不占显存，立刻能跑。
*
* 两条路都是 OpenAI 兼容协议，所以这里只实现一个调用器，靠参数区分：
* - 本地：base_url 指向 llama-server，不需要 key；
* - 云端：base_url / model / key 从 `config/modelConfig.json` 里取（唯一配置源）。
*
* @module dsh-webhook-weixin/memory-model
*/
/** 默认的云端模型（配置里没有指定时的兜底）。 */
const DEFAULT_CLOUD_MODEL = "volcengine/doubao-seed-2-0-lite-260215";
/** 项目里 modelConfig.json 的位置（本包 → 项目根 → config）。 */
function default_config_path() {
	return join(process.env.DSH_WEIXIN_WORKSPACE ?? process.cwd(), "config", "modelConfig.json");
}
/**
* 从配置解析云端目标；解析不出来返回 undefined。
*
* 密钥现在单独放在与 modelConfig.json **同目录的 secrets.json**（见项目 README/启动器说明），
* 所以这里在两个文件里找 apiKey：模型定义读 modelConfig，密钥读 secrets。
*
* @param config_path - modelConfig.json 的路径（secrets.json 按同目录推导）。
* @param cloud_model - `provider/model` 形式的目标。
*/
function resolve_cloud_target(config_path, cloud_model) {
	try {
		const config = JSON.parse(readFileSync(config_path, "utf8"));
		const [provider_id, model_id] = cloud_model.split("/");
		const provider = config.providers?.[provider_id ?? ""];
		if (provider === void 0) return void 0;
		const model = model_id !== void 0 && model_id !== "" ? model_id : provider.models?.[0]?.id;
		if (model === void 0 || provider.baseURL === void 0) return void 0;
		const api_key = provider.apiKey ?? read_secret_api_key(config_path, provider_id ?? "");
		return {
			base_url: provider.baseURL.replace(/\/$/, ""),
			model,
			...api_key === void 0 ? {} : { api_key }
		};
	} catch {
		return;
	}
}
/** 从 `secrets.json`（与配置文件同目录）读某个 provider 的 apiKey；读不到返回 undefined。 */
function read_secret_api_key(config_path, provider_id) {
	try {
		const secrets_path = join(dirname(config_path), "secrets.json");
		if (!existsSync(secrets_path)) return void 0;
		const key = JSON.parse(readFileSync(secrets_path, "utf8")).providers?.[provider_id]?.apiKey;
		return typeof key === "string" && key !== "" ? key : void 0;
	} catch {
		return;
	}
}
/**
* 创建记忆模型客户端。
*
* @param options - 引擎选择与端点参数。
*/
function create_memory_model(options) {
	const timeout_ms = options?.timeout_ms ?? Number(process.env.SYNLOVE_LLM_TIMEOUT_MS ?? "300000");
	const fetch_impl = options?.fetch_impl ?? fetch;
	const engine = options.engine;
	const config_path = options.config_path ?? default_config_path();
	let base_url;
	let model;
	let api_key;
	let label;
	if (engine === "cloud") {
		const wanted = options.cloud_model?.trim() || DEFAULT_CLOUD_MODEL;
		const target = resolve_cloud_target(config_path, wanted);
		if (target === void 0) {
			probe("memory-model", "cloud.resolve_failed", {
				wanted,
				config_path
			});
			base_url = (options.gemma_base_url ?? process.env.GEMMA_LOCAL_BASE_URL ?? "http://127.0.0.1:8080/v1").replace(/\/$/, "");
			model = options.gemma_model ?? process.env.DSH_GEMMA_MODEL ?? "gemma-4-E4B-it-Q4_K_M";
			api_key = process.env[process.env.SYNLOVE_LLM_API_KEY_ENV ?? "SYNLOVE_LLM_API_KEY"];
			label = `本地 Gemma（云端 ${wanted} 解析失败，已回退）`;
		} else {
			base_url = target.base_url;
			model = target.model;
			api_key = target.api_key;
			label = `云端 ${wanted}`;
		}
	} else {
		base_url = (options.gemma_base_url ?? process.env.GEMMA_LOCAL_BASE_URL ?? "http://127.0.0.1:8080/v1").replace(/\/$/, "");
		model = options.gemma_model ?? process.env.DSH_GEMMA_MODEL ?? "gemma-4-E4B-it-Q4_K_M";
		api_key = process.env[process.env.SYNLOVE_LLM_API_KEY_ENV ?? "SYNLOVE_LLM_API_KEY"];
		label = `本地 Gemma（${model}）`;
	}
	/** 统一的一次 chat 调用。 */
	const chat = async (content, max_tokens) => {
		const controller = new AbortController();
		const timer = setTimeout(() => controller.abort(), timeout_ms);
		try {
			const response = await fetch_impl(`${base_url}/chat/completions`, {
				redirect: "error",
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					...api_key === void 0 ? {} : { Authorization: `Bearer ${api_key}` }
				},
				body: JSON.stringify({
					model,
					messages: [{
						role: "user",
						content
					}],
					temperature: .2,
					max_tokens,
					stream: false
				}),
				signal: controller.signal
			});
			if (!response.ok) throw new Error(`记忆模型 HTTP ${response.status}（${label}）`);
			const parsed = await response.json();
			probe("memory-model", "completion", {
				label,
				prompt_tokens: parsed.usage?.prompt_tokens,
				completion_tokens: parsed.usage?.completion_tokens
			});
			return parsed.choices?.[0]?.message?.content ?? "";
		} finally {
			clearTimeout(timer);
		}
	};
	return {
		label,
		complete: (prompt, max_tokens) => chat(prompt, max_tokens ?? 1200),
		describe_image: async (input) => {
			return chat([{
				type: "text",
				text: "用一句中文描述这张图片的主要内容（谁/什么、在哪、在做什么、明显的文字）。只输出这一句话，不要评价、不要客套。"
			}, {
				type: "image_url",
				image_url: { url: `data:${input.media_type};base64,${Buffer.from(input.data).toString("base64")}` }
			}], 200);
		}
	};
}
//#endregion
//#region lib/types/memory/timelineScheduler.js
/**
* 时间线摘要的**低频**定时器。
*
* 刻意独立于 iLink 轮询：
* - 轮询（~15s）是**接收**通道，属于微信协议的一部分；
* - 这里的检查是**纯本地**操作（读索引/记录文件），一个微信接口都不调。
* 两者解耦后，"摘要调度"再怎么改也不会影响微信链路。
*
* 频率也不需要高：触发条件是"某个日期已空闲 ≥3 小时"，所以每 5 分钟看一眼，
* 最多只会让摘要晚 5 分钟，完全够用。
*
* @module dsh-webhook-weixin/timeline-scheduler
*/
/**
* 启动摘要调度器。
*
* @param options - 存储与摘要器。
* @returns 句柄；调用方负责在卸载时 `stop()`。
*/
function start_timeline_scheduler(options) {
	const interval_ms = options.interval_ms ?? Math.max(1, Number(process.env.DSH_TIMELINE_CHECK_MINUTES ?? "5")) * 6e4;
	let busy = false;
	let stopped = false;
	const run_once = async (now = Date.now()) => {
		if (busy) {
			probe("timeline", "check.skipped_busy");
			return [];
		}
		busy = true;
		try {
			const due = options.store.days_needing_summary(now);
			if (due.length === 0) return [];
			probe("timeline", "check.due", { dates: due });
			const updated = await options.store.summarize_due(options.summarizer, now);
			if (updated.length > 0) options.on_update?.(updated);
			return updated;
		} finally {
			busy = false;
		}
	};
	const timer = setInterval(() => {
		run_once();
	}, interval_ms);
	timer.unref?.();
	probe("timeline", "scheduler.started", { interval_ms });
	return {
		run_once,
		stop() {
			if (stopped) return;
			stopped = true;
			clearInterval(timer);
			probe("timeline", "scheduler.stopped");
		}
	};
}
//#endregion
//#region lib/types/memory/sharedMemory.js
/**
* 进程内共享的长期记忆套件（时间线 + 图谱）。
*
* 背景：记忆引擎（timeline/graph）只在微信连接器里实例化。QQ 通道也要
* 落档和读图，但又不能各起一套 store（会双写同一目录）。所以由微信连接器
* 在构造完成后 `set_shared_memory` 注册一份，QQ 通道用 `get_shared_memory`
* 取同一实例：QQ 只负责「写记录」和「读查询工具」，摘要/图谱更新仍由
* 微信连接器里的调度器统一做。
*
* @module dsh-webhook-weixin/shared-memory
*/
let suite;
/** 微信连接器构造完成后注册（幂等，后写覆盖）。 */
function set_shared_memory(next) {
	suite = next;
}
/** 取当前共享套件；微信连接器尚未就绪时为 undefined。 */
function get_shared_memory() {
	return suite;
}
//#endregion
//#region lib/types/runtime/weixinConnector.js
/**
* 微信连接器：把传输层、入站协调器、出站协调器与生命周期装配到一起。
*
* 重构后的职责边界（设计文档 §3.1）：
*   WeixinIngress       只拉取/规范化
*   InboundCoordinator  幂等、路由、控制、投递、恢复
*   OutboundCoordinator 唯一发送队列（含心跳与恢复）
*   本文件              只做装配、生命周期与 assistant-stream 桥接
*
* 本文件**不再**维护：delivery 去重、user→session 路由、回复缓冲、发送队列、per-session 心跳。
* @module dsh-webhook-weixin/connector
*/
/** 项目目录（启动器注入）；用它定位 `config/modelConfig.json` 等配置文件。 */
const PROJECT_CONFIG_DIR = process.env.DSH_WEIXIN_WORKSPACE ?? process.cwd();
/** 探测某个本地端口是否在监听（用于判断 Gemma 服务是否就绪）。 */
function port_open(port) {
	return new Promise((resolve) => {
		const socket = createConnection({
			port,
			host: "127.0.0.1"
		});
		const finish = (value) => {
			socket.destroy();
			resolve(value);
		};
		socket.setTimeout(1500);
		socket.once("connect", () => finish(true));
		socket.once("timeout", () => finish(false));
		socket.once("error", () => finish(false));
	});
}
/** 退避序列（毫秒）。 */
const BACKOFF_STEPS = [
	1e3,
	2e3,
	5e3,
	1e4,
	2e4,
	3e4
];
/**
* 思考摘要推送的最小间隔（毫秒）。
*
* 旧逻辑是"每 2 次工具调用推一次"，但很多任务只调 1 次工具（甚至 0 次），
* 于是用户全程看不到任何进度——明明模型一直在输出思考。
* 改成按时间驱动：只要距上次推送超过该间隔，就把新累积的思考摘要发一条。
* 可用 DSH_WEIXIN_PROGRESS_INTERVAL_MS 调整；设为 0 关闭。
*/
const PROGRESS_MIN_INTERVAL_MS = Number(process.env.DSH_WEIXIN_PROGRESS_INTERVAL_MS ?? "25000");
/** 摘要太短就不值得单独发一条（避免刷屏）。 */
const PROGRESS_MIN_CHARS = 12;
/**
* turn 绑定的有效期（毫秒）。
* 超过就认为是"重启前遗留的绑定"并强制重绑：turn 号会重复，但 context_token 可能已过期，
* 复用旧 token 会让回复被判 `prepare failed`。
*/
const BINDING_TTL_MS = Number(process.env.DSH_WEIXIN_BINDING_TTL_MS ?? String(30 * 6e4));
/**
* agent 报错后等多久没有恢复，就把这一轮按失败收尾（毫秒）。
*
* 取 60 秒：harness 的 llm-retry 是"秒级退避重试"，重试成功的话新分片很快就会到；
* 60 秒还没有任何输出，基本就是额度/鉴权这类**重试也没用**的错误，早点告诉用户。
*/
const AGENT_ERROR_GRACE_MS = Number(process.env.DSH_WEIXIN_AGENT_ERROR_GRACE_MS ?? "60000");
/**
* 状态库里"这一轮还在进行中"的标记（`inflight_turn:<session_id>` → {turn, delivery_id, at}）。
*
* 轮次开始写；**正常结束或用户取消时删**。进程被强杀（重启/崩溃）时会留下残留记录 ——
* 启动时用它认出"哪一轮没跑完"，把那条 delivery 退回 received 让恢复流程重投。
* 否则 delivery 停在 injected（终态），恢复流程不看它，用户就只看到"发出去了没回音"。
* 前缀常量本身定义在 state/weixinStateStore（取消路径也要用，避免两处字符串漂移）。
*/
/** 补跑时的最小冷却：避免把"刚起 5 秒的一轮"误判成被杀掉的。 */
const INFLIGHT_RESCUE_MIN_AGE_MS = 5e3;
/**
* 账号指纹：给日志/租约用的**不可逆**身份标识。
*
* 以前用的是 `token.slice(0, 16)`——那是 token 的明文片段，会出现在探针日志、
* 租约标记文件和启动日志里，而它们都在项目目录内（会被一起拷走）。
* 现在改成 sha256 前 12 位：同一账号稳定可区分，但反推不出 token。
*
* @param token - 微信 bot token；缺失时返回 `'unknown'`。
*/
function account_fingerprint(token) {
	if (token === void 0 || token === "") return "unknown";
	return "acct-" + createHash("sha256").update(token).digest("hex").slice(0, 12);
}
/** 读一个 JSON 文件；读不到或坏了返回空对象（调用方都按"缺省"处理）。 */
function read_json_file(path) {
	try {
		return JSON.parse(readFileSync(path, "utf8"));
	} catch {
		return {};
	}
}
/** 微信连接器：装配 + 生命周期。 */
var WeixinConnector = class {
	store;
	turn_context;
	ingress;
	inbound;
	outbound;
	host;
	lease;
	client;
	/** Codex 远程桥（`/codex` 指令）。 */
	codex_bridge;
	/** Codex 任务完成监视器：任务一结束就主动推微信。 */
	codex_watcher;
	/** opencode 远程桥（`/opencode` 指令）。 */
	opencode_bridge;
	/** opencode 完成监视器：只汇报外部发起的任务。 */
	opencode_watcher;
	controller = new AbortController();
	stream_disposers = /* @__PURE__ */ new Map();
	stream_agents = /* @__PURE__ */ new Map();
	observation_disposers;
	/** 每个 Stream 的工具调用计数（用于"每 N 次工具调用推一次摘要"）。 */
	tool_calls_seen = /* @__PURE__ */ new Map();
	/**
	* 每个 Session 当前所在的 turn。
	* 只有 `start` 帧带 `turn`；`chunk`/`end` 帧不带，因此必须记住最近一次的 turn，
	* 否则 turn context 会全部绑到 0（绑定/查询错位）。
	*/
	current_turn = /* @__PURE__ */ new Map();
	/**
	* 待定的"这一轮失败了"收尾定时器（agent 报错后等宽限期，没有恢复就收尾）。
	* 没有它的话，模型持续报错时这一轮会永远停在"处理中"。
	*/
	turn_failure_timers = /* @__PURE__ */ new Map();
	/** 当前在用的模型（故障转移会改它；初值来自配置）。 */
	current_model = {
		provider: "",
		model: ""
	};
	/** 模型健康表：失活的 API 在冷却期内不再被选。 */
	model_health = new ModelHealth();
	/** 每个会话最近一条用户消息（换模型后要重做它）。 */
	last_user_message = /* @__PURE__ */ new Map();
	/** 本轮是否报过错（用于"整轮失败却一个字都没发"时补一条说明）。 */
	turn_error = /* @__PURE__ */ new Map();
	/** 本轮有没有成功发出过正文。 */
	turn_visible = /* @__PURE__ */ new Set();
	/**
	* 正在压缩上下文的会话（`session_id`）。
	*
	* 压缩要读完整段历史（实测 1–3 分钟），这期间**不应该再往会话里塞新消息**：
	* 否则新消息会和压缩抢同一个会话、甚至被压缩吞掉。做法是期间的新消息只入库不投递
	* （`handle_batch(..., { hold: true })`），压缩结束后由恢复流程统一重投。
	*/
	compacting_sessions = /* @__PURE__ */ new Set();
	/** 压缩期间已经回过"稍后回复"的会话（同一个压缩窗口只回一次，别刷屏）。 */
	compact_ack_sent = /* @__PURE__ */ new Set();
	/** 每个 Session 上次推送思考摘要的时间（进度节流）。 */
	last_progress_at = /* @__PURE__ */ new Map();
	aggregators = /* @__PURE__ */ new Map();
	running;
	stopped = false;
	/** 本机调试注入口（仅当设置 DSH_WEIXIN_INJECT_PORT 时启用）。 */
	inject_server;
	/** 长期记忆（时间线）；DSH_TIMELINE_ENABLED=0 可整体关闭。 */
	timeline;
	/** 知识图谱（社交网络）：人物 + "谁称呼谁为什么"的有向边。 */
	graph;
	/** 会话工作目录（会话落盘路径由它推导，必须与 HarnessSessionHost 一致）。 */
	workspace_path = process.cwd();
	/** 通知开关：codex / opencode 的完成汇报是否推送。 */
	notify;
	/** 摘要调度器（低频、纯本地检查；与 iLink 轮询无关）。 */
	timeline_scheduler;
	/** 知识图谱的空闲增量更新调度器。 */
	graph_scheduler;
	/**
	* 记忆更新的模型客户端：本地 Gemma 或云端模型（由 `DSH_MEMORY_ENGINE` 决定）。
	*
	* 本地免费但要占约 5.3GB 显存；云端不占显存但花额度。用户可在控制台切换。
	*/
	memory;
	/**
	* 后台任务的闸门（摘要 / 图谱 / 图片描述共用）。
	*
	* 本地引擎：并发 1 + **等显存空闲再启动**（用户可能在跑游戏/训练）；
	* 云端引擎：不等显存（本来就不占），但同样限制并发，避免把额度打爆。
	*/
	gemma;
	/** 组装全部组件；构造期不做 IO 失败判定。 */
	/** 宿主 ctx（故障转移要经它取 sessionController）。 */
	ctx;
	constructor(ctx, options) {
		this.ctx = ctx;
		this.observation_disposers = [ctx.on("session/event", (session, event) => {
			try {
				ObservationRecorder.observe_event(String(session.id), event);
			} catch {}
		}), ctx.on("session/disposed", (session) => {
			ObservationRecorder.forget_session(String(session.id));
		})];
		const db_path = options.state_db ?? join(join(process.env.DSH_HOME ?? join(process.env.USERPROFILE ?? ".", ".dsh")), "weixin-state.db");
		this.store = new WeixinStateStore({ path: db_path });
		const legacy_path = options.state_file ?? join(process.env.DSH_WEIXIN_STATE_FILE ?? join(process.cwd(), "runtime", "weixin-route-state.json"));
		probe("lifecycle", "migration.result", { ...migrate_legacy_route_state(this.store, legacy_path) });
		this.turn_context = new TurnContextStore(this.store);
		const timeline_enabled = process.env.DSH_TIMELINE_ENABLED !== "0";
		this.timeline = timeline_enabled ? new TimelineStore({
			dir: options.timeline_dir ?? process.env.DSH_TIMELINE_DIR ?? join(process.env.DSH_HOME ?? join(process.env.USERPROFILE ?? ".", ".dsh"), "timeline"),
			...process.env.DSH_TIMELINE_TIMEZONE === void 0 ? {} : { timezone: process.env.DSH_TIMELINE_TIMEZONE }
		}) : void 0;
		this.graph = new KnowledgeGraphStore({ dir: options.graph_dir ?? process.env.DSH_GRAPH_DIR ?? join(process.env.DSH_HOME ?? join(process.env.USERPROFILE ?? ".", ".dsh"), "graph") });
		if (this.timeline !== void 0) set_shared_memory({
			timeline: this.timeline,
			graph: this.graph
		});
		try {
			this.graph.ensure_center();
			probe("graph", "center.ready", { ...this.graph.stats() });
		} catch (error) {
			probe("graph", "center.failed", { error: String(error) });
		}
		this.notify = create_notify_settings(this.store);
		const engine = process.env.DSH_MEMORY_ENGINE === "cloud" ? "cloud" : "gemma";
		this.memory = create_memory_model({
			engine,
			...process.env.DSH_MEMORY_CLOUD_MODEL === void 0 ? {} : { cloud_model: process.env.DSH_MEMORY_CLOUD_MODEL },
			config_path: find_model_config(PROJECT_CONFIG_DIR) ?? join(PROJECT_CONFIG_DIR, "config", "modelConfig.json")
		});
		this.gemma = create_gemma_gate({
			min_free_vram_mb: engine === "gemma" && process.env.SYNLOVE_LLM_MANAGEMENT !== "external" ? Number(process.env.DSH_GEMMA_MIN_FREE_VRAM_MB ?? "5200") : 0,
			...engine === "gemma" && process.env.SYNLOVE_LLM_MANAGEMENT !== "external" ? { ensure_ready: () => this.ensure_gemma_running() } : {}
		});
		probe("lifecycle", "memory_engine", {
			engine,
			label: this.memory.label
		});
		this.client = new IlinkClient(options);
		this.workspace_path = options.workspace_path ?? process.cwd();
		this.current_model = {
			provider: options.provider ?? "",
			model: options.model ?? ""
		};
		this.host = new HarnessSessionHost(ctx, {
			...options.agent_preset === void 0 ? {} : { agent_preset: options.agent_preset },
			workspace_path: options.workspace_path ?? process.cwd(),
			...options.provider === void 0 ? {} : { provider: options.provider },
			...options.model === void 0 ? {} : { model: options.model },
			...options.reasoning_effort === void 0 ? {} : { reasoning_effort: options.reasoning_effort }
		});
		this.lease = new ConsumerLease({
			store: this.store,
			account_id: account_fingerprint(options.token),
			...options.lease_marker === void 0 ? {} : { marker_path: options.lease_marker }
		});
		this.ingress = new WeixinIngress({
			client: this.client,
			observer: {
				on_error: (error) => probe("ingress", "observer.error", { error: String(error) }),
				on_status: (status, detail) => probe("ingress", "observer.status", {
					status,
					detail
				})
			}
		});
		this.outbound = new OutboundCoordinator({
			store: this.store,
			turn_context: this.turn_context,
			sender: async (input) => {
				if (!this.lease.is_held()) throw new Error("微信消费者所有权已丢失，停止发送");
				await this.client.send_message_with_retry({
					to_user_id: input.user_id,
					text: input.text,
					client_id: input.client_id,
					...input.context_token === void 0 ? {} : { context_token: input.context_token }
				});
			},
			record_entry: (entry) => {
				this.record_timeline(entry);
			},
			classify_retryable: (error) => {
				return error?.retryable !== false;
			}
		});
		const attachments = ctx.get("attachments");
		this.codex_bridge = new CodexBridge(this.store, (user_id, session_id, text, kind, context_token) => {
			this.outbound.enqueue_notice({
				session_id,
				user_id,
				kind,
				text,
				...context_token === void 0 ? {} : { context_token }
			});
			this.outbound.drain();
		}, options.codex_workspace ?? process.env.DSH_CODEX_WORKSPACE);
		this.opencode_bridge = new OpencodeBridge(this.store, (user_id, session_id, text, kind, context_token) => {
			this.outbound.enqueue_notice({
				session_id,
				user_id,
				kind,
				text,
				...context_token === void 0 ? {} : { context_token }
			});
			this.outbound.drain();
		});
		this.opencode_watcher = new OpencodeWatcher({
			store: this.store,
			push: (user_id, text) => {
				if (!this.notify.is_enabled("opencode")) {
					probe("opencode-watch", "notify.suppressed");
					return;
				}
				const session_id = this.store.get_session_id(user_id) ?? `weixin-${user_id}`;
				this.outbound.enqueue_notice({
					session_id,
					user_id,
					kind: "assistant",
					text
				});
				this.outbound.drain();
			}
		});
		this.codex_watcher = new CodexWatcher({
			store: this.store,
			push: (user_id, text) => {
				if (!this.notify.is_enabled("codex")) {
					probe("codex-watch", "notify.suppressed");
					return;
				}
				const session_id = this.store.get_session_id(user_id) ?? `weixin-${user_id}`;
				this.outbound.enqueue_notice({
					session_id,
					user_id,
					kind: "assistant",
					text
				});
				this.outbound.drain();
			}
		});
		this.inbound = new InboundCoordinator({
			store: this.store,
			turn_context: this.turn_context,
			host: this.host,
			...attachments === void 0 ? {} : { attachments },
			enqueue_outbound: (input) => {
				this.outbound.enqueue_notice(input);
			},
			send_media: async (user_id, path, context_token) => {
				if (!this.lease.is_held()) throw new Error("微信消费者所有权已丢失，停止发送媒体");
				const { readFile } = await import("node:fs/promises");
				const data = new Uint8Array(await readFile(path));
				if (!this.lease.is_held()) throw new Error("微信消费者所有权已丢失，停止发送媒体");
				await this.client.send_image({
					to_user_id: user_id,
					data,
					...context_token === void 0 ? {} : { context_token }
				});
				probe("outbound", "weixin.send_image", {
					user_id,
					bytes: data.byteLength
				});
			},
			on_agent_ready: (agent) => {
				this.bind_agent_stream(agent);
			},
			on_user_message: (session_id, user_id, text) => {
				this.remember_user_message(session_id, user_id, text);
			},
			on_cancel: async (user_id) => {
				this.opencode_bridge.stop(user_id);
				this.codex_bridge.stop(user_id);
				const result = kill_external_runs();
				probe("inbound", "cancel.external_runs", {
					user_id,
					killed: result.killed,
					pids: [...result.pids]
				});
				return {
					killed: result.killed,
					pids: result.pids
				};
			},
			on_turn_cancelled: (session_id) => this.finalize_turn(session_id),
			codex_bridge: {
				parse: (text) => parse_codex_command(text),
				handle: (user_id, session_id, command, context_token) => this.codex_bridge.handle(user_id, session_id, command, context_token)
			},
			opencode_bridge: {
				parse: (text) => parse_opencode_command(text),
				handle: (user_id, session_id, command, context_token) => this.opencode_bridge.handle(user_id, session_id, command, context_token)
			},
			hydrate_message: async (message) => {
				const hydrated = await this.ingress.media_normalizer.hydrate_media(message, (media) => this.ingress.download_media(media));
				await this.record_media_to_timeline(hydrated, attachments).catch((error) => {
					probe("timeline", "media.record_failed", { error: String(error) });
				});
				return hydrated;
			},
			record_entry: (entry) => {
				this.record_timeline(entry);
			}
		});
		probe("lifecycle", "connector.constructed", {
			db_path,
			preset: options.agent_preset ?? "(default)",
			workspace: options.workspace_path
		});
	}
	/**
	* 启动：preflight → 装配检查 → lease → 恢复 → 开轮询。
	* 任一步失败都抛出，让上层把"微信消费者未启动"如实暴露出来（禁止假成功）。
	*/
	async start() {
		if (this.running !== void 0) return this.running;
		this.running = this.run();
		return this.running;
	}
	/** 真正的启动序列。 */
	async run() {
		if (process.env.DSH_WEIXIN_SKIP_OPENCLAW_CHECK !== "1") {
			const isolation = preflight_openclaw_isolation();
			if (!isolation.ok) throw new Error("webhook-weixin: OpenClaw 隔离检查未通过，拒绝启动：\n  - " + isolation.problems.join("\n  - ") + "\n请先停用 OpenClaw 的微信通道（openclaw-weixin.enabled=false）。");
		}
		this.host.assert_capabilities();
		this.lease.acquire();
		const rescued = this.rescue_interrupted_turns();
		const recovered = await this.inbound.recover_unfinished();
		const pending = await this.outbound.recover_pending();
		probe("lifecycle", "recover.summary", {
			...recovered,
			outbox_pending: pending,
			rescued_turns: rescued
		});
		console.error(`[webhook-weixin] 启动恢复：delivery=${recovered.recovered} outbox=${pending}${rescued > 0 ? `，补跑被打断的轮次=${rescued}` : ""}；状态快照 ${JSON.stringify(this.store.snapshot())}`);
		this.codex_watcher.start();
		this.opencode_watcher.start();
		this.start_inject_server_if_configured();
		this.start_timeline_scheduler_if_enabled();
		try {
			await this.ingress.notify_start(this.controller.signal);
			await this.poll_loop();
		} finally {
			await this.ingress.notify_stop(this.controller.signal);
		}
	}
	/**
	* 启动本机注入口（开发设施，默认关闭）。
	*
	* 用途：把一条"用户消息"塞进真实入站管线（delivery → 协调器 → agent → outbox → 微信），
	* 这样测试时不必每次都用真手机发微信。只绑 127.0.0.1，且只有设置端口才启用。
	*/
	start_inject_server_if_configured() {
		const port = Number(process.env.DSH_WEIXIN_INJECT_PORT ?? "0");
		if (!Number.isInteger(port) || port <= 0) return;
		this.inject_server = start_inject_server({
			port,
			inject: (input) => this.inject_test_message(input),
			admin: (action, body) => this.admin_action(action, body)
		});
	}
	/**
	* 管理动作分发（控制台按钮走这里）。
	*
	* 之所以由本进程执行而不是让控制台直接删文件：时间线/图谱有内存缓存、
	* 会话上下文还被 agent 句柄持有——只有持有者才知道该怎么正确清。
	*/
	async admin_action(action, body) {
		const user_id = typeof body.user_id === "string" && body.user_id !== "" ? body.user_id : void 0;
		switch (action) {
			case "clear-context": return this.clear_context(user_id === void 0 ? {} : { user_id });
			case "clear-memory": {
				const target = String(body.target ?? "");
				if (target !== "timeline" && target !== "graph") throw new Error(`clear-memory 的 target 只能是 timeline 或 graph，收到：${target}`);
				return this.clear_memory(target);
			}
			case "memory-stats": return this.memory_stats();
			case "clear-queue": return { removed: this.store.clear_queue() };
			case "failover-status": {
				const path = find_model_config(PROJECT_CONFIG_DIR);
				const config = path === void 0 ? {} : read_json_file(path);
				return {
					current: this.current_model,
					order: parse_failover_config(config).order,
					dead: this.model_health.snapshot()
				};
			}
			case "reload-preset": return this.reload_preset();
			default: throw new Error(`未知的管理动作：${action}`);
		}
	}
	/**
	* 清空长期记忆。
	*
	* - `timeline`：删掉时间线目录内容，并清掉存储层的内存缓存（否则还会列出已删的天）；
	* - `graph`：删掉图谱目录内容 → **重建中心**（欣爱 + 主人，否则查询会说"图谱里没有人"）
	*   → 水位归零（让图谱从现有历史里重新长出来）。
	*
	* 与"清空上下文"是两件事：这里清的是"她长期记得什么"，上下文是"这次聊天记得什么"。
	*/
	clear_memory(target) {
		if (target === "timeline") {
			if (this.timeline === void 0) return {
				target,
				files: 0,
				bytes: 0,
				dir: "",
				note: "时间线记忆未启用（DSH_TIMELINE_ENABLED=0）"
			};
			const removed = clear_directory_contents(this.timeline.dir_path());
			this.timeline.ensure_dirs();
			this.timeline.forget_caches();
			probe("memory", "timeline.cleared", { ...removed });
			return {
				target,
				...removed
			};
		}
		const removed = clear_directory_contents(this.graph.dir_path());
		this.graph.ensure_center();
		this.graph.reset_watermark();
		probe("memory", "graph.cleared", { ...removed });
		return {
			target,
			...removed,
			note: `已重建中心（欣爱 · 主人），并归零水位；下次空闲时会从现有历史里重新积累`
		};
	}
	/**
	* 三类记忆的体量（控制台展示用）。
	*
	* 上下文的体量按**落盘会话文件**算（用户 id → 会话 id → 会话目录），
	* 所以它显示的是真实占用，而不是一个估计值。
	*/
	memory_stats() {
		const stats = this.timeline === void 0 ? { error: "时间线记忆未启用" } : (() => {
			const summary = this.timeline.stats();
			return {
				days: summary.days ?? 0,
				entries: summary.entries ?? 0,
				bytes: directory_bytes(this.timeline.dir_path()),
				dir: this.timeline.dir_path()
			};
		})();
		const graph_stats = (() => {
			const summary = this.graph.stats();
			return {
				people: summary.people,
				relations: summary.relations,
				bytes: directory_bytes(this.graph.dir_path()),
				dir: this.graph.dir_path()
			};
		})();
		const root = sessions_root();
		const users = this.store.list_users().map((user_id) => {
			const session_id = this.store.get_session_id(user_id) ?? this.store.session_id_for(user_id);
			const log = session_log_stats(session_dir_path(root, this.workspace_path, session_id));
			return {
				user_id,
				session_id,
				epoch: this.store.context_epoch(user_id),
				online: this.host.get_agent(brandString(session_id)) !== void 0,
				bytes: log?.bytes ?? 0,
				modified_at: log?.modified_at ?? 0
			};
		});
		return {
			timeline: stats,
			graph: graph_stats,
			context: {
				users,
				bytes: users.reduce((sum, item) => sum + item.bytes, 0)
			}
		};
	}
	/**
	* 清空对话上下文（控制台「清空上下文」按钮调这里）。
	*
	* 四步，顺序不能换：
	* 1. **dispose agent**：只有 owner 释放才会关掉会话日志的写句柄。
	*    若跳过这步直接删文件，残留句柄下次 append 会写出"没有 header 的坏文件"——
	*    那个会话以后再也恢复不起来（这是实测踩过的坑）。
	* 2. 丢掉该会话的内存残留（stream/turn/聚合器）。
	* 3. **尽力删除旧会话文件**：回收磁盘；删不掉也不影响结果（见第 4 步）。
	* 4. **清路由 + 代次 +1**：下一条消息因此拿到一个新会话 id → 必然是空上下文。
	*
	* 长期记忆（时间线/图谱）**不动**：上下文是"这次聊天记得什么"，
	* 长期记忆是"她这个人记得什么"，两者是分开的，各有各的清空入口。
	*/
	async clear_context(input) {
		const users = input.user_id === void 0 || input.user_id === "" ? this.store.list_users() : [input.user_id];
		const epochs = {};
		const results = [];
		for (const user_id of users) {
			const session_id = this.store.get_session_id(user_id) ?? this.store.session_id_for(user_id);
			const disposed = await this.host.dispose_agent(session_id);
			this.forget_session(session_id);
			const deletion = delete_session_artifacts({
				root: sessions_root(),
				cwd: this.workspace_path,
				id: session_id
			});
			this.store.clear_session_id(user_id);
			epochs[user_id] = this.store.bump_context_epoch(user_id);
			results.push({
				user_id,
				session_id,
				disposed,
				deleted: deletion.deleted,
				...deletion.reason === void 0 ? {} : { note: deletion.reason }
			});
			probe("lifecycle", "context.cleared", {
				user_id,
				session_id,
				disposed,
				deleted: deletion.deleted,
				epoch: epochs[user_id]
			});
		}
		return {
			users: results,
			epochs
		};
	}
	/**
	* 重新解析 preset（GUI 挂载/卸载工具后调用），并**真的验证一次**。
	*
	* 为什么要在本进程做：preset 组合是宿主实例记忆的，必须由持有它的进程重置。
	* 验证方式是用一个探针会话真创建一次 Agent —— preset 里挂了个缺依赖的插件时，
	* 失败会在这里暴露（而不是等到用户下一条消息才发现 bot 不回话了），
	* 验证完立刻释放并删掉探针会话文件，不留痕迹。
	*/
	async reload_preset() {
		this.host.reset_composition();
		const probe_id = `preset-check-${Date.now().toString(36)}`;
		let failure = "";
		try {
			const agent = await this.host.create_agent(brandString(probe_id));
			if (this.timeline !== void 0) {
				register_timeline_tools(agent.ctx, this.timeline);
				register_graph_tools(agent.ctx, this.graph);
			}
			this.bind_agent_stream(agent);
			this.bind_agent_stream(agent);
		} catch (error) {
			failure = error instanceof Error ? error.message : String(error);
		} finally {
			this.forget_session(probe_id);
			await this.host.dispose_agent(probe_id);
			delete_session_artifacts({
				root: sessions_root(),
				cwd: this.workspace_path,
				id: probe_id
			});
		}
		if (failure !== "") {
			probe("lifecycle", "preset.reload_failed", { error: failure });
			throw new Error(`preset 组装失败，已回滚：${failure}`);
		}
		probe("lifecycle", "preset.reloaded", {});
		return {
			reloaded: true,
			routing_checked: true
		};
	}
	/** 丢掉某个会话在内存里的残留状态（上下文已经清掉，这些映射不能再留着）。 */
	forget_session(session_id) {
		try {
			this.stream_disposers.get(session_id)?.();
		} catch (error) {
			probe("lifecycle", "context.dispose_failed", {
				session_id,
				error: String(error)
			});
		}
		this.stream_disposers.delete(session_id);
		this.stream_agents.delete(session_id);
		this.tool_calls_seen.delete(session_id);
		this.current_turn.delete(session_id);
		this.last_progress_at.delete(session_id);
		this.aggregators.delete(session_id);
	}
	/** 注入一条测试消息：复用真实入站管线，但不提交 iLink 游标（它本来就不是 iLink 来的）。 */
	async inject_test_message(input) {
		const user_id = input.user_id ?? this.store.latest_user_id();
		if (user_id === void 0) throw new Error("还没有任何入站用户，无法确定收件人；请先用真实微信发一条消息");
		const delivery_id = `inject-${Date.now()}-${randomBytes(4).toString("hex")}`;
		probe("inbound", "inject.accepted", {
			delivery_id,
			user_id,
			text_len: input.text.length,
			text: input.text.slice(0, 120)
		});
		await this.handle_inbound([{
			delivery_id,
			user_id,
			text: input.text,
			media: [],
			received_at: Date.now()
		}], void 0);
		return {
			delivery_id,
			...this.store.get_session_id(user_id) === void 0 ? {} : { session_id: this.store.get_session_id(user_id) }
		};
	}
	/**
	* 把消息里的媒体记进长期记忆。
	*
	* 分两步，且都**不阻塞**用户这条消息的处理：
	* 1. 解析出附件引用（原图由 harness 附件服务按内容寻址存好，天然去重），
	*    把引用挂到当天那条记录上——**不复制文件**，所以"存原图"几乎零成本；
	* 2. 后台让视觉模型（Gemma + mmproj）写一句描述，回填到引用上——
	*    描述让"发了什么图"变成可检索文本，并会随当天摘要一起被总结。
	*
	* 原图永远是权威：描述错了、模型不在线，都不影响原始事实已经存下来了。
	*/
	async record_media_to_timeline(message, attachments) {
		if (this.timeline === void 0 || attachments === void 0) return;
		const images = message.media.filter((item) => item.kind === "image" && item.data !== void 0);
		if (images.length === 0) return;
		const date = this.timeline.day_key(message.received_at);
		const refs = [];
		for (const item of images) try {
			const ref = await attachments.saveImage({
				data: item.data,
				mediaType: item.mime_type ?? "image/jpeg",
				...item.name === void 0 ? {} : { name: item.name }
			});
			refs.push({
				id: String(ref.attachmentId),
				media_type: ref.mediaType,
				bytes: ref.bytes,
				width: ref.width,
				height: ref.height,
				...ref.name === void 0 ? {} : { name: ref.name }
			});
		} catch (error) {
			probe("timeline", "media.save_failed", { error: String(error) });
		}
		if (refs.length === 0) return;
		this.timeline.update_entry(date, message.delivery_id, { attachments: refs });
		probe("timeline", "media.recorded", {
			date,
			delivery_id: message.delivery_id,
			count: refs.length
		});
		for (const [index, item] of images.entries()) {
			const ref = refs[index];
			if (ref === void 0) continue;
			this.describe_image(item, date, message.delivery_id, ref.id);
		}
	}
	/** 让视觉模型给一张图写一句描述并回填；全程 best-effort。 */
	async describe_image(media, date, entry_id, attachment_id) {
		if (this.timeline === void 0 || media.data === void 0) return;
		try {
			const text = await this.gemma.run("image-describe", () => this.memory.describe_image({
				data: media.data,
				media_type: media.mime_type ?? "image/jpeg"
			}));
			if (text.trim() === "") return;
			this.timeline.update_attachment_description(date, entry_id, attachment_id, text.trim());
		} catch (error) {
			probe("timeline", "media.describe_failed", {
				attachment_id,
				error: String(error)
			});
		}
	}
	/** 写一条长期记忆；失败只留痕，绝不影响消息收发。 */
	record_timeline(entry) {
		if (this.timeline === void 0) return;
		try {
			this.timeline.append(entry);
		} catch (error) {
			probe("timeline", "append.failed", {
				id: entry.id,
				error: String(error)
			});
		}
	}
	/**
	* 启动时间线摘要调度（低频、纯本地）。
	*
	* 刻意与 iLink 轮询解耦：这里只读本地文件，不发任何微信请求；
	* 触发条件是"某日已空闲 ≥3 小时"，所以 5 分钟看一次足够，也不会给微信链路添压力。
	*/
	start_timeline_scheduler_if_enabled() {
		if (this.timeline === void 0) return;
		const summarize = create_gemma_summarizer({ complete: (prompt, max_tokens) => this.memory.complete(prompt, max_tokens) });
		this.timeline_scheduler = start_timeline_scheduler({
			store: this.timeline,
			summarizer: (request) => this.gemma.run("timeline-summary", () => summarize(request)),
			on_update: (dates) => probe("timeline", "summary.updated_days", { dates })
		});
		this.graph_scheduler = start_graph_scheduler({
			graph: this.graph,
			timeline: this.timeline,
			complete: (prompt) => this.gemma.run("graph-extract", () => this.memory.complete(prompt)),
			on_update: (result) => probe("graph", "updated", { ...result })
		});
	}
	/**
	* 确保本地 Gemma 服务在跑（运行期按需补拉）。
	*
	* 启动时如果显存被占用，启动器会跳过拉起；等显存空出来之后由这里补拉，
	* 路径/端口都从启动器注入的环境变量读取（**不在这里重新猜路径**，避免两处不一致）。
	*/
	async ensure_gemma_running() {
		if (process.env.SYNLOVE_LLM_MANAGEMENT === "external") return;
		if (await port_open(Number(process.env.GEMMA_SERVER_PORT ?? "8080"))) return;
		const binary = process.env.GEMMA_SERVER_BIN;
		const model = process.env.GEMMA_MODEL_PATH;
		if (binary === void 0 || model === void 0) throw new Error("不知道 Gemma 怎么启动（缺 GEMMA_SERVER_BIN/GEMMA_MODEL_PATH），请重启一次链路。");
		const args = [
			"-m",
			model,
			"--host",
			"127.0.0.1",
			"--port",
			process.env.GEMMA_SERVER_PORT ?? "8080",
			"-c",
			process.env.GEMMA_CONTEXT ?? "32768",
			"-ngl",
			"99",
			"--jinja",
			"--reasoning",
			"off",
			"-np",
			"1",
			"-fa",
			"on",
			"--alias",
			process.env.DSH_GEMMA_MODEL ?? "gemma-4-E4B-it-Q4_K_M",
			"--sleep-idle-seconds",
			process.env.DSH_GEMMA_SLEEP_IDLE_SECONDS ?? "120"
		];
		if (process.env.GEMMA_MMPROJ_PATH !== void 0 && process.env.GEMMA_MMPROJ_PATH !== "") args.push("--mmproj", process.env.GEMMA_MMPROJ_PATH);
		probe("gemma", "server.start_on_demand", {
			binary,
			model
		});
		spawn(binary, args, {
			windowsHide: true,
			stdio: "ignore",
			detached: true
		}).unref();
		for (let attempt = 0; attempt < 120; attempt += 1) {
			if (await port_open(Number(process.env.GEMMA_SERVER_PORT ?? "8080"))) return;
			await new Promise((resolve) => setTimeout(resolve, 2e3));
		}
		throw new Error("Gemma 服务启动后一直没就绪");
	}
	/** 轮询主循环：批次完成后才提交 cursor；错误按类别处理。 */
	async poll_loop() {
		let backoff_index = 0;
		/** 游标连续被卡住的次数；用于退避，避免服务端立刻重投造成热循环。 */
		let cursor_stall = 0;
		while (!this.controller.signal.aborted && !this.stopped) {
			if (!this.lease.is_held()) {
				this.controller.abort();
				this.outbound.stop();
				await this.inbound.dispose();
				probe("lifecycle", "lease.lost_stop");
				return;
			}
			this.outbound.drain();
			const cursor = this.store.get_cursor();
			const result = await this.ingress.poll(cursor, this.controller.signal);
			if (!this.lease.is_held()) continue;
			switch (result.kind) {
				case "messages": {
					backoff_index = 0;
					const committable = await this.handle_inbound(result.messages, result.cursor);
					if (committable && result.cursor !== void 0) {
						this.store.set_cursor(result.cursor);
						cursor_stall = 0;
					} else if (!committable) {
						probe("inbound", "cursor.held", {
							reason: "有 delivery 未达终态，等待重投或恢复",
							delivery_ids: result.messages.map((m) => m.delivery_id),
							stall: cursor_stall + 1
						});
						cursor_stall += 1;
						await sleep(Math.min(5e3, 500 * cursor_stall), this.controller.signal);
					}
					await this.outbound.drain();
					break;
				}
				case "idle":
					backoff_index = 0;
					if (result.cursor !== void 0) this.store.set_cursor(result.cursor);
					await this.outbound.drain();
					break;
				case "session_timeout":
					probe("ingress", "session_timeout.recover");
					await this.ingress.notify_start(this.controller.signal).catch((error) => {
						probe("ingress", "session_timeout.notify_failed", { error: String(error) });
					});
					break;
				case "auth_required":
					probe("lifecycle", "auth_required", { detail: result.detail });
					this.write_auth_marker(result.detail);
					this.stopped = true;
					return;
				case "protocol_error": {
					const wait = BACKOFF_STEPS[Math.min(backoff_index, BACKOFF_STEPS.length - 1)];
					backoff_index += 1;
					probe("ingress", "backoff", {
						wait_ms: wait,
						attempt: backoff_index,
						detail: result.detail
					});
					await sleep(wait, this.controller.signal);
					break;
				}
			}
		}
	}
	/** 写认证失效标记，供启动器提示扫码。 */
	write_auth_marker(detail) {
		const marker = process.env.DSH_WEIXIN_TOKEN_MARKER ?? join(process.env.DSH_HOME ?? join(process.env.USERPROFILE ?? ".", ".dsh"), "weixin-token-invalid.flag");
		import("node:fs/promises").then((fs) => fs.writeFile(marker, `${(/* @__PURE__ */ new Date()).toISOString()} ${detail}\n`, "utf8")).catch(() => void 0);
		console.error("[webhook-weixin] 认证失效，已停止消费；请运行 weixin_login.py 重新扫码。标记：" + marker);
	}
	/** 桥接 assistant-stream：聚合成正文、按 turn 绑定上下文、驱动进度/心跳。 */
	bind_agent_stream(agent) {
		const session_id = String(agent.id);
		if (this.stream_agents.get(session_id) === agent) return;
		this.stream_disposers.get(session_id)?.();
		if (this.timeline !== void 0) agent.ctx.effect(() => {
			register_timeline_tools(agent.ctx, this.timeline);
			register_graph_tools(agent.ctx, this.graph);
			register_notify_tools(agent.ctx, this.notify);
			register_bridge_result_tools(agent.ctx, this.store);
			register_bridge_session_tools(agent.ctx);
			return () => {};
		}, "webhook-weixin.memory-tools()");
		const dispose = agent.ctx.on("agent/assistant-stream", ({ agent: subject, frame }) => {
			if (subject !== agent) return;
			if (frame.type === "chunk") this.handle_chunk(session_id, frame);
		});
		const dispose_pre_step = agent.ctx.on("agent/pre-step", ({ agent: subject, turn, step }, next) => {
			if (subject === agent && (step === 1 || this.current_turn.get(session_id) !== turn)) this.begin_turn_once(session_id, turn);
			return next();
		});
		const dispose_turn_stopping = agent.ctx.on("agent/turn-stopping", ({ agent: subject, turn }) => {
			if (subject !== agent) return;
			this.handle_turn_end(session_id, turn);
		});
		const dispose_error = agent.ctx.on("agent/error", ({ error }) => {
			const detail = error instanceof Error ? error.message : String(error);
			probe("inbound", "agent.error", {
				session_id,
				error: detail
			});
			console.error(`[webhook-weixin] agent error session=${session_id}: ${detail}`);
			this.turn_error.set(session_id, detail);
			this.arm_turn_failure(session_id, detail);
			const reason = classify_llm_failure(detail);
			if (reason !== void 0) this.failover_model(session_id, detail, reason);
		});
		const dispose_compaction = agent.ctx.on("session/event", (session, event) => {
			if (session !== agent.session) return;
			if (event.type === "compaction/start") {
				this.compacting_sessions.add(session_id);
				probe("lifecycle", "compaction.suspend_inbound", { session_id });
				return;
			}
			if (event.type !== "compaction/end") return;
			this.compacting_sessions.delete(session_id);
			this.compact_ack_sent.delete(session_id);
			probe("lifecycle", "compaction.resume_inbound", { session_id });
			this.inbound.recover_unfinished().then((result) => {
				if (result.recovered > 0) {
					probe("lifecycle", "compaction.requeued", {
						session_id,
						recovered: result.recovered
					});
					this.outbound.drain();
				}
			}).catch((error) => {
				probe("lifecycle", "compaction.requeue_failed", {
					session_id,
					error: String(error)
				});
			});
		});
		this.stream_disposers.set(session_id, () => {
			dispose();
			dispose_error();
			dispose_pre_step();
			dispose_turn_stopping();
			dispose_compaction();
			ObservationRecorder.forget_session(session_id);
		});
		this.stream_agents.set(session_id, agent);
		probe("lifecycle", "stream.bound", { session_id });
	}
	/**
	* 补跑"被重启打断的轮次"。
	*
	* 做法：启动时扫描 `inflight_turn:*` 残留标记（轮次开始写、结束删），把对应 delivery
	* 从 injected 退回 received —— 随后 `inbound.recover_unfinished()` 会把它重新注入，
	* 用户就能拿到这一轮的回复，而不是被静默丢掉。
	*
	* @returns 补跑的轮次数
	*/
	rescue_interrupted_turns() {
		let rescued = 0;
		for (const item of this.store.list_meta_prefix(INFLIGHT_TURN_PREFIX)) {
			const session_id = item.key.slice(14);
			let detail = {};
			try {
				detail = JSON.parse(item.value);
			} catch {}
			this.store.delete_meta(item.key);
			if (typeof detail.delivery_id !== "string") continue;
			if (typeof detail.at === "number" && Date.now() - detail.at < INFLIGHT_RESCUE_MIN_AGE_MS) continue;
			const record = this.store.get_delivery(detail.delivery_id);
			if (record === void 0 || record.status === "failed_terminal") continue;
			try {
				this.store.update_delivery(detail.delivery_id, "received", { last_error: "重启打断了这一轮，自动重投" });
				rescued += 1;
				probe("inbound", "turn.rescued", {
					session_id,
					turn: detail.turn,
					delivery_id: detail.delivery_id
				});
				this.outbound.enqueue_notice({
					session_id,
					user_id: record.user_id,
					kind: "assistant",
					text: "刚才那轮被重启打断了，我重跑一遍。"
				});
			} catch (error) {
				probe("inbound", "turn.rescue_failed", {
					session_id,
					delivery_id: detail.delivery_id,
					error: String(error)
				});
			}
		}
		if (rescued > 0) console.error(`[webhook-weixin] 补跑被打断的轮次：${rescued} 条`);
		return rescued;
	}
	/**
	* 入站批次的统一入口（轮询与调试注入口都走这里）。
	*
	* **压缩上下文期间不收新消息**：只入库（payload 已落库）+ 回一句"稍后回复"，
	* 游标故意不提交；压缩结束后 `recover_unfinished()` 会把它们重投。
	* 这样新消息既不会打断/污染压缩，也不会丢。
	*
	* @param messages - 本批入站消息
	* @param cursor - 平台游标（注入口没有）
	* @returns 是否允许提交该批次 cursor
	*/
	async handle_inbound(messages, cursor) {
		const compacting = messages.filter((item) => {
			const session = this.store.session_id_for(item.user_id);
			return session !== void 0 && this.compacting_sessions.has(session);
		});
		if (compacting.length > 0) {
			for (const item of compacting) this.notify_compacting(item.user_id);
			await this.inbound.handle_batch(messages, cursor, { hold: true });
			probe("inbound", "batch.held_for_compaction", { count: messages.length });
			await this.outbound.drain();
			return false;
		}
		return this.inbound.handle_batch(messages, cursor);
	}
	/**
	* 压缩上下文期间收到消息：回一句"正在压缩上下文，稍后回复"（同一个窗口只回一次）。
	* 消息本身已经入库，压缩结束后会补处理。
	*/
	notify_compacting(user_id) {
		const key = this.store.session_id_for(user_id) ?? `weixin-${user_id}`;
		if (this.compact_ack_sent.has(key)) return;
		this.compact_ack_sent.add(key);
		probe("outbound", "compaction_ack", {
			user_id,
			session_id: key
		});
		this.outbound.enqueue_notice({
			session_id: key,
			user_id,
			kind: "assistant",
			text: "（正在压缩上下文，稍后回复；你这条已经排上队了）"
		});
	}
	/**
	* 取消后的收尾（幂等）。
	*
	* `agent.cancel()` 走 abort 路径时**不会**触发 `agent/turn-stopping`（实测），
	* 所以这里把 handle_turn_end 里那些"必须做"的清理补上：
	* 停心跳（否则每 5 分钟继续发「还在处理中」）、删 inflight 标记（否则下次启动会重跑已取消的任务）、
	* 清错误/可见性/聚合器/当前轮次状态。
	*/
	finalize_turn(session_id) {
		const turn = this.current_turn.get(session_id);
		this.outbound.end_turn(session_id);
		this.store.delete_meta(`${INFLIGHT_TURN_PREFIX}${session_id}`);
		this.turn_error.delete(session_id);
		this.turn_visible.delete(session_id);
		this.clear_turn_failure(session_id);
		this.aggregators.delete(session_id);
		this.tool_calls_seen.delete(session_id);
		this.current_turn.delete(session_id);
		probe("lifecycle", "turn.finalized_after_cancel", {
			session_id,
			turn
		});
	}
	/**
	* 一轮开始：**只在首次进入该 turn 时**绑定回复上下文并启动进度追踪。
	*
	* 关键约束（修复"同一 turn 上下文被后续消息覆盖"）：
	* 已经绑定过的 turn 绝不重绑——长任务期间用户再发消息时，
	* 那条新消息属于下一个 turn，不能改写本轮回复该用的 context_token。
	*/
	begin_turn_once(session_id, turn) {
		const already = this.turn_context.get(session_id, turn);
		this.current_turn.set(session_id, turn);
		this.aggregators.set(session_id, new ReplyAggregator());
		this.tool_calls_seen.set(session_id, /* @__PURE__ */ new Set());
		this.outbound.begin_turn(session_id, turn);
		if (already !== void 0 && this.binding_is_fresh(already.delivery_id)) {
			probe("outbound", "turn.start_reuse", {
				session_id,
				turn,
				delivery_id: already.delivery_id
			});
			this.store.set_meta(`${INFLIGHT_TURN_PREFIX}${session_id}`, JSON.stringify({
				turn,
				delivery_id: already.delivery_id,
				at: Date.now()
			}));
			return;
		}
		if (already !== void 0) probe("outbound", "turn.rebind_stale", {
			session_id,
			turn,
			delivery_id: already.delivery_id
		});
		const latest = this.store.latest_delivery_for_session(session_id);
		if (latest !== void 0) {
			const delivery = this.store.get_delivery(latest.delivery_id);
			const context_token = this.store.delivery_context_token(latest.delivery_id);
			this.turn_context.bind_if_absent(session_id, turn, {
				delivery_id: latest.delivery_id,
				user_id: delivery?.user_id ?? latest.user_id,
				...context_token === void 0 ? {} : { context_token }
			});
		}
		const context = this.turn_context.resolve(session_id, turn);
		probe("outbound", "turn.start", {
			session_id,
			turn,
			delivery_id: context?.delivery_id,
			has_token: context?.context_token !== void 0
		});
		this.store.set_meta(`${INFLIGHT_TURN_PREFIX}${session_id}`, JSON.stringify({
			turn,
			delivery_id: context?.delivery_id,
			at: Date.now()
		}));
	}
	/**
	* 该 turn 绑定是否仍然新鲜。
	* 超过 TTL 就视为"重启前的旧绑定"：turn 号会重复，但 context_token 可能已失效。
	*/
	binding_is_fresh(delivery_id) {
		const record = this.store.get_delivery(delivery_id);
		if (record === void 0) return false;
		return Date.now() - record.received_at < BINDING_TTL_MS;
	}
	/**
	* 自动换模型：把失活的 API 拉黑，切到下一个可用的，并把用户刚才那条消息重做一遍。
	*
	* 全过程不重启、不丢上下文（`sessionController.selectModel` 装到下一个请求上），
	* 并同步写回 `modelConfig.json` 的默认模型——否则控制台显示的、以及下次启动器同步出去的
	* 还是那个已经挂掉的模型，等于白换。
	*/
	async failover_model(session_id, detail, reason) {
		try {
			const config_path = find_model_config(PROJECT_CONFIG_DIR);
			if (config_path === void 0) {
				probe("failover", "config_missing", { workspace: PROJECT_CONFIG_DIR });
				return;
			}
			const config = read_json_file(config_path);
			const failover = parse_failover_config(config);
			if (!failover.enabled) return;
			const current = this.current_model;
			const dead = this.model_health.mark_dead(current, reason, detail, cooldown_ms(reason, failover));
			probe("failover", "model.dead", {
				provider: current.provider,
				model: current.model,
				reason,
				until: dead.until,
				detail: detail.slice(0, 120)
			});
			const next = next_alive_candidate(failover.order, current, (candidate) => this.model_health.is_dead(candidate));
			if (next === void 0) {
				probe("failover", "no_candidate", { tried: current });
				const user_id = this.store.latest_user_id();
				if (user_id !== void 0) {
					const session_key = this.store.get_session_id(user_id) ?? session_id;
					this.outbound.enqueue_notice({
						session_id: session_key,
						user_id,
						kind: "assistant",
						text: `配置里的模型接口都不可用了（${current.model}：${detail.slice(0, 80)}）。换个 key 或等额度恢复后我再试；也可以让我用别的模型。`
					});
					this.outbound.drain();
				}
				return;
			}
			const controller = this.ctx.get("sessionController");
			if (controller?.selectModel === void 0) {
				probe("failover", "controller_missing", {});
				return;
			}
			await controller.selectModel({
				sessionId: brandString(session_id),
				provider: next.provider,
				model: next.model
			});
			this.current_model = {
				provider: next.provider,
				model: next.model
			};
			this.persist_default_model(config_path, config, next);
			probe("failover", "model.switched", {
				from: `${current.provider}/${current.model}`,
				to: `${next.provider}/${next.model}`,
				reason
			});
			const user_id = this.store.latest_user_id();
			if (user_id !== void 0) {
				const session_key = this.store.get_session_id(user_id) ?? session_id;
				this.outbound.enqueue_notice({
					session_id: session_key,
					user_id,
					kind: "assistant",
					text: `「${current.model}」这个接口用不了了（${reason === "quota" ? "额度用完" : reason === "auth" ? "key 失效" : "模型下线"}），我先换成「${next.model}」，把你刚才那条重做一遍。`
				});
				this.outbound.drain();
				await this.retry_last_message(session_id, user_id);
			}
		} catch (error) {
			probe("failover", "failed", { error: String(error).slice(0, 200) });
		}
	}
	/** 把新的默认模型写回 modelConfig.json（含备份）。 */
	persist_default_model(config_path, config, next) {
		try {
			const parsed = config;
			parsed.defaultModel = {
				provider: next.provider,
				model: next.model
			};
			parsed.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
			copyFileSync(config_path, `${config_path}.bak-failover`);
			writeFileSync(config_path, `${JSON.stringify(parsed, null, 2)}\n`, "utf8");
		} catch (error) {
			probe("failover", "persist_failed", { error: String(error).slice(0, 160) });
		}
	}
	/** 记住最近一条用户消息（换模型后要重做它）。 */
	remember_user_message(session_id, user_id, text) {
		this.last_user_message.set(session_id, {
			user_id,
			text,
			at: Date.now(),
			retried: false
		});
	}
	/**
	* 重做最近一条用户消息（每个会话只自动重做一次，避免连环重试）。
	*
	* 走的是**真实入站管线**（和用户自己发一样），所以工具、记忆、回复都一样；
	* 只是 delivery id 用 `retry-` 前缀，便于在探针里区分。
	*/
	async retry_last_message(session_id, user_id) {
		const remembered = this.last_user_message.get(session_id);
		if (remembered === void 0 || remembered.text.trim() === "") return;
		if (remembered.retried) {
			probe("failover", "retry.skipped_already", { session_id });
			return;
		}
		remembered.retried = true;
		const delivery_id = `retry-${Date.now()}-${randomBytes(3).toString("hex")}`;
		probe("failover", "retry.start", {
			session_id,
			delivery_id,
			chars: remembered.text.length
		});
		try {
			await this.handle_inbound([{
				delivery_id,
				user_id,
				text: remembered.text,
				media: [],
				received_at: Date.now()
			}], void 0);
		} catch (error) {
			probe("failover", "retry.failed", {
				session_id,
				error: String(error).slice(0, 160)
			});
		}
	}
	/**
	* 记一次 agent 错误，并在宽限期后把这一轮按失败收尾。
	*
	* 为什么必须做：模型接口持续报错（例如 429「api key 日限额已用完」）时，agent loop
	* 会一直重试/卡住，`agent/turn-stopping` 永远不来——用户那边只看到每 5 分钟一条
	* "还在处理中"，永远等不到结果（2026-09-21 23:46 那轮实测卡了 10 分钟以上）。
	* 宽限期内只要有任何新分片就取消收尾（说明重试成功了，别抢话）。
	*/
	arm_turn_failure(session_id, detail) {
		const existing = this.turn_failure_timers.get(session_id);
		if (existing !== void 0) clearTimeout(existing);
		const timer = setTimeout(() => {
			this.turn_failure_timers.delete(session_id);
			this.fail_turn(session_id, detail);
		}, AGENT_ERROR_GRACE_MS);
		this.turn_failure_timers.set(session_id, timer);
	}
	/** 有活动就取消待定的失败收尾。 */
	clear_turn_failure(session_id) {
		const timer = this.turn_failure_timers.get(session_id);
		if (timer !== void 0) {
			clearTimeout(timer);
			this.turn_failure_timers.delete(session_id);
		}
	}
	/**
	* 把模型接口的报错翻成"人话 + 该怎么办"。
	*
	* 为什么必须分类：以前**所有**失败都写"多半是额度/限额用完或 key 失效"，而实测
	* 最常见的其实是传输层失败（OpenAI SDK 的 `APIConnectionError`，消息就是
	* "Connection error."——接口那一小段时间连不上/连接被断开），跟额度完全无关。
	* 说错原因会让用户白等额度、或者去换根本不坏的 key。
	*/
	explain_llm_failure(detail) {
		const text = detail.toLowerCase();
		if (/connection error|fetch failed|econnreset|econnrefused|etimedout|enotfound|socket hang up|network|transport|proxy/.test(text)) return "这是**网络/接口连接**问题（不是额度）：接口那边这一小会儿没连上或把连接断了。让我重试一般就恢复。";
		if (/quota|insufficient|余额|欠费|限额|额度/.test(text)) return "这是**额度/限额**问题：等额度恢复，或在控制台「模型选择」换一个模型。";
		if (/401|403|unauthor|invalid.*key|api.?key|鉴权|auth/.test(text)) return "这是**鉴权**失败（key 失效或没权限）：需要换 key，或换一个模型。";
		if (/429|rate.?limit|too many|限流/.test(text)) return "这是被**限流**了：稍等一下就好，我可以再试一次。";
		return "可以让我再试一次；如果反复出现，就在控制台「模型选择」换一个模型。";
	}
	/**
	* 把卡住的一轮按失败收尾：**如实说明原因**，并把这一轮关掉。
	*
	* 宁可说"没做成 + 为什么"，也不要让用户对着"还在处理中"干等——
	* 这也是人格里"工具失败就直接说失败和可确认的原因"的要求。
	*/
	async fail_turn(session_id, detail) {
		const turn = this.current_turn.get(session_id);
		if (turn === void 0) return;
		const user_id = this.turn_context.resolve(session_id, turn)?.user_id ?? "";
		probe("inbound", "turn.failed", {
			session_id,
			turn,
			detail
		});
		this.turn_error.delete(session_id);
		this.outbound.enqueue_assistant({
			session_id,
			user_id,
			turn,
			text: `我这轮没做成：模型接口报错「${detail.slice(0, 140)}」。\n${this.explain_llm_failure(detail)}`
		});
		await this.handle_turn_end(session_id, turn);
	}
	/** 一轮结束：补发尾段、清理状态。 */
	async handle_turn_end(session_id, turn) {
		this.clear_turn_failure(session_id);
		this.store.delete_meta(`${INFLIGHT_TURN_PREFIX}${session_id}`);
		const error = this.turn_error.get(session_id);
		const visible = this.turn_visible.has(session_id);
		this.turn_error.delete(session_id);
		this.turn_visible.delete(session_id);
		if (error !== void 0 && !visible) {
			probe("outbound", "turn.failed_notice", {
				session_id,
				turn
			});
			this.outbound.enqueue_notice({
				session_id,
				user_id: this.turn_context.resolve(session_id, turn)?.user_id ?? "",
				kind: "assistant",
				text: `我这轮没做成：模型接口报错「${error.slice(0, 120)}」。\n${this.explain_llm_failure(error)}`
			});
		}
		const aggregator = this.aggregators.get(session_id);
		this.aggregators.delete(session_id);
		if (aggregator !== void 0) {
			const tail = aggregator.finish();
			if (tail) this.outbound.enqueue_assistant({
				session_id,
				user_id: this.turn_context.resolve(session_id, turn)?.user_id ?? "",
				turn,
				text: tail
			});
		}
		this.outbound.end_turn(session_id);
		this.tool_calls_seen.delete(session_id);
		this.current_turn.delete(session_id);
		this.last_progress_at.delete(session_id);
		this.turn_context.prune(session_id);
		await this.outbound.drain();
		probe("outbound", "turn.done", {
			session_id,
			turn
		});
	}
	/** 处理流分片：正文聚合发送、思考摘要作为进度（即心跳）。 */
	handle_chunk(session_id, frame) {
		if (frame.type !== "chunk") return;
		this.clear_turn_failure(session_id);
		const chunk = frame.chunk;
		if (chunk?.type === "text-delta" && typeof chunk.text === "string") {
			const aggregator = this.aggregators.get(session_id) ?? new ReplyAggregator();
			this.aggregators.set(session_id, aggregator);
			const turn = this.current_turn.get(session_id) ?? 0;
			for (const segment of aggregator.accept_chunk(chunk.text)) {
				this.outbound.note_visible(session_id);
				this.turn_visible.add(session_id);
				this.outbound.enqueue_assistant({
					session_id,
					user_id: "",
					turn,
					text: segment
				});
			}
			this.outbound.note_visible(session_id);
			this.outbound.drain();
			return;
		}
		if (chunk?.type === "reasoning-delta" && typeof chunk.text === "string") {
			const aggregator = this.aggregators.get(session_id) ?? new ReplyAggregator();
			this.aggregators.set(session_id, aggregator);
			aggregator.accept_reasoning(chunk.text);
			this.maybe_push_progress(session_id, aggregator);
			return;
		}
		if (chunk?.type === "tool-call-delta" && typeof chunk.name === "string" && chunk.name.length > 0) {
			const seen = this.tool_calls_seen.get(session_id) ?? /* @__PURE__ */ new Set();
			const call_id = typeof chunk.id === "string" && chunk.id.length > 0 ? chunk.id : `${chunk.name}:${seen.size}`;
			if (seen.has(call_id)) return;
			seen.add(call_id);
			this.tool_calls_seen.set(session_id, seen);
		}
	}
	/**
	* 按时间推送思考摘要（即进度，也是心跳）。
	*
	* 触发条件：距上次推送 ≥ PROGRESS_MIN_INTERVAL_MS，且有足够长度的新摘要。
	* 不再依赖工具调用次数——否则"只调一次工具"的任务全程无进度。
	*/
	maybe_push_progress(session_id, aggregator) {
		if (PROGRESS_MIN_INTERVAL_MS <= 0) return;
		const now = Date.now();
		if (now - (this.last_progress_at.get(session_id) ?? 0) < PROGRESS_MIN_INTERVAL_MS) return;
		const excerpt = aggregator.take_reasoning_excerpt(120);
		if (excerpt === void 0 || excerpt.length < PROGRESS_MIN_CHARS) return;
		if ((excerpt.match(/[\u4e00-\u9fff]/g) ?? []).length < PROGRESS_MIN_CHARS) return;
		this.last_progress_at.set(session_id, now);
		this.outbound.enqueue_progress(session_id, `（进度·欣爱在想）${excerpt}`);
		probe("outbound", "progress.pushed", {
			session_id,
			chars: excerpt.length
		});
		this.outbound.drain();
	}
	/** 停止：幂等；释放监听、租约与 Agent。 */
	async stop() {
		if (this.stopped && this.running === void 0) return;
		this.stopped = true;
		this.controller.abort();
		await this.running?.catch(() => void 0);
		this.codex_watcher.stop();
		this.opencode_watcher.stop();
		this.outbound.stop();
		if (this.inject_server !== void 0) {
			this.inject_server.close();
			this.inject_server = void 0;
		}
		this.timeline_scheduler?.stop();
		this.timeline_scheduler = void 0;
		try {
			this.timeline?.flush_views();
		} catch (error) {
			probe("timeline", "flush.failed", { error: String(error) });
		}
		this.graph_scheduler?.stop();
		this.graph_scheduler = void 0;
		for (const dispose of this.stream_disposers.values()) dispose();
		this.stream_disposers.clear();
		this.stream_agents.clear();
		await this.inbound.dispose();
		this.aggregators.clear();
		this.tool_calls_seen.clear();
		await this.outbound.drain().catch(() => void 0);
		await this.host.dispose_agents();
		for (const dispose of this.observation_disposers) dispose();
		this.lease.release();
		this.store.close();
		probe("lifecycle", "connector.stopped");
	}
};
/** 可被 signal 打断的等待。 */
function sleep(ms, signal) {
	return new Promise((resolve) => {
		const timer = setTimeout(done, ms);
		function done() {
			clearTimeout(timer);
			signal?.removeEventListener("abort", done);
			resolve();
		}
		signal?.addEventListener("abort", done, { once: true });
	});
}
/**
* 将连接器绑定到 Cordis effect；卸载时幂等地停止。
*
* 关键：`start()` 必须被 **await**，让 lease 冲突 / OpenClaw 隔离失败 / Agent 能力缺失 /
* 微信启动失败都变成插件装配失败（fail closed）。否则会退化成"Web 后台正常、微信消费者
* 其实没起来"的假成功——此时进程还活着，但用户发消息永远没人接。
*/
function install_weixin_connector(ctx, options) {
	const connector = new WeixinConnector(ctx, options);
	ctx.effect(async () => {
		const done = probe_timer("lifecycle", "connector.start");
		try {
			await connector.start();
			done({ outcome: "ok" });
		} catch (error) {
			probe("lifecycle", "connector.start_failed", { error: error instanceof Error ? error.message : String(error) });
			console.error("[webhook-weixin] 连接器启动失败：" + (error instanceof Error ? error.stack ?? error.message : String(error)));
			throw error;
		}
		return () => connector.stop();
	}, "webhook-weixin.lifecycle()");
	return connector;
}
//#endregion
//#region ../../../vendor/cosmokit/src/misc.ts
/** Return true when a value is `null` or `undefined`. */
function isNullable(value) {
	return value === null || value === void 0;
}
/** Return true for non-array object values. */
function isPlainObject(data) {
	return data && typeof data === "object" && !Array.isArray(data);
}
/** Filter object entries and return a new object. */
function filterKeys(object, filter) {
	return Object.fromEntries(Object.entries(object).filter(([key, value]) => filter(key, value)));
}
/** Map object values while preserving the original key set. */
function mapValues(object, transform) {
	return Object.fromEntries(Object.entries(object).map(([key, value]) => [key, transform(value, key)]));
}
/** Pick selected keys from an object, optionally including `undefined` values. */
function pick(source, keys, forced) {
	if (!keys) return { ...source };
	const result = {};
	for (const key of keys) if (forced || source[key] !== void 0) result[key] = source[key];
	return result;
}
//#endregion
//#region ../../../vendor/cosmokit/src/types.ts
/** Test values using `instanceof` with a `toStringTag` fallback. */
function is(type, value) {
	if (arguments.length === 1) return (value) => is(type, value);
	return type in globalThis && value instanceof globalThis[type] || Object.prototype.toString.call(value).slice(8, -1) === type;
}
function isArrayBufferLike(value) {
	return is("ArrayBuffer", value) || is("SharedArrayBuffer", value);
}
function isArrayBufferSource(value) {
	return isArrayBufferLike(value) || ArrayBuffer.isView(value);
}
let Binary;
(function(_Binary) {
	_Binary.is = isArrayBufferLike;
	_Binary.isSource = isArrayBufferSource;
	function fromSource(source) {
		if (ArrayBuffer.isView(source)) return source.buffer.slice(source.byteOffset, source.byteOffset + source.byteLength);
		else return source;
	}
	_Binary.fromSource = fromSource;
	function toBase64(source) {
		source = fromSource(source);
		if (typeof Buffer !== "undefined") return Buffer.from(source).toString("base64");
		let binary = "";
		const bytes = new Uint8Array(source);
		for (let i = 0; i < bytes.byteLength; i++) binary += String.fromCharCode(bytes[i]);
		return btoa(binary);
	}
	_Binary.toBase64 = toBase64;
	function fromBase64(source) {
		if (typeof Buffer !== "undefined") return fromSource(Buffer.from(source, "base64"));
		return Uint8Array.from(atob(source), (c) => c.charCodeAt(0));
	}
	_Binary.fromBase64 = fromBase64;
	function toHex(source) {
		source = fromSource(source);
		if (typeof Buffer !== "undefined") return Buffer.from(source).toString("hex");
		return Array.from(new Uint8Array(source), (byte) => byte.toString(16).padStart(2, "0")).join("");
	}
	_Binary.toHex = toHex;
	function fromHex(source) {
		if (typeof Buffer !== "undefined") return fromSource(Buffer.from(source, "hex"));
		const hex = source.length % 2 === 0 ? source : source.slice(0, source.length - 1);
		const buffer = [];
		for (let i = 0; i < hex.length; i += 2) buffer.push(parseInt(`${hex[i]}${hex[i + 1]}`, 16));
		return Uint8Array.from(buffer).buffer;
	}
	_Binary.fromHex = fromHex;
})(Binary || (Binary = {}));
Binary.fromBase64;
Binary.toBase64;
Binary.fromHex;
Binary.toHex;
/** Deep-clone common JavaScript values while preserving prototypes and cycles. */
function clone(source, refs = /* @__PURE__ */ new Map()) {
	if (!source || typeof source !== "object") return source;
	if (is("Date", source)) return new Date(source.valueOf());
	if (is("RegExp", source)) return new RegExp(source.source, source.flags);
	if (isArrayBufferLike(source)) return source.slice(0);
	if (ArrayBuffer.isView(source)) return source.buffer.slice(source.byteOffset, source.byteOffset + source.byteLength);
	const cached = refs.get(source);
	if (cached) return cached;
	if (Array.isArray(source)) {
		const result = [];
		refs.set(source, result);
		source.forEach((value, index) => {
			result[index] = Reflect.apply(clone, null, [value, refs]);
		});
		return result;
	}
	const result = Object.create(Object.getPrototypeOf(source));
	refs.set(source, result);
	for (const key of Reflect.ownKeys(source)) {
		const descriptor = { ...Reflect.getOwnPropertyDescriptor(source, key) };
		if ("value" in descriptor) descriptor.value = Reflect.apply(clone, null, [descriptor.value, refs]);
		Reflect.defineProperty(result, key, descriptor);
	}
	return result;
}
/** Deeply compare arrays, dates, regexps, buffers, and plain object fields. */
function deepEqual(a, b, strict) {
	if (a === b) return true;
	if (!strict && isNullable(a) && isNullable(b)) return true;
	if (typeof a !== typeof b) return false;
	if (typeof a !== "object") return false;
	if (!a || !b) return false;
	function check(test, then) {
		return test(a) ? test(b) ? then(a, b) : false : test(b) ? false : void 0;
	}
	return check(Array.isArray, (a, b) => a.length === b.length && a.every((item, index) => deepEqual(item, b[index]))) ?? check(is("Date"), (a, b) => a.valueOf() === b.valueOf()) ?? check(is("RegExp"), (a, b) => a.source === b.source && a.flags === b.flags) ?? check(isArrayBufferLike, (a, b) => {
		if (a.byteLength !== b.byteLength) return false;
		const viewA = new Uint8Array(a);
		const viewB = new Uint8Array(b);
		for (let i = 0; i < viewA.length; i++) if (viewA[i] !== viewB[i]) return false;
		return true;
	}) ?? Object.keys({
		...a,
		...b
	}).every((key) => deepEqual(a[key], b[key], strict));
}
//#endregion
//#region ../../../vendor/cosmokit/src/time.ts
let Time;
(function(_Time) {
	_Time.millisecond = 1;
	const second = _Time.second = 1e3;
	const minute = _Time.minute = second * 60;
	const hour = _Time.hour = minute * 60;
	const day = _Time.day = hour * 24;
	const week = _Time.week = day * 7;
	let timezoneOffset = (/* @__PURE__ */ new Date()).getTimezoneOffset();
	function setTimezoneOffset(offset) {
		timezoneOffset = offset;
	}
	_Time.setTimezoneOffset = setTimezoneOffset;
	function getTimezoneOffset() {
		return timezoneOffset;
	}
	_Time.getTimezoneOffset = getTimezoneOffset;
	function getDateNumber(date = /* @__PURE__ */ new Date(), offset) {
		if (typeof date === "number") date = new Date(date);
		if (offset === void 0) offset = timezoneOffset;
		return Math.floor((date.valueOf() / minute - offset) / 1440);
	}
	_Time.getDateNumber = getDateNumber;
	function fromDateNumber(value, offset) {
		const date = new Date(value * day);
		if (offset === void 0) offset = timezoneOffset;
		return new Date(+date + offset * minute);
	}
	_Time.fromDateNumber = fromDateNumber;
	const numeric = /\d+(?:\.\d+)?/.source;
	const timeRegExp = new RegExp(`^${[
		"w(?:eek(?:s)?)?",
		"d(?:ay(?:s)?)?",
		"h(?:our(?:s)?)?",
		"m(?:in(?:ute)?(?:s)?)?",
		"s(?:ec(?:ond)?(?:s)?)?"
	].map((unit) => `(${numeric}${unit})?`).join("")}$`);
	function parseTime(source) {
		const capture = timeRegExp.exec(source);
		if (!capture) return 0;
		return (parseFloat(capture[1]) * week || 0) + (parseFloat(capture[2]) * day || 0) + (parseFloat(capture[3]) * hour || 0) + (parseFloat(capture[4]) * minute || 0) + (parseFloat(capture[5]) * second || 0);
	}
	_Time.parseTime = parseTime;
	function parseDate(date) {
		const parsed = parseTime(date);
		if (parsed) date = Date.now() + parsed;
		else if (/^\d{1,2}(:\d{1,2}){1,2}$/.test(date)) date = `${(/* @__PURE__ */ new Date()).toLocaleDateString()}-${date}`;
		else if (/^\d{1,2}-\d{1,2}-\d{1,2}(:\d{1,2}){1,2}$/.test(date)) date = `${(/* @__PURE__ */ new Date()).getFullYear()}-${date}`;
		return date ? new Date(date) : /* @__PURE__ */ new Date();
	}
	_Time.parseDate = parseDate;
	function format(ms) {
		const abs = Math.abs(ms);
		if (abs >= day - hour / 2) return Math.round(ms / day) + "d";
		else if (abs >= hour - minute / 2) return Math.round(ms / hour) + "h";
		else if (abs >= minute - second / 2) return Math.round(ms / minute) + "m";
		else if (abs >= second) return Math.round(ms / second) + "s";
		return ms + "ms";
	}
	_Time.format = format;
	function toDigits(source, length = 2) {
		return source.toString().padStart(length, "0");
	}
	_Time.toDigits = toDigits;
	function template(template, time = /* @__PURE__ */ new Date()) {
		return template.replace("yyyy", time.getFullYear().toString()).replace("yy", time.getFullYear().toString().slice(2)).replace("MM", toDigits(time.getMonth() + 1)).replace("dd", toDigits(time.getDate())).replace("hh", toDigits(time.getHours())).replace("mm", toDigits(time.getMinutes())).replace("ss", toDigits(time.getSeconds())).replace("SSS", toDigits(time.getMilliseconds(), 3));
	}
	_Time.template = template;
})(Time || (Time = {}));
//#endregion
//#region ../../../vendor/schemastery/src/index.ts
const kSchema = Symbol.for("schemastery");
const kValidationError = Symbol.for("ValidationError");
globalThis.__schemastery_index__ ??= 0;
globalThis.__schemastery_refs__ = void 0;
var ValidationError = class extends TypeError {
	options;
	name = "ValidationError";
	constructor(message, options) {
		let prefix = "$";
		for (const segment of options.path || []) if (typeof segment === "string") prefix += "." + segment;
		else if (typeof segment === "number") prefix += "[" + segment + "]";
		else if (typeof segment === "symbol") prefix += `[Symbol(${segment.toString()})]`;
		if (prefix.startsWith(".")) prefix = prefix.slice(1);
		super((prefix === "$" ? "" : `${prefix} `) + message);
		this.options = options;
	}
	static is(error) {
		return !!error?.[kValidationError];
	}
};
Object.defineProperty(ValidationError.prototype, kValidationError, { value: true });
const Schema = function(options) {
	const schema = function(data, options = {}) {
		return Schema.resolve(data, schema, options)[0];
	};
	if (options.refs) {
		const refs = mapValues(options.refs, (options) => new Schema(options));
		const getRef = (uid) => refs[uid];
		for (const key in refs) {
			const options = refs[key];
			options.sKey = getRef(options.sKey);
			options.inner = getRef(options.inner);
			options.list = options.list && options.list.map(getRef);
			options.dict = options.dict && mapValues(options.dict, getRef);
		}
		return refs[options.uid];
	}
	Object.assign(schema, options);
	if (typeof schema.callback === "string") try {
		schema.callback = new Function("return " + schema.callback)();
	} catch {}
	Object.defineProperty(schema, "uid", { value: globalThis.__schemastery_index__++ });
	Object.setPrototypeOf(schema, Schema.prototype);
	schema.meta ||= {};
	schema.toString = schema.toString.bind(schema);
	return schema;
};
Schema.prototype = Object.create(Function.prototype);
Schema.prototype[kSchema] = true;
Object.defineProperty(Schema.prototype, "~standard", { get() {
	return {
		version: 1,
		vendor: "schemastery",
		validate: (value) => {
			try {
				return { value: Schema.resolve(value, this, {})[0] };
			} catch (error) {
				if (ValidationError.is(error)) return { issues: [{
					message: error.message,
					path: error.options.path
				}] };
				throw error;
			}
		}
	};
} });
Schema.ValidationError = ValidationError;
Schema.prototype.toJSON = function toJSON() {
	if (globalThis.__schemastery_refs__) {
		globalThis.__schemastery_refs__[this.uid] ??= JSON.parse(JSON.stringify({ ...this }));
		return this.uid;
	}
	globalThis.__schemastery_refs__ = { [this.uid]: { ...this } };
	globalThis.__schemastery_refs__[this.uid] = JSON.parse(JSON.stringify({ ...this }));
	const result = {
		uid: this.uid,
		refs: globalThis.__schemastery_refs__
	};
	globalThis.__schemastery_refs__ = void 0;
	return result;
};
Schema.prototype.set = function set(key, value) {
	this.dict[key] = value;
	return this;
};
Schema.prototype.push = function push(value) {
	this.list.push(value);
	return this;
};
function mergeDesc(original, messages) {
	const result = typeof original === "string" ? { "": original } : { ...original };
	for (const locale in messages) {
		const value = messages[locale];
		if (value?.$description || value?.$desc) result[locale] = value.$description || value.$desc;
		else if (typeof value === "string") result[locale] = value;
	}
	return result;
}
function getInner(value) {
	return value?.$value ?? value?.$inner;
}
function extractKeys(data) {
	return filterKeys(data ?? {}, (key) => !key.startsWith("$"));
}
Schema.prototype.i18n = function i18n(messages) {
	const schema = Schema(this);
	const desc = mergeDesc(schema.meta.description, messages);
	if (Object.keys(desc).length) schema.meta.description = desc;
	if (schema.dict) schema.dict = mapValues(schema.dict, (inner, key) => {
		return inner.i18n(mapValues(messages, (data) => getInner(data)?.[key] ?? data?.[key]));
	});
	if (schema.list) schema.list = schema.list.map((inner, index) => {
		return inner.i18n(mapValues(messages, (data = {}) => {
			if (Array.isArray(getInner(data))) return getInner(data)[index];
			if (Array.isArray(data)) return data[index];
			return extractKeys(data);
		}));
	});
	if (schema.inner) schema.inner = schema.inner.i18n(mapValues(messages, (data) => {
		if (getInner(data)) return getInner(data);
		return extractKeys(data);
	}));
	if (schema.sKey) schema.sKey = schema.sKey.i18n(mapValues(messages, (data) => data?.$key));
	return schema;
};
Schema.prototype.extra = function extra(key, value) {
	const schema = Schema(this);
	schema.meta = {
		...schema.meta,
		[key]: value
	};
	return schema;
};
for (const key of [
	"required",
	"disabled",
	"collapse",
	"hidden",
	"loose"
]) Object.assign(Schema.prototype, { [key](value = true) {
	const schema = Schema(this);
	schema.meta = {
		...schema.meta,
		[key]: value
	};
	return schema;
} });
Schema.prototype.deprecated = function deprecated() {
	const schema = Schema(this);
	schema.meta.badges ||= [];
	schema.meta.badges.push({
		text: "deprecated",
		type: "danger"
	});
	return schema;
};
Schema.prototype.experimental = function experimental() {
	const schema = Schema(this);
	schema.meta.badges ||= [];
	schema.meta.badges.push({
		text: "experimental",
		type: "warning"
	});
	return schema;
};
Schema.prototype.pattern = function pattern(regexp) {
	const schema = Schema(this);
	const pattern = pick(regexp, ["source", "flags"]);
	schema.meta = {
		...schema.meta,
		pattern
	};
	return schema;
};
Schema.prototype.simplify = function simplify(value) {
	if (deepEqual(value, this.meta.default, this.type === "dict")) return null;
	if (isNullable(value)) return value;
	if (this.type === "object" || this.type === "dict") {
		const result = {};
		for (const key in value) {
			const item = (this.type === "object" ? this.dict[key] : this.inner)?.simplify(value[key]);
			if (this.type === "dict" || !isNullable(item)) result[key] = item;
		}
		if (deepEqual(result, this.meta.default, this.type === "dict")) return null;
		return result;
	} else if (this.type === "array" || this.type === "tuple") {
		const result = [];
		value.forEach((value, index) => {
			const schema = this.type === "array" ? this.inner : this.list[index];
			const item = schema ? schema.simplify(value) : value;
			result.push(item);
		});
		return result;
	} else if (this.type === "intersect") {
		const result = {};
		for (const item of this.list) Object.assign(result, item.simplify(value));
		return result;
	} else if (this.type === "union") for (const schema of this.list) try {
		Schema.resolve(value, schema, {});
		return schema.simplify(value);
	} catch {}
	return value;
};
Schema.prototype.toString = function toString(inline) {
	return formatters[this.type]?.(this, inline) ?? `Schema<${this.type}>`;
};
Schema.prototype.role = function role(role, extra) {
	const schema = Schema(this);
	schema.meta = {
		...schema.meta,
		role,
		extra
	};
	return schema;
};
for (const key of [
	"default",
	"link",
	"comment",
	"description",
	"max",
	"min",
	"step"
]) Object.assign(Schema.prototype, { [key](value) {
	const schema = Schema(this);
	schema.meta = {
		...schema.meta,
		[key]: value
	};
	return schema;
} });
const resolvers = {};
Schema.extend = function extend(type, resolve) {
	resolvers[type] = resolve;
};
Schema.resolve = function resolve(data, schema, options = {}, strict = false) {
	if (!schema) return [data];
	if (options.ignore?.(data, schema)) return [data];
	if (isNullable(data) && schema.type !== "lazy") {
		if (schema.meta.required) throw new ValidationError(`missing required value`, options);
		let current = schema;
		let fallback = schema.meta.default;
		while (current?.type === "intersect" && isNullable(fallback)) {
			current = current.list[0];
			fallback = current?.meta.default;
		}
		if (isNullable(fallback)) return [data];
		data = clone(fallback);
	}
	const callback = resolvers[schema.type];
	if (!callback) throw new ValidationError(`unsupported type "${schema.type}"`, options);
	try {
		return callback(data, schema, options, strict);
	} catch (error) {
		if (!schema.meta.loose) throw error;
		return [schema.meta.default];
	}
};
Schema.from = function from(source) {
	if (isNullable(source)) return Schema.any();
	else if ([
		"string",
		"number",
		"boolean"
	].includes(typeof source)) return Schema.const(source).required();
	else if (source[kSchema]) return source;
	else if (typeof source === "function") switch (source) {
		case String: return Schema.string().required();
		case Number: return Schema.number().required();
		case Boolean: return Schema.boolean().required();
		case Function: return Schema.function().required();
		default: return Schema.is(source).required();
	}
	else throw new TypeError(`cannot infer schema from ${source}`);
};
Schema.lazy = function lazy(builder) {
	const toJSON = () => {
		if (!schema.inner[kSchema]) {
			schema.inner = schema.builder();
			schema.inner.meta = {
				...schema.meta,
				...schema.inner.meta
			};
		}
		return schema.inner.toJSON();
	};
	const schema = new Schema({
		type: "lazy",
		builder,
		inner: { toJSON }
	});
	return schema;
};
Schema.natural = function natural() {
	return Schema.number().step(1).min(0);
};
Schema.percent = function percent() {
	return Schema.number().step(.01).min(0).max(1).role("slider");
};
Schema.date = function date() {
	return Schema.union([Schema.is(Date), Schema.transform(Schema.string().role("datetime"), (value, options) => {
		const date = new Date(value);
		if (isNaN(+date)) throw new ValidationError(`invalid date "${value}"`, options);
		return date;
	}, true)]);
};
Schema.regExp = function regExp(flag = "") {
	return Schema.union([Schema.is(RegExp), Schema.transform(Schema.string().role("regexp", { flag }), (value, options) => {
		try {
			return new RegExp(value, flag);
		} catch (e) {
			throw new ValidationError(e.message, options);
		}
	}, true)]);
};
Schema.arrayBuffer = function arrayBuffer(encoding) {
	return Schema.union([
		Schema.is(ArrayBuffer),
		Schema.is(SharedArrayBuffer),
		Schema.transform(Schema.any(), (value, options) => {
			if (Binary.isSource(value)) return Binary.fromSource(value);
			throw new ValidationError(`expected ArrayBufferSource but got ${value}`, options);
		}, true),
		...encoding ? [Schema.transform(Schema.string(), (value, options) => {
			try {
				return encoding === "base64" ? Binary.fromBase64(value) : Binary.fromHex(value);
			} catch (e) {
				throw new ValidationError(e.message, options);
			}
		}, true)] : []
	]);
};
Schema.extend("lazy", (data, schema, options, strict) => {
	if (!schema.inner[kSchema]) {
		schema.inner = schema.builder();
		schema.inner.meta = {
			...schema.meta,
			...schema.inner.meta
		};
	}
	return Schema.resolve(data, schema.inner, options, strict);
});
Schema.extend("any", (data) => {
	return [data];
});
Schema.extend("never", (data, _, options) => {
	throw new ValidationError(`expected nullable but got ${data}`, options);
});
Schema.extend("const", (data, { value }, options) => {
	if (deepEqual(data, value)) return [value];
	throw new ValidationError(`expected ${value} but got ${data}`, options);
});
function checkWithinRange(data, meta, description, options, skipMin = false) {
	const { max = Infinity, min = -Infinity } = meta;
	if (data > max) throw new ValidationError(`expected ${description} <= ${max} but got ${data}`, options);
	if (data < min && !skipMin) throw new ValidationError(`expected ${description} >= ${min} but got ${data}`, options);
}
Schema.extend("string", (data, { meta }, options) => {
	if (typeof data !== "string") throw new ValidationError(`expected string but got ${data}`, options);
	if (meta.pattern) {
		const regexp = new RegExp(meta.pattern.source, meta.pattern.flags);
		if (!regexp.test(data)) throw new ValidationError(`expect string to match regexp ${regexp}`, options);
	}
	checkWithinRange(data.length, meta, "string length", options);
	return [data];
});
function decimalShift(data, digits) {
	const str = data.toString();
	if (str.includes("e")) return data * Math.pow(10, digits);
	const index = str.indexOf(".");
	if (index === -1) return data * Math.pow(10, digits);
	const frac = str.slice(index + 1);
	const integer = str.slice(0, index);
	if (frac.length <= digits) return +(integer + frac.padEnd(digits, "0"));
	return +(integer + frac.slice(0, digits) + "." + frac.slice(digits));
}
function isMultipleOf(data, min, step) {
	step = Math.abs(step);
	if (!/^\d+\.\d+$/.test(step.toString())) return (data - min) % step === 0;
	const index = step.toString().indexOf(".");
	const digits = step.toString().slice(index + 1).length;
	return Math.abs(decimalShift(data, digits) - decimalShift(min, digits)) % decimalShift(step, digits) === 0;
}
Schema.extend("number", (data, { meta }, options) => {
	if (typeof data !== "number") throw new ValidationError(`expected number but got ${data}`, options);
	checkWithinRange(data, meta, "number", options);
	const { step } = meta;
	if (step && !isMultipleOf(data, meta.min ?? 0, step)) throw new ValidationError(`expected number multiple of ${step} but got ${data}`, options);
	return [data];
});
Schema.extend("boolean", (data, _, options) => {
	if (typeof data === "boolean") return [data];
	throw new ValidationError(`expected boolean but got ${data}`, options);
});
Schema.extend("bitset", (data, { bits, meta }, options) => {
	let value = 0, keys = [];
	if (typeof data === "number") {
		value = data;
		for (const key in bits) if (data & bits[key]) keys.push(key);
	} else if (Array.isArray(data)) {
		keys = data;
		for (const key of keys) {
			if (typeof key !== "string") throw new ValidationError(`expected string but got ${key}`, options);
			if (key in bits) value |= bits[key];
		}
	} else throw new ValidationError(`expected number or array but got ${data}`, options);
	if (value === meta.default) return [value];
	return [value, keys];
});
Schema.extend("function", (data, _, options) => {
	if (typeof data === "function") return [data];
	throw new ValidationError(`expected function but got ${data}`, options);
});
Schema.extend("is", (data, { constructor }, options) => {
	if (typeof constructor === "function") {
		if (data instanceof constructor) return [data];
		throw new ValidationError(`expected ${constructor.name} but got ${data}`, options);
	} else {
		if (isNullable(data)) throw new ValidationError(`expected ${constructor} but got ${data}`, options);
		let prototype = Object.getPrototypeOf(data);
		while (prototype) {
			if (prototype.constructor?.name === constructor) return [data];
			prototype = Object.getPrototypeOf(prototype);
		}
		throw new ValidationError(`expected ${constructor} but got ${data}`, options);
	}
});
function property(data, key, schema, options) {
	try {
		const [value, adapted] = Schema.resolve(data[key], schema, {
			...options,
			path: [...options.path || [], key]
		});
		if (adapted !== void 0) data[key] = adapted;
		return value;
	} catch (e) {
		if (!options?.autofix) throw e;
		delete data[key];
		return schema.meta.default;
	}
}
Schema.extend("array", (data, { inner, meta }, options) => {
	if (!Array.isArray(data)) throw new ValidationError(`expected array but got ${data}`, options);
	checkWithinRange(data.length, meta, "array length", options, !isNullable(inner.meta.default));
	return [data.map((_, index) => property(data, index, inner, options))];
});
Schema.extend("dict", (data, { inner, sKey }, options, strict) => {
	if (!isPlainObject(data)) throw new ValidationError(`expected object but got ${data}`, options);
	const result = {};
	for (const key in data) {
		let rKey;
		try {
			rKey = Schema.resolve(key, sKey, options)[0];
		} catch (error) {
			if (strict) continue;
			throw error;
		}
		result[rKey] = property(data, key, inner, options);
		data[rKey] = data[key];
		if (key !== rKey) delete data[key];
	}
	return [result];
});
Schema.extend("tuple", (data, { list }, options, strict) => {
	if (!Array.isArray(data)) throw new ValidationError(`expected array but got ${data}`, options);
	const result = list.map((inner, index) => property(data, index, inner, options));
	if (strict) return [result];
	result.push(...data.slice(list.length));
	return [result];
});
function merge(result, data) {
	for (const key in data) {
		if (key in result) continue;
		result[key] = data[key];
	}
}
Schema.extend("object", (data, { dict }, options, strict) => {
	if (!isPlainObject(data)) throw new ValidationError(`expected object but got ${data}`, options);
	const result = {};
	for (const key in dict) {
		const value = property(data, key, dict[key], options);
		if (!isNullable(value) || key in data) result[key] = value;
	}
	if (!strict) merge(result, data);
	return [result];
});
Schema.extend("union", (data, { list, toString }, options, strict) => {
	const messages = [];
	for (const inner of list) try {
		return Schema.resolve(data, inner, options, strict);
	} catch (error) {
		messages.push(error);
	}
	throw new ValidationError(`expected ${toString()} but got ${JSON.stringify(data)}`, options);
});
Schema.extend("intersect", (data, { list, toString }, options, strict) => {
	if (!list.length) return [data];
	let result;
	for (const inner of list) {
		const value = Schema.resolve(data, inner, options, true)[0];
		if (isNullable(value)) continue;
		if (isNullable(result)) result = value;
		else if (typeof result !== typeof value) throw new ValidationError(`expected ${toString()} but got ${JSON.stringify(data)}`, options);
		else if (typeof value === "object") merge(result ??= {}, value);
		else if (result !== value) throw new ValidationError(`expected ${toString()} but got ${JSON.stringify(data)}`, options);
	}
	if (!strict && isPlainObject(data)) merge(result, data);
	return [result];
});
Schema.extend("transform", (data, { inner, callback, preserve }, options) => {
	const [result, adapted = data] = Schema.resolve(data, inner, options, true);
	if (preserve) return [callback(result)];
	else return [callback(result), callback(adapted)];
});
const formatters = {};
function defineMethod(name, keys, format) {
	formatters[name] = format;
	Object.assign(Schema, { [name](...args) {
		const schema = new Schema({ type: name });
		keys.forEach((key, index) => {
			switch (key) {
				case "sKey":
					schema.sKey = args[index] ?? Schema.string();
					break;
				case "inner":
					schema.inner = Schema.from(args[index]);
					break;
				case "list":
					schema.list = args[index].map(Schema.from);
					break;
				case "dict":
					schema.dict = mapValues(args[index], Schema.from);
					break;
				case "bits":
					schema.bits = {};
					for (const key in args[index]) {
						if (typeof args[index][key] !== "number") continue;
						schema.bits[key] = args[index][key];
					}
					break;
				case "callback": {
					const callback = schema.callback = args[index];
					callback["toJSON"] ||= () => callback.toString();
					break;
				}
				case "constructor": {
					const constructor = schema.constructor = args[index];
					if (typeof constructor === "function") constructor["toJSON"] ||= () => constructor["name"];
					break;
				}
				default: schema[key] = args[index];
			}
		});
		if (name === "object" || name === "dict") schema.meta.default = {};
		else if (name === "array" || name === "tuple") schema.meta.default = [];
		else if (name === "bitset") schema.meta.default = 0;
		return schema;
	} });
}
defineMethod("is", ["constructor"], ({ constructor }) => {
	if (typeof constructor === "function") return constructor.name;
	else return constructor;
});
defineMethod("any", [], () => "any");
defineMethod("never", [], () => "never");
defineMethod("const", ["value"], ({ value }) => typeof value === "string" ? JSON.stringify(value) : value);
defineMethod("string", [], () => "string");
defineMethod("number", [], () => "number");
defineMethod("boolean", [], () => "boolean");
defineMethod("bitset", ["bits"], () => "bitset");
defineMethod("function", [], () => "function");
defineMethod("array", ["inner"], ({ inner }) => `${inner.toString(true)}[]`);
defineMethod("dict", ["inner", "sKey"], ({ inner, sKey }) => `{ [key: ${sKey.toString()}]: ${inner.toString()} }`);
defineMethod("tuple", ["list"], ({ list }) => `[${list.map((inner) => inner.toString()).join(", ")}]`);
defineMethod("object", ["dict"], ({ dict }) => {
	if (Object.keys(dict).length === 0) return "{}";
	return `{ ${Object.entries(dict).map(([key, inner]) => {
		return `${key}${inner.meta.required ? "" : "?"}: ${inner.toString()}`;
	}).join(", ")} }`;
});
defineMethod("union", ["list"], ({ list }, inline) => {
	const result = list.map(({ toString: format }) => format()).join(" | ");
	return inline ? `(${result})` : result;
});
defineMethod("intersect", ["list"], ({ list }) => {
	return `${list.map((inner) => inner.toString(true)).join(" & ")}`;
});
defineMethod("transform", [
	"inner",
	"callback",
	"preserve"
], ({ inner }, isInner) => inner.toString(isInner));
//#endregion
//#region lib/types/plugin.js
/** 微信 iLink Cordis 插件：校验配置并把连接器生命周期交给 effect。 */
/** Cordis function-plugin 名称。 */
const name = "webhook-weixin";
/**
* 连接器所需的 Harness 原生服务。
* 重构设计要求（§8.2）：依赖必须在装配期声明，缺失时**启动即失败**，
* 而不是等第一条消息到达才暴露。
*/
const inject = [
	"agents",
	"agentPresets",
	"sessionPersistence"
];
/** Schemastery 配置定义，保持模型/API 配置与代码分离。 */
const Config = Schema.object({
	base_url: Schema.string().required(),
	token: Schema.string().role("secret").required(),
	cdn_base_url: Schema.string(),
	state_file: Schema.string(),
	workspace_path: Schema.string().required(),
	provider: Schema.string().required(),
	model: Schema.string().required(),
	reasoning_effort: Schema.string(),
	agent_preset: Schema.string(),
	send_retry_count: Schema.number(),
	send_retry_delay_ms: Schema.number(),
	state_db: Schema.string(),
	lease_marker: Schema.string()
});
/** 注册微信连接器，并把长轮询与生命周期交给 Cordis effect。 */
function apply(ctx, config) {
	if (!config.base_url.startsWith("http")) throw new Error("webhook-weixin base_url must be an HTTP URL");
	if (!config.token.trim()) throw new Error("webhook-weixin token must be non-empty");
	probe("lifecycle", "plugin.apply", {
		base_url: config.base_url,
		preset: config.agent_preset ?? "(default)",
		provider: config.provider,
		model: config.model,
		state_db: config.state_db ?? "(default)"
	});
	install_weixin_connector(ctx, config);
}
//#endregion
export { Config, ConsumerLease, GRAPH_ASSISTANT_NAME, GRAPH_USER_NAME, HarnessSessionHost, INFLIGHT_TURN_PREFIX, IlinkClient, InboundCoordinator, KnowledgeGraphStore, MessageNormalizer, OutboundCoordinator, ReplyAggregator, STATE_SCHEMA_VERSION, TimelineStore, TurnContextStore, WeixinConnector, WeixinIngress, WeixinStateStore, apply, decrypt_aes_ecb, get_shared_memory, inject, install_weixin_connector, is_cursor_committable, lease_holder_alive, merge_messages, migrate_legacy_route_state, name, parse_aes_key, parse_control_command, preflight_openclaw_isolation, probe, probe_enabled, probe_timer, register_graph_tools, register_timeline_tools, set_shared_memory, trace_to_file, transcode_silk_to_wav };
