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
import { probe } from "../diagnostics/probe.js";
import { wait_for_free_vram } from "./gpuMonitor.js";
/**
 * 创建闸门。
 *
 * @param options - 并发与排队上限。
 */
export function create_gemma_gate(options) {
    const concurrency = Math.max(1, options?.concurrency ?? 1);
    const max_queue = Math.max(0, options?.max_queue ?? 20);
    const wait_timeout_ms = options?.wait_timeout_ms ?? 600_000;
    let active = 0;
    const queue = [];
    const release = () => {
        active -= 1;
        const next = queue.shift();
        if (next !== undefined)
            next();
    };
    return {
        async run(label, fn) {
            // 先等显存空出来（**在拿并发槽之前**等：等显存不该占着槽位）。
            // 超时说明显卡一直被别的东西占着，本轮就放弃——后台任务晚一轮做没有损失，
            // 硬启动却可能把用户正在跑的东西一起拖垮。
            const vram_ok = await wait_for_free_vram({
                min_free_mb: options?.min_free_vram_mb ?? 5_200,
                timeout_ms: options?.vram_wait_ms ?? 600_000,
                poll_ms: options?.vram_poll_ms ?? 30_000,
                label,
            });
            if (!vram_ok) {
                throw new Error(`显存一直被占用，暂缓「${label}」（等不到至少 ${options?.min_free_vram_mb ?? 5_200}MB 空闲）。稍后会自动重试。`);
            }
            // 显存够了再确保服务在跑（启动器当时可能因为显存忙而没拉起）
            if (options?.ensure_ready !== undefined)
                await options.ensure_ready();
            if (active >= concurrency) {
                if (queue.length >= max_queue) {
                    probe('gemma', 'gate.rejected', { label, active, queued: queue.length });
                    throw new Error(`Gemma 调用排队已满（${max_queue}），本次「${label}」跳过。`);
                }
                await new Promise((resolve, reject) => {
                    const timer = setTimeout(() => {
                        // 超时后把占位从队列里摘掉，避免它稍后又被唤醒
                        const at = queue.indexOf(entry);
                        if (at >= 0)
                            queue.splice(at, 1);
                        reject(new Error(`等待 Gemma 闸门超时（${wait_timeout_ms}ms）：${label}`));
                    }, wait_timeout_ms);
                    const entry = () => { clearTimeout(timer); resolve(); };
                    queue.push(entry);
                });
            }
            active += 1;
            probe('gemma', 'gate.enter', { label, active, queued: queue.length });
            try {
                return await fn();
            }
            finally {
                release();
                probe('gemma', 'gate.leave', { label, active, queued: queue.length });
            }
        },
        stats: () => ({ active, queued: queue.length }),
    };
}
//# sourceMappingURL=gemmaGate.js.map