/**
 * 时间线（长期记忆）的类型定义。
 *
 * 结构约定（用户口径）：**以日期为索引，24 小时为一个 node**。每个 node 里有两部分：
 * 1. 该 24 小时内"用户与系统对话的摘要"（`summary`，可人工编辑）；
 * 2. 该 24 小时内"切实的聊天记录"（`entries`：用户上行 + 系统最终回复；
 *    模型推理是否落盘由 `persist_reasoning` 决定，暂定不存）。
 *
 * 这里的类型只描述**数据结构**，不绑定存储实现：当前是文件后端
 * （`days/<date>.jsonl` + `<date>.summary.md` + 生成的 `<date>.md`），
 * 之后要换 MySQL 只需换掉 TimelineStore 的实现，类型与调用方不变。
 *
 * @module dsh-webhook-weixin/timeline-types
 */
export {};
//# sourceMappingURL=timelineTypes.js.map