"""提醒工具（MCP stdio）：让欣爱能创建/查看/删除"到点必达"的提醒。

为什么不用 harness 自带 schedule：那是会话内定时器，会话不活跃/重启后就不响
（今天的吃药提醒就是这样丢的）。这里直接写独立提醒库，由后台服务轮询、QQ 直推。

工具：
    remind_add     text + (at | dailyAt | inSeconds)
    remind_list    列出待触发
    remind_delete  id
"""

import io
import json
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
sys.path.insert(0, str(ROOT / "reminders"))

import reminderService as svc  # noqa: E402

TOOLS = [
    {
        "name": "remind_add",
        "description": "创建一条到点必达的提醒（独立服务推送，重启发消息都不影响）。时间三选一：dailyAt='12:00' 每天；at='2026-10-27 12:00' 一次性；inSeconds=600 多少秒后。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "提醒内容，例如：该吃药了"},
                "dailyAt": {"type": "string", "description": "每天几点，HH:MM"},
                "at": {"type": "string", "description": "一次性时间，YYYY-MM-DD HH:MM"},
                "inSeconds": {"type": "integer", "description": "多少秒后提醒（测试用）"},
            },
            "required": ["text"],
        },
    },
    {
        "name": "remind_list",
        "description": "列出所有待触发的提醒。",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "remind_delete",
        "description": "按 id 删除提醒。",
        "inputSchema": {
            "type": "object",
            "properties": {"id": {"type": "integer", "description": "提醒 id（remind_list 里能看到）"}},
            "required": ["id"],
        },
    },
]


def handle(message):
    """JSON-RPC 分发。"""
    method = message.get("method")
    request_id = message.get("id")
    if method == "initialize":
        return {"jsonrpc": "2.0", "id": request_id, "result": {
            "protocolVersion": "2025-06-18", "capabilities": {"tools": {}},
            "serverInfo": {"name": "reminders", "version": "1.0.0"}}}
    if method in ("notifications/initialized", "notifications/cancelled"):
        return None
    if method == "ping":
        return {"jsonrpc": "2.0", "id": request_id, "result": {}}
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": request_id, "result": {"tools": TOOLS}}
    if method == "tools/call":
        params = message.get("params") or {}
        name = params.get("name")
        args = params.get("arguments") or {}
        try:
            if name == "remind_add":
                reminder_id, next_at = svc.add(
                    str(args.get("text", "")).strip(),
                    at=args.get("at") or None,
                    daily=args.get("dailyAt") or None,
                    in_seconds=args.get("inSeconds") or None,
                )
                text = f"已创建提醒 #{reminder_id}，下次触发：{next_at}"
            elif name == "remind_list":
                rows = svc.pending()
                text = "\n".join(
                    f"#{row[0]} {row[2]} {'每天 ' + row[3] if row[3] else '一次性'} {row[1]}" for row in rows
                ) or "（没有待触发的提醒）"
            elif name == "remind_delete":
                text = "已删除" if svc.remove(int(args.get("id"))) else "没找到该提醒"
            else:
                raise ValueError(f"未知工具：{name}")
            return {"jsonrpc": "2.0", "id": request_id, "result": {"content": [{"type": "text", "text": text}]}}
        except Exception as error:  # noqa: BLE001
            return {"jsonrpc": "2.0", "id": request_id, "result": {
                "content": [{"type": "text", "text": f"操作失败：{type(error).__name__}: {error}"}],
                "isError": True}}
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": -32601, "message": f"unsupported: {method}"}}


def main():
    """stdio 循环。"""
    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        response = handle(message)
        if response is not None:
            sys.stdout.write(json.dumps(response, ensure_ascii=False) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
