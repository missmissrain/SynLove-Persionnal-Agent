"""新闻摘要的 MCP 工具服务：让欣爱能查/改/停/删定时任务，并能立刻跑一次。

职责：
- 以 **MCP stdio 服务**的形式挂在 harness 里（和 windows-mcp / playwright 相同的挂载方式）；
- 对外只暴露两个工具，避免工具表膨胀：
    · `digest_schedule` —— action=get|set|disable|delete；
      set 可以改时间、启停、关注画像、条数、字数（改配置会落到 newsDigest.json）；
    · `digest_run` —— 立刻跑一次采集+筛选+推送（dry_run=true 时只生成不发送）；
- 定时任务本身用 Windows 计划任务 `SynLoveNewsDigest` 承载（schtasks），
  所以"删除"是删任务、"停用"是 /DISABLE（保留定义，便于恢复）。

启动参数（可选环境变量）：
    DIGEST_TASK_NAME   计划任务名，默认 SynLoveNewsDigest
    PYTHON             python 解释器，默认当前解释器（可用环境变量覆盖）
"""

import io
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent                      # 项目根目录（微信Harness链路）
CONFIG_PATH = ROOT / "newsDigest" / "config" / "newsDigest.json"
RUN_SCRIPT = ROOT / "newsDigest" / "runDigest.py"
TASK_NAME = os.environ.get("DIGEST_TASK_NAME", "SynLoveNewsDigest")
PYTHON = os.environ.get("PYTHON") or sys.executable

TOOLS = [
    {
        "name": "digest_schedule",
        "description": (
            "管理「每日新闻摘要」的定时任务。"
            "action=get 查当前状态；set 设置/修改（可同时改时间和关注方向）；"
            "disable 停用但保留设置；delete 删除任务。"
            "时间用 24 小时制 HH:MM，例如 08:00。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "action": {"type": "string", "enum": ["get", "set", "disable", "delete"]},
                "time": {"type": "string", "description": "每天几点跑，形如 08:00（action=set 时用）"},
                "interest": {"type": "string", "description": "关注方向，比如'关心AI、健康和本市民生，不看体育娱乐'"},
                "maxItems": {"type": "integer", "description": "一条消息最多写几条新闻（1-8）"},
                "maxChars": {"type": "integer", "description": "消息字数上限（60-400）"},
            },
            "required": ["action"],
        },
    },
    {
        "name": "digest_run",
        "description": "立刻跑一次新闻摘要（采集→筛选→推送）。dry_run=true 时只生成不发送，用于先看效果。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dry_run": {"type": "boolean", "description": "true=只生成不推送"},
            },
        },
    },
]


def log(message):
    """把调试信息写到 stderr（不能写 stdout，那是 MCP 协议通道）。"""
    sys.stderr.write(f"[digest-mcp] {message}\n")
    sys.stderr.flush()


def read_config():
    """读 newsDigest.json（容忍 BOM），失败返回空 dict。"""
    try:
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return {}


def write_config(config):
    """写回 newsDigest.json（UTF-8 无 BOM、两空格缩进）。"""
    CONFIG_PATH.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run_schtasks(args):
    """调用 schtasks 并返回 (退出码, 输出)。

    两个必须：`stdin=DEVNULL`（/Change 遇到改账户相关参数会**弹密码提示**，
    在 MCP 里 stdin 是协议管道，会被卡死）+ 超时兜底；输出按系统本地编码解（中文 Windows 是 GBK）。
    """
    result = subprocess.run(
        ["schtasks"] + args, stdin=subprocess.DEVNULL, capture_output=True, timeout=90,
    )
    output = b""
    if result.stdout:
        output += result.stdout
    if result.stderr:
        output += result.stderr
    if output[:2] in (b"\xff\xfe", b"\xfe\xff"):
        text = output.decode("utf-16", errors="replace")
    else:
        text = output.decode("mbcs", errors="replace") if sys.platform == "win32" else output.decode("utf-8", errors="replace")
    return result.returncode, text


def run_powershell(script):
    """执行一段 PowerShell 命令，返回 (退出码, 输出文本)。"""
    result = subprocess.run(
        ["powershell.exe", "-NoLogo", "-NoProfile", "-Command", script],
        stdin=subprocess.DEVNULL, capture_output=True, timeout=120,
    )
    output = (result.stdout or b"") + (result.stderr or b"")
    if output[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return result.returncode, output.decode("utf-16", errors="replace")
    return result.returncode, output.decode("mbcs", errors="replace")


def task_xml():
    """用 /XML 取任务定义（标签是 ASCII，不受系统语言影响），返回 None 表示任务不存在。"""
    result = subprocess.run(
        ["schtasks", "/Query", "/TN", TASK_NAME, "/XML"], stdin=subprocess.DEVNULL, capture_output=True, timeout=60,
    )
    if result.returncode != 0 or not result.stdout:
        return None
    data = result.stdout
    if data[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return data.decode("utf-16", errors="replace")
    if data[:3] == b"\xef\xbb\xbf":
        return data[3:].decode("utf-8", errors="replace")
    return data.decode("utf-8", errors="replace")


def task_query():
    """查询任务是否存在、每天几点跑、是否启用，返回 (是否存在, dict)。

    为什么不用 /FO LIST：那套字段名会随系统语言变（中文 Windows 输出"下次运行时间"），
    解码与匹配都容易错；XML 里 `<StartBoundary>` / `<Enabled>` 是固定标签，稳定。
    """
    xml = task_xml()
    if xml is None:
        return False, {}
    start = re.search(r"<StartBoundary>([^<]+)</StartBoundary>", xml)
    enabled = re.search(r"<Enabled>([^<]+)</Enabled>", xml)
    info = {}
    if start is not None:
        boundary = start.group(1)
        clock = boundary.split("T", 1)[1][:5] if "T" in boundary else boundary
        info["每天"] = clock
    info["是否启用"] = "否" if enabled is not None and enabled.group(1).strip().lower() == "false" else "是"
    return True, info


def task_create(hhmm):
    """按每天 hhmm 创建计划任务（已存在则走 task_change）。"""
    cmd = ROOT / "newsDigest" / "runDigestTask.cmd"
    return run_schtasks(["/Create", "/F", "/SC", "DAILY", "/ST", hhmm, "/TN", TASK_NAME, "/TR", f'"{cmd}"'])


def task_change(hhmm):
    """修改计划任务的时间。

    为什么不用 `schtasks /Change /ST`：实测它会**弹"请输入密码"**（改任务参数要凭据），
    在 MCP 进程里 stdin 是协议管道，会被直接卡死。PowerShell 的 Set-ScheduledTask
    走任务计划服务接口，改当前用户的任务不需要密码。
    """
    script = (
        f"Set-ScheduledTask -TaskName '{TASK_NAME}' "
        f"-Trigger (New-ScheduledTaskTrigger -Daily -At {hhmm}) | Out-Null"
    )
    code, output = run_powershell(script)
    if code != 0:
        # 兜底：任务可能是用 schtasks 建的，Set-ScheduledTask 找不到时再试 schtasks（带 DEVNULL 不会卡）
        return run_schtasks(["/Change", "/TN", TASK_NAME, "/ST", hhmm])
    return code, output


def task_enable(enable):
    """启用/停用计划任务（schtasks 的 /ENABLE、/DISABLE 不需要密码）。"""
    flag = "/ENABLE" if enable else "/DISABLE"
    return run_schtasks(["/Change", "/TN", TASK_NAME, flag])


def schedule_report():
    """拼一段人类可读的状态说明（给模型看，用于回复用户）。"""
    config = read_config()
    exists, info = task_query()
    lines = []
    if exists:
        lines.append(f"任务存在：{TASK_NAME}")
        clock = info.get("每天", "")
        lines.append(f"  运行时间：每天 {clock}（{info.get('是否启用', '是')}启用）" if clock else f"  是否启用：{info.get('是否启用', '?')}")
    else:
        lines.append(f"任务不存在：{TASK_NAME}")
    lines.append(f"关注方向：{config.get('interest', '(未配置)')}")
    lines.append(f"条数/字数：{config.get('maxItems', 3)} 条 / {config.get('maxChars', 120)} 字")
    sources = config.get("sources") or []
    if sources:
        lines.append("采集源：" + "、".join(str(item.get("name", "?")) for item in sources))
    return "\n".join(lines)


def tool_digest_schedule(arguments):
    """处理 digest_schedule：get/set/disable/delete。"""
    action = str(arguments.get("action", "get"))
    if action == "get":
        return schedule_report()

    if action == "delete":
        code, output = run_schtasks(["/Delete", "/TN", TASK_NAME, "/F"])
        if code != 0 and "cannot find" not in output.lower() and "找不到" not in output:
            raise RuntimeError(f"删除失败：{output.strip()[:300]}")
        return "已删除定时任务。需要恢复时让我 set 一次即可。"

    if action in ("disable", "enable"):
        code, output = task_enable(action == "enable")
        if code != 0:
            raise RuntimeError(f"{'启用' if action == 'enable' else '停用'}失败：{output.strip()[:300]}")
        return "已恢复每日运行" if action == "enable" else "已停用（保留设置，随时可以 enable）"

    if action != "set":
        raise ValueError(f"未知 action：{action}")

    # set：先改配置，再动任务
    config = read_config()
    if "interest" in arguments and str(arguments["interest"]).strip():
        config["interest"] = str(arguments["interest"]).strip()
    if "maxItems" in arguments:
        value = int(arguments["maxItems"])
        if not 1 <= value <= 8:
            raise ValueError("maxItems 要在 1~8 之间")
        config["maxItems"] = value
    if "maxChars" in arguments:
        value = int(arguments["maxChars"])
        if not 60 <= value <= 400:
            raise ValueError("maxChars 要在 60~400 之间")
        config["maxChars"] = value
    write_config(config)

    hhmm = str(arguments.get("time", "")).strip()
    enabled = arguments.get("enabled", True)
    if enabled is False:
        code, output = task_enable(False)
        if code != 0:
            raise RuntimeError(f"停用失败：{output.strip()[:300]}")
        return "已停用定时任务（设置已更新）。"
    if enabled is True and str(arguments.get("time", "")).strip() == "":
        task_enable(True)

    if hhmm:
        if not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", hhmm):
            raise ValueError("time 要写成 HH:MM，例如 08:30")
        exists, _ = task_query()
        code, output = task_change(hhmm) if exists else task_create(hhmm)
        if code != 0:
            raise RuntimeError(f"设置时间失败：{output.strip()[:300]}")
        # 改了时间通常意味着"要它跑"：顺手恢复启用，避免上次 disable 后改时间仍是停用状态
        task_enable(True)
        return f"已设置为每天 {hhmm} 运行。\n" + schedule_report()

    exists, _ = task_query()
    if not exists:
        return "配置已更新，但定时任务不存在（没有给 time）。需要的话告诉我几点跑。"
    return "配置已更新。\n" + schedule_report()


def tool_digest_run(arguments):
    """处理 digest_run：同步跑一次 runDigest.py，返回日志尾部。"""
    command = [PYTHON, str(RUN_SCRIPT)]
    if bool(arguments.get("dry_run")):
        command.append("--dry-run")
    env = dict(os.environ, PYTHONUTF8="1", PYTHONIOENCODING="utf-8")
    started = time.time()
    result = subprocess.run(
        command, cwd=str(ROOT), capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=900, env=env,
    )
    elapsed = time.time() - started
    output = ((result.stdout or "") + (result.stderr or "")).strip()
    tail = "\n".join(output.splitlines()[-12:])
    status = "成功" if result.returncode == 0 else f"失败（退出码 {result.returncode}）"
    return f"本轮摘要{status}，用时 {elapsed:.1f}s。\n{tail}"


def handle_request(message):
    """按 JSON-RPC 方法分发，返回结果 dict（或 None 表示不需要响应）。"""
    method = message.get("method")
    request_id = message.get("id")
    if method == "initialize":
        return {
            "jsonrpc": "2.0", "id": request_id,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "digest", "version": "1.0.0"},
            },
        }
    if method in ("notifications/initialized", "notifications/cancelled"):
        return None
    if method == "ping":
        return {"jsonrpc": "2.0", "id": request_id, "result": {}}
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": request_id, "result": {"tools": TOOLS}}
    if method == "tools/call":
        params = message.get("params") or {}
        name = params.get("name")
        arguments = params.get("arguments") or {}
        try:
            if name == "digest_schedule":
                text = tool_digest_schedule(arguments)
            elif name == "digest_run":
                text = tool_digest_run(arguments)
            else:
                raise ValueError(f"未知工具：{name}")
            return {
                "jsonrpc": "2.0", "id": request_id,
                "result": {"content": [{"type": "text", "text": text}]},
            }
        except Exception as error:  # noqa: BLE001 —— 工具错误要回给模型，不是让进程退出
            return {
                "jsonrpc": "2.0", "id": request_id,
                "result": {"content": [{"type": "text", "text": f"操作失败：{type(error).__name__}: {error}"}], "isError": True},
            }
    return {
        "jsonrpc": "2.0", "id": request_id,
        "error": {"code": -32601, "message": f"unsupported method: {method}"},
    }


def main():
    """stdio 主循环：逐行读 JSON-RPC，写回响应。"""
    log(f"started, python={PYTHON}, task={TASK_NAME}")
    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            log(f"非法 JSON：{line[:120]}")
            continue
        response = handle_request(message)
        if response is not None:
            sys.stdout.write(json.dumps(response, ensure_ascii=False) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
