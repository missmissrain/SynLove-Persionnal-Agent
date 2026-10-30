"""编码 agent 自修：把「重启和清状态都修不好」的链路故障交给 codex / opencode 改代码。

负责的功能：
- 判断这次故障值不值得动手（冷却、每日上限、同类故障不重复修）；
- 改之前把可改目录整体快照一份，用来回滚；
- 组织一段自修提示词（故障证据 + 白名单 + 禁止项 + 自验证要求）；
- 优先 codex，失败再换 opencode，都是非交互执行、stdin 关掉；
- 改完做静态验证（Python 语法、YAML 能否解析），不过就整目录还原；
- 全过程写 `runtime/coderepair.log`，状态落 `runtime/coderepair.json`。

它**不做**动态验证：真正的「preset 能不能挂上」由调用方跑 recoverLinks 的
reload-preset 验证，避免为了验证再建一个探针会话。

注册参数（构造）：
    root      项目根目录，默认本文件上一级
    targets   允许编码 agent 修改的目录列表，默认 tools/ + config/ + 微信 preset 目录
    timeout    opencode 的超时秒数，默认 480
    codex_timeout codex 的超时秒数，默认 1800（它高峰期很慢，给足 30 分钟）
    cooldown  两次自修之间的最小间隔秒数，默认 1800
    daily_max 每天最多自修几次，默认 4
    cli_chain 依次尝试的 CLI，默认 ('codex', 'opencode')
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT_DEFAULT = Path(__file__).resolve().parents[1]
PRESET_DEFAULT = Path.home() / ".dsh" / ".agent-presets" / "weixin-lite"


def _first_existing(candidates) -> Path | None:
    """按顺序找第一个存在的可执行文件；都没有返回 None。

    参数 candidates：候选路径（可含 None，来自未设置的环境变量）。
    """
    for item in candidates:
        if not item:
            continue
        path = Path(str(item))
        if path.is_file():
            return path
    return None


def _find_codex() -> Path | None:
    """找 codex：环境变量 → 桌面端安装目录 → PATH。

    PATH 里通常是 `codex.CMD` 垫片，直接 spawn 会报「找不到命令」，
    但 _run 会把 .cmd 交给 cmd.exe 执行，所以这里允许返回 .cmd。
    """
    local = os.environ.get("LOCALAPPDATA") or ""
    globbed = sorted(Path(local).glob("OpenAI/Codex/bin/codex.exe")) if local else []
    return _first_existing([os.environ.get("SYNLOVE_CODEX_BIN"), *globbed,
                            shutil.which("codex")])


def _find_opencode() -> Path | None:
    """找 opencode：环境变量 → 桌面端自带 CLI → node 全局安装 → PATH。"""
    local = os.environ.get("LOCALAPPDATA") or ""
    candidates = []
    if local:
        candidates += sorted(Path(local).glob("Programs/**/opencode-cli.exe"))
        candidates += sorted(Path(local).glob("Programs/**/opencode.exe"))
        candidates += sorted(Path(local).glob(
            "opencode-ai/node_modules/opencode-windows-x64/bin/opencode.exe"))
    candidates.append(shutil.which("opencode"))
    return _first_existing(candidates)


# 两个 CLI 的真实可执行文件（可用环境变量覆盖，不写死本机路径）
CODEX_BIN = _find_codex()
OPENCODE_BIN = _find_opencode()

# 快照时跳过的东西：缓存和运行时产物不属于「可修代码」，而且 runtime 有 500MB+
IGNORE_DIRS = {"__pycache__", ".git", "node_modules", "runtime", ".venv"}
MAX_SNAPSHOT_FILE = 5 * 1024 * 1024   # 单文件超过 5MB 不快照（都是二进制/模型，不该被改）

# 绝对不允许被碰的文件名片段（密钥、环境变量、证书）
FORBIDDEN_PARTS = ("secrets.json", ".env", ".pem", ".key", ".crt", ".pfx")


class codeRepair:
    """把「L0~L3 都救不回来」的故障交给编码 agent，改完验证，不通过就回滚。"""

    def __init__(self, root: Path = ROOT_DEFAULT, targets: list[Path] | None = None,
                 timeout: int = 480, cooldown: int = 1800, daily_max: int = 4,
                 cli_chain: tuple[str, ...] = ("codex", "opencode"),
                 codex_timeout: int = 1800) -> None:
        """参数见模块说明；targets 里不存在的目录会被自动跳过。

        timeout 管 opencode，codex_timeout 单独管 codex —— codex 高峰期很慢，
        实测一次小改要 1 分钟，网络差时更久，所以给它 30 分钟；opencode 一直很快。
        """
        self.root = Path(root)
        self.runtime = self.root / "runtime"
        self.state_file = self.runtime / "coderepair.json"
        self.log_file = self.runtime / "coderepair.log"
        self.timeout = timeout
        self.codex_timeout = codex_timeout
        self.cooldown = cooldown
        self.daily_max = daily_max
        self.cli_chain = cli_chain
        self.targets = [Path(p) for p in (targets or [self.root / "tools",
                                                      self.root / "config",
                                                      PRESET_DEFAULT])]
        self.state = self._load()

    # ---------------------------------------------------------------- 状态

    def _load(self) -> dict:
        """读状态文件；坏文件当没修过处理。"""
        default = {"last_at": 0.0, "day": "", "count_today": 0, "history": []}
        try:
            if self.state_file.is_file():
                data = json.loads(self.state_file.read_text(encoding="utf-8"))
                default.update({key: data[key] for key in default if key in data})
        except (OSError, json.JSONDecodeError):
            pass
        return default

    def _save(self) -> None:
        """落盘状态，只留最近 20 条历史。"""
        try:
            self.runtime.mkdir(parents=True, exist_ok=True)
            self.state["history"] = self.state["history"][-20:]
            self.state_file.write_text(json.dumps(self.state, ensure_ascii=False, indent=2),
                                       encoding="utf-8")
        except OSError:
            pass

    def log(self, message: str) -> None:
        """打印并追加日志。参数 message：一行内容。"""
        line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}"
        print(line, flush=True)
        try:
            with self.log_file.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")
        except OSError:
            pass

    def _allowed(self) -> bool:
        """冷却和每日上限：防止「修不好 → 反复修」烧 token。"""
        now = time.time()
        today = time.strftime("%Y-%m-%d")
        if self.state.get("day") != today:
            self.state["day"] = today
            self.state["count_today"] = 0
        if now - float(self.state.get("last_at") or 0) < self.cooldown:
            self.log(f"距上次自修不足 {self.cooldown} 秒，跳过")
            return False
        if int(self.state.get("count_today") or 0) >= self.daily_max:
            self.log(f"今天已自修 {self.state['count_today']} 次（上限 {self.daily_max}），跳过")
            return False
        return True

    # ---------------------------------------------------------------- 快照

    def _snapshot(self) -> Path | None:
        """把可改目录复制到 runtime/coderepair-snapshot，返回快照根目录。

        只复制「人可能改的源码/配置」，跳过 runtime、缓存、大文件和密钥。
        """
        stamp = time.strftime("%Y%m%d-%H%M%S")
        target_root = self.runtime / f"coderepair-snapshot-{stamp}"
        try:
            target_root.mkdir(parents=True, exist_ok=True)
            for index, source in enumerate(self.targets):
                if not source.is_dir():
                    continue
                destination = target_root / str(index)
                for path in self._walk(source):
                    relative = path.relative_to(source)
                    copy_to = destination / relative
                    copy_to.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(path, copy_to)
                (target_root / f"{index}.path").write_text(str(source), encoding="utf-8")
        except OSError as error:
            self.log(f"快照失败，放弃自修：{error}")
            return None
        return target_root

    def _walk(self, folder: Path):
        """遍历目录下的普通文件，跳过缓存、大文件和密钥。

        跳过规则按**相对路径**判断：快照本身放在 runtime 下，
        如果按绝对路径判断，"runtime" 会把所有文件都过滤掉。
        """
        for path in folder.rglob("*"):
            if not path.is_file():
                continue
            try:
                relative = path.relative_to(folder)
            except ValueError:
                relative = path
            if any(part in IGNORE_DIRS for part in relative.parts):
                continue
            if any(part in path.name.lower() for part in FORBIDDEN_PARTS):
                continue
            try:
                if path.stat().st_size > MAX_SNAPSHOT_FILE:
                    continue
            except OSError:
                continue
            yield path

    def _digest(self, path: Path) -> str:
        """文件内容 md5；读不了返回空串。"""
        try:
            hasher = hashlib.md5()
            with path.open("rb") as handle:
                for block in iter(lambda: handle.read(1 << 20), b""):
                    hasher.update(block)
            return hasher.hexdigest()
        except OSError:
            return ""

    def _diff(self, snapshot: Path) -> dict:
        """对比快照和现状，返回 {'changed': [...], 'created': [...], 'deleted': [...]}。"""
        result = {"changed": [], "created": [], "deleted": []}
        for item in sorted(snapshot.iterdir()):
            if item.name.endswith(".path"):
                index = item.name.split(".")[0]
                source = Path(item.read_text(encoding="utf-8"))
                baseline = snapshot / index
                if not baseline.is_dir():
                    continue
                before = {str(p.relative_to(baseline)): self._digest(p) for p in self._walk(baseline)}
                after = {str(p.relative_to(source)): self._digest(p) for p in self._walk(source)}
                for name, digest in after.items():
                    if name not in before:
                        result["created"].append(str(source / name))
                    elif before[name] != digest:
                        result["changed"].append(str(source / name))
                for name in before:
                    if name not in after:
                        result["deleted"].append(str(source / name))
        return result

    # ---------------------------------------------------------------- 提示词

    def _build_prompt(self, reason: str, detail: str = "") -> str:
        """组织自修提示词：给证据、给白名单、给禁止项、要求自验证并回 JSON。

        参数 reason：故障一句话描述；detail：补充证据（日志尾部等）。
        """
        whitelist = "\n".join(f"{index + 1}. {path}" for index, path in enumerate(self.targets)
                              if path.exists())
        lines = [
            "你是「欣爱」——一个跑在 Windows 上的微信/QQ 家庭管家——的自动修复程序。",
            "她现在坏了，自动化恢复（重启进程、清死锁、清状态库、清对话上下文）都试过了，修不好。",
            "这基本意味着是**代码或配置本身有问题**，需要你动手改。",
            "",
            "## 故障",
            reason.strip()[:1500],
        ]
        if detail.strip():
            lines += ["", "## 证据", detail.strip()[:4000]]
        lines += [
            "",
            "## 只允许修改这些位置",
            whitelist,
            "",
            "## 绝对禁止",
            "- 不准碰 runtime 目录（日志、状态、pid 文件）。",
            "- 不准碰 secrets.json、任何 .env、证书（.pem/.key/.crt）。",
            "- 不准删除文件，只能修改或新增；不准做 git 操作、不准装依赖、不准重启服务。",
            "- 不准把报错吞掉来「修复」：不许注释掉校验、不许 except: pass、不许降低检查强度。",
            "- 不准改测试或验证逻辑来让验证通过。",
            "",
            "## 要求",
            "1. 先读相关文件定位根因，再改；改动越小越好，改一处能修好就不要改三处。",
            "2. 改完必须自己验证：对每个改过的 .py 执行",
            f"   {sys.executable} -m py_compile <文件>；改了 .yml 就确认 YAML 能解析。",
            "3. 环境没人值守，不要问问题，10 分钟内做完。",
            "",
            "## 输出",
            "最后单独一行输出 JSON（不要包在代码块里）：",
            '{"root_cause":"一句话根因","changed":["改动的文件路径"],"verify":"你跑了什么验证、结果如何"}',
        ]
        return "\n".join(lines)

    # ---------------------------------------------------------------- 两个 CLI

    def _run(self, command: list[str], cwd: Path, timeout: int) -> tuple[bool, str]:
        """跑一个 CLI，返回 (是否正常退出, 输出)。stdin 必须关掉，否则会挂住等输入。

        参数 timeout：这个 CLI 的超时秒数（codex 给 30 分钟，opencode 给 8 分钟）。
        .cmd / .bat 垫片必须交给 cmd.exe，否则 CreateProcess 直接报「找不到文件」。
        """
        if command[0].lower().endswith((".cmd", ".bat")):
            command = ["cmd", "/c", *command]
        try:
            result = subprocess.run(command, cwd=str(cwd), timeout=timeout,
                                    stdin=subprocess.DEVNULL, capture_output=True,
                                    text=True, encoding="utf-8", errors="replace")
        except subprocess.TimeoutExpired:
            return False, f"超时（{timeout} 秒）"
        except FileNotFoundError:
            return False, f"找不到可执行文件：{command[0]}"
        except OSError as error:
            return False, f"启动失败：{error}"
        output = ((result.stdout or "") + (result.stderr or "")).strip()
        return result.returncode == 0, output

    def _codex(self, prompt: str) -> tuple[bool, str]:
        """codex exec：非交互执行，工作目录限死，沙箱放开（要改 preset 目录）。"""
        if CODEX_BIN is None:
            return False, "没找到 codex（可设 SYNLOVE_CODEX_BIN 指定路径）"
        return self._run([str(CODEX_BIN), "exec", "--skip-git-repo-check",
                          "-s", "danger-full-access", "-C", str(self.root), prompt],
                         self.root, self.codex_timeout)

    def _opencode(self, prompt: str) -> tuple[bool, str]:
        """opencode run：v1.19.5 用 --dir 指定工作目录。"""
        if OPENCODE_BIN is None:
            return False, "没找到 opencode（可设 SYNLOVE_OPENCODE_BIN 指定路径）"
        return self._run([str(OPENCODE_BIN), "run", "--dir", str(self.root), prompt],
                         self.root, self.timeout)

    # ---------------------------------------------------------------- 验证

    def _verify(self, diff: dict) -> tuple[bool, str]:
        """静态验证：Python 语法 + YAML 能否解析。返回 (是否通过, 说明)。

        动态验证（preset 到底能不能挂上）交给调用方的 recoverLinks，避免重复建会话。
        """
        files = list(diff["changed"]) + list(diff["created"])
        if not files:
            return False, "编码 agent 没有改动任何文件"
        if diff["deleted"]:
            return False, f"编码 agent 删了文件（不允许）：{diff['deleted'][:3]}"

        problems = []
        python = Path(sys.executable)
        for name in files:
            path = Path(name)
            suffix = path.suffix.lower()
            if suffix == ".py":
                result = subprocess.run([str(python), "-m", "py_compile", str(path)],
                                        capture_output=True, text=True, encoding="utf-8",
                                        errors="replace", timeout=120)
                if result.returncode != 0:
                    problems.append(f"{path.name} 语法错误：{(result.stderr or '')[:200]}")
            elif suffix in (".yml", ".yaml"):
                try:
                    import yaml  # noqa: PLC0415 —— 只有改了 yml 才需要
                except ImportError:
                    problems.append(f"{path.name} 改了但本机没有 PyYAML，无法验证")
                    continue
                try:
                    yaml.safe_load(path.read_text(encoding="utf-8"))
                except Exception as error:  # noqa: BLE001
                    problems.append(f"{path.name} YAML 解析失败：{error}")

        if problems:
            return False, "；".join(problems)[:500]
        return True, f"静态验证通过（{len(files)} 个文件）"

    def _restore(self, snapshot: Path, diff: dict) -> None:
        """回滚：改过的从快照覆盖，新增的删掉，返回是否干净。"""
        restored = 0
        try:
            for name in diff["created"]:
                Path(name).unlink(missing_ok=True)
                restored += 1
            for item in sorted(snapshot.iterdir()):
                if not item.name.endswith(".path"):
                    continue
                index = item.name.split(".")[0]
                source = Path(item.read_text(encoding="utf-8"))
                baseline = snapshot / index
                if not baseline.is_dir():
                    continue
                for path in baseline.rglob("*"):
                    if not path.is_file():
                        continue
                    target = source / path.relative_to(baseline)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(path, target)
                    restored += 1
        except OSError as error:
            self.log(f"回滚失败（需要人工介入）：{error}")
            return
        self.log(f"已回滚：{restored} 个文件恢复到自修前的状态")

    # ---------------------------------------------------------------- 主流程

    def attempt(self, reason: str, detail: str = "") -> dict:
        """跑一轮自修。返回 {'ok': bool, 'cli': str, 'changed': [...], 'verify': str}。

        参数 reason：故障描述；detail：补充证据（日志尾部）。
        ok=False 时文件已经回滚，调用方可以放心停机。
        """
        if not self._allowed():
            return {"ok": False, "cli": "", "changed": [], "verify": "未动手（冷却或超上限）"}

        self.state["last_at"] = time.time()
        self.state["count_today"] = int(self.state.get("count_today") or 0) + 1
        self._save()

        snapshot = self._snapshot()
        if snapshot is None:
            return {"ok": False, "cli": "", "changed": [], "verify": "快照失败，未动手"}

        prompt = self._build_prompt(reason, detail)
        self.log(f"开始自修（第 {self.state['count_today']} 次/今天）：{reason[:100]}")

        used = ""
        output = ""
        for name in self.cli_chain:
            if name == "codex":
                ok, output = self._codex(prompt)
            else:
                ok, output = self._opencode(prompt)
            used = name
            self.log(f"{name} 退出：{'成功' if ok else '失败'}；输出尾部 {output[-300:]}")
            diff = self._diff(snapshot)
            if ok and (diff["changed"] or diff["created"]):
                break
            if not ok:
                self.log(f"{name} 没成功，换下一个")
        else:
            self._restore(snapshot, self._diff(snapshot))
            self._record(False, used, [], "所有 CLI 都失败")
            return {"ok": False, "cli": used, "changed": [], "verify": "所有 CLI 都失败"}

        diff = self._diff(snapshot)
        passed, verify = self._verify(diff)
        changed = diff["changed"] + diff["created"]
        self.log(f"静态验证：{'通过' if passed else '不通过'}（{verify[:150]}）")
        if not passed:
            self._restore(snapshot, diff)
            self._record(False, used, changed, verify)
            return {"ok": False, "cli": used, "changed": changed, "verify": verify}

        # 快照留着，方便事后对比；只保留最近 3 份
        self._prune_snapshots()
        self._record(True, used, changed, verify)
        return {"ok": True, "cli": used, "changed": changed, "verify": verify}

    def _record(self, ok: bool, cli: str, changed: list[str], verify: str) -> None:
        """把这一轮结果写进状态文件的历史里。"""
        self.state["history"].append({
            "at": time.time(), "ok": ok, "cli": cli,
            "changed": changed, "verify": verify[:300],
        })
        self._save()

    def _prune_snapshots(self) -> None:
        """只留最近 3 份快照，别把 runtime 撑爆。"""
        try:
            snapshots = sorted(self.runtime.glob("coderepair-snapshot-*"))
            for old in snapshots[:-3]:
                shutil.rmtree(old, ignore_errors=True)
        except OSError:
            pass


def main(argv: list[str] | None = None) -> int:
    """命令行入口：手动跑一次自修（调试用）。参数 argv：默认取 sys.argv。"""
    import argparse

    parser = argparse.ArgumentParser(description="让 codex/opencode 修欣爱自己")
    parser.add_argument("--reason", default="手动触发", help="故障描述")
    parser.add_argument("--detail", default="", help="补充证据（日志尾部等）")
    parser.add_argument("--dry-run", action="store_true", help="只打印提示词，不调用 CLI")
    args = parser.parse_args(argv)

    repair = codeRepair()
    if args.dry_run:
        print(repair._build_prompt(args.reason, args.detail))
        return 3

    result = repair.attempt(args.reason, args.detail)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
