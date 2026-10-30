"""音频抽取与语音转写步骤。

职责：
- audioExtractor：用 ffmpeg 把视频转成 16kHz 单声道 wav；
- speechRecognizer：调用本机 whisper 服务（不在就唤起），拿全文与分段，然后让服务退出释放显存。

注册参数（config 的 whisper 段）：
    port / model / language / idleSeconds
"""

import io
import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))
from services.pythonRuntime import service_python
SERVICE = ROOT / "services" / "whisperService.py"
LOG = ROOT / "runtime" / "whisper-server.log"
FFMPEG = os.environ.get("FFMPEG") or "ffmpeg"


class audioExtractor:
    """从视频里抽一条适合 ASR 的 wav。"""

    def __init__(self, out_dir):
        """out_dir：中间文件目录。"""
        self.out_dir = Path(out_dir)

    def extract(self, video_path):
        """返回 16k 单声道 wav 的路径（失败抛异常）。"""
        self.out_dir.mkdir(parents=True, exist_ok=True)
        wav_path = self.out_dir / (Path(video_path).stem + ".asr.wav")
        started = time.time()
        result = subprocess.run(
            [FFMPEG, "-y", "-hide_banner", "-loglevel", "error", "-i", str(video_path),
             "-vn", "-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le", str(wav_path)],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        if result.returncode != 0 or not wav_path.is_file():
            raise RuntimeError(f"抽音频失败：{(result.stderr or '')[:200]}")
        print(f"[音频] 已抽取 {wav_path.name}（{time.time() - started:.1f}s）", flush=True)
        return str(wav_path)


class speechRecognizer:
    """调用 whisper 服务做转写。"""

    def __init__(self, config):
        """config：whisper 段。"""
        self.vad_filter = bool(config.get("vadFilter", True))
        self.port = int(config.get("port", 8081))
        self.model = str(config.get("model", "small"))
        self.language = config.get("language", "zh")
        self.idle = int(config.get("idleSeconds", 120))

    def _port_open(self):
        """服务是否已监听。"""
        try:
            with socket.create_connection(("127.0.0.1", self.port), timeout=1.0):
                return True
        except OSError:
            return False

    def _ensure(self):
        """服务不在就后台拉起。"""
        if self._port_open():
            return
        LOG.parent.mkdir(parents=True, exist_ok=True)
        flags = 0x08000000 | 0x00000008
        with LOG.open("a", encoding="utf-8") as handle:
            subprocess.Popen(
                [service_python(), str(SERVICE), "--port", str(self.port), "--model", self.model,
                 "--idle-seconds", str(self.idle)],
                cwd=str(ROOT), stdout=handle, stderr=handle, creationflags=flags,
            )
        for _ in range(40):
            time.sleep(0.5)
            if self._port_open():
                return
        raise RuntimeError(f"whisper 服务启动超时，见日志 {LOG}")

    def unload(self):
        """让服务退出，把显存让给 VLM。"""
        if not self._port_open():
            return
        try:
            request = urllib.request.Request(f"http://127.0.0.1:{self.port}/unload", data=b"{}",
                                             headers={"Content-Type": "application/json"}, method="POST")
            urllib.request.urlopen(request, timeout=10).read()
        except Exception:  # noqa: BLE001
            pass

    def transcribe(self, wav_path):
        """返回 {"text": str, "segments": [...], "language": str}。"""
        self._ensure()
        payload = json.dumps({"path": wav_path, "language": self.language,
                              "vadFilter": bool(self.vad_filter)}).encode("utf-8")
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/transcribe", data=payload,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        started = time.time()
        with urllib.request.urlopen(request, timeout=3600) as response:
            data = json.loads(response.read().decode("utf-8"))
        # 音乐/演唱会这类视频 VAD 会把歌声滤掉：没转出字就关掉 VAD 再试一次
        if not str(data.get("text", "")).strip() and self.vad_filter:
            print("[ASR] 首次为空，关闭 VAD 重试…", flush=True)
            payload = json.dumps({"path": wav_path, "language": self.language, "vadFilter": False}).encode("utf-8")
            request = urllib.request.Request(
                f"http://127.0.0.1:{self.port}/transcribe", data=payload,
                headers={"Content-Type": "application/json"}, method="POST",
            )
            with urllib.request.urlopen(request, timeout=3600) as response:
                data = json.loads(response.read().decode("utf-8"))
        print(f"[ASR] {len(data.get('text', ''))} 字，{len(data.get('segments', []))} 段，"
              f"用时 {time.time() - started:.1f}s", flush=True)
        return data
