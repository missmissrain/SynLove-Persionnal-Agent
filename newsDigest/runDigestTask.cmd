@echo off
rem 计划任务专用入口：与 runDigest.cmd 相同，但**不 pause**（前台会卡住计划任务）。
rem 日志由 runDigest.py 自己写 runtime/news-digest.log，这里只负责带 PYTHONUTF8=1 启动。
setlocal
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
if not defined PYTHON set "PYTHON=python"
if not exist "%PYTHON%" set "PYTHON=python"
"%PYTHON%" "%~dp0runDigest.py" %*
endlocal
