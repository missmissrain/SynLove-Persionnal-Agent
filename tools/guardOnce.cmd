@echo off
chcp 65001 >nul
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
cd /d "%~dp0.."
if not defined PYTHON set "PYTHON=python"
%PYTHON% tools\guardService.py --once >> runtime\guard-service.log 2>&1
