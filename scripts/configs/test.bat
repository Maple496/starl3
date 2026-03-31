@echo off
chcp 65001 >nul
title StarL3 Pipeline 测试工具
cd /d "%~dp0"
echo ========================================
echo  StarL3 Pipeline 测试工具
echo ========================================
echo.
echo 正在启动测试...
echo.
"%LOCALAPPDATA%\Programs\Python\Python313\python.exe" main_test.py
echo.
echo 测试结束
echo.
pause
