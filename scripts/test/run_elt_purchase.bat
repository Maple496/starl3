@echo off
chcp 65001 >nul
echo ==========================================
echo StarL3 - 采购数据处理 (elt_purchase)
echo ==========================================
echo.
echo 配置文件: F:\Projects\starl3\scripts\test\elt_purchase.json
echo.

REM 切换到 scripts 目录（批处理文件的上级目录）
cd /d "%~dp0\.."

REM 使用指定的虚拟环境 Python 解释器运行主程序
"F:\Projects\starl3\venv\Scripts\python.exe" main_starl3.py "F:\Projects\starl3\scripts\test\elt_purchase.json"

echo.
echo ==========================================
echo 处理完成
echo ==========================================
pause
