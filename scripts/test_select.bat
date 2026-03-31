@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ==========================================
echo StarL3 Pipeline - 资源选择测试工具
echo ==========================================
echo.
python main_test_select.py
pause
