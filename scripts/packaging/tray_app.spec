# -*- mode: python ; coding: utf-8 -*-
"""
StarL3 系统托盘程序 - PyInstaller 构建配置
打包虚拟环境所有依赖
"""
from PyInstaller.building.build_main import Analysis, PYZ, EXE
from PyInstaller.utils.hooks import collect_all, collect_submodules, collect_data_files
import os
import sys

# 虚拟环境路径
VENV_DIR = r'F:\projects\starl3\venv'
VENV_SITE_PACKAGES = os.path.join(VENV_DIR, 'Lib', 'site-packages')

# 收集必要的数据文件（不包含测试模块）
# flask 静态资源
flask_datas = collect_data_files('flask')
# pystray 图标资源  
pystray_datas = collect_data_files('pystray')

block_cipher = None

# ==========================================
# Analysis - 分析阶段
# ==========================================
a = Analysis(
    ['../tray_app.py'],  # 只保留主入口，其他模块通过导入使用
    
    pathex=[
        'f:/projects/starl3/scripts',
        VENV_SITE_PACKAGES,
    ],
    
    binaries=[],
    
    datas=[
        ('../frontend/static', 'frontend/static'),
        ('../configs', 'configs'),
        *flask_datas,
        *pystray_datas,
    ],
    
    hiddenimports=[
        # 项目自定义模块
        'app.resource_path',
        'core', 'core.constants', 'core.context', 'core.logger',
        'core.pipeline_engine', 'core.registry', 'core.safe_eval', 'core.utils',
        'ops', 'ops.config_ops', 'ops.datavisual_ops', 'ops.elt_ops', 'ops.file_ops',
        'ops.viz', 'ops.viz.csv_to_table', 'ops.viz.csv_to_chart',
        'app', 'app.cli', 'app.tray', 'app.tasks', 'app.web', 'app.config_store',
        'app.server', 'app.server.pipeline_editor', 'app.server.dynamic_config',
        'app.server.pipeline_editor.json_utils',
        
        # Python 标准库
        'json', 'csv', 'zipfile', 'pathlib', 'tkinter', 'tkinter.filedialog',
        'tkinter.messagebox', 'tkinter.ttk', 'http.server', 'urllib.parse',
        'uuid', 'enum', 'dataclasses', 'typing',
        
        # 确保 pandas Excel 引擎被包含
        'pandas.io.excel._openpyxl',
        'pandas.io.excel._xlrd',
        'pandas.io.excel._xlsxwriter',
        'openpyxl.cell', 'openpyxl.utils', 'openpyxl.styles',
        'openpyxl.workbook', 'openpyxl.worksheet', 'openpyxl.reader',
        'openpyxl.writer', 'openpyxl.packaging', 'openpyxl.descriptors',
    ],
    
    hookspath=['hooks'],
    hooksconfig={},
    runtime_hooks=[],
    
    # 不排除任何包，全部打包
    excludes=[],
    
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

# ==========================================
# PYZ - 压缩阶段
# ==========================================
pyz = PYZ(
    a.pure,
    a.zipped_data,
    cipher=block_cipher
)

# ==========================================
# EXE - 打包阶段
# ==========================================
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    
    name=r'F:\projects\starl3\StarL3Tray',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
