# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.building.build_main import Analysis, PYZ, EXE
import os

block_cipher = None

a = Analysis(
    ['tray_app.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('web_static', 'web_static'),  # 包含Web静态文件
    ],
    hiddenimports=[
        'flask',
        'flask_cors',
        'pystray',
        'PIL',
        'PIL.Image',
        'PIL.ImageDraw',
        # ops 模块
        'ops.ai_ops', 'ops.attemper_ops', 'ops.config_ops', 'ops.crawler_ops',
        'ops.datavisual_ops', 'ops.elt_ops', 'ops.email_listener_ops', 
        'ops.email_ops', 'ops.file_ops', 'ops.include_ops', 'ops.input_ops',
        'ops.network_listener_ops', 'ops.scheduler_ops', 'ops.web_framework_ops',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'torch', 'scipy', 'sklearn', 'matplotlib', 'tensorboard',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='StarL3Tray',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,  # 无控制台窗口
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
