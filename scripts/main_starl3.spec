# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['main_starl3.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[
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
    noarchive=False,
    optimize=1,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='main_starl3',
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
