# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['0_run_all.py'],
    pathex=[],
    binaries=[],
    datas=[('1_copy_ems_to_local_config.json', '.'), ('1_copy_to_local_config.json', '.'), ('2_table_aggregator_config.json', '.'), ('2_table_aggregator_config-备份.json', '.'), ('3_config.json', '.')],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='0_run_all',
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
