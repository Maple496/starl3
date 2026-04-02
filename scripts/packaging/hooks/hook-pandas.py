# PyInstaller hook for pandas
from PyInstaller.utils.hooks import collect_submodules, collect_data_files

# 收集 pandas 所有子模块
hiddenimports = collect_submodules('pandas')

# 收集 pandas 数据文件（如 .csv, .json 等）
datas = collect_data_files('pandas')

# 额外添加 pandas 依赖的库
hiddenimports += [
    'pandas._libs.tslibs.np_datetime',
    'pandas._libs.tslibs.nattype',
    'pandas._libs.tslibs.timezones',
    'pandas._libs.tslibs.timedeltas',
    'pandas._libs.tslibs.timestamps',
    'pandas._libs.tslibs.offsets',
    'pandas._libs.tslibs.period',
    'pandas._libs.tslibs.resolution',
    'pandas._libs.tslibs.fields',
    'pandas._libs.properties',
    'pandas._libs.hashtable',
    'pandas._libs.index',
    'pandas._libs.indexing',
    'pandas._libs.internals',
    'pandas._libs.interval',
    'pandas._libs.join',
    'pandas._libs.lib',
    'pandas._libs.missing',
    'pandas._libs.parsers',
    'pandas._libs.reduction',
    'pandas._libs.ops',
    'pandas._libs.ops_dispatch',
    'pandas._libs.sparse',
    'pandas._libs.writers',
    'pandas._libs.hashing',
    'pandas._libs.json',
    'pandas._libs.algos',
    'pandas._libs.groupby',
    'pandas._libs.window.indexers',
    'pandas._libs.skiplist',
    'pandas.io.excel._openpyxl',
    'pandas.io.excel._xlrd',
    'pandas.io.excel._xlsxwriter',
]
