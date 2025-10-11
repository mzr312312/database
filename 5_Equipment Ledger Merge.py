import pandas as pd
from sqlalchemy import create_engine

# 数据库连接配置（请根据实际情况修改）
db_config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'mazhuoran',
    'password': 'mazhuoran',
    'database': 'mzr_db'
}

# 创建数据库连接
engine = create_engine(
    f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}?charset=utf8")

# 原始表列表
source_tables = [
    'f设备表_1_扬州', 'f设备表_2_东台', 'f设备表_3_合肥',
    'f设备表_4_鄂尔多斯', 'f设备表_5_曲靖', 'f设备表_6_奉贤',
    'f设备表_7_包头', 'f设备表_8_义乌', 'f设备表_9_邢台',
    'f设备表_10_宁晋', 'f设备表_11_石家庄', 'f设备表_12_巴彦淖尔'
]

# 新表名称
new_table = '3_设备台账合并表'

try:
    # 存储所有DataFrame的列表
    dfs = []

    # 读取所有原始表
    for table in source_tables:
        print(f"正在读取表: {table}")
        df = pd.read_sql_table(table, engine)
        dfs.append(df)

    # 合并所有DataFrame
    print("开始合并数据...")
    merged_df = pd.concat(dfs, ignore_index=True)

    # 保存到新表
    print(f"保存到新表: {new_table}")
    merged_df.to_sql(
        name=new_table,
        con=engine,
        index=False,
        if_exists='replace',  # 如果表存在则替换
        chunksize=1000
    )

    print(f"成功合并 {len(source_tables)} 张表到 {new_table}")

except Exception as e:
    print(f"操作失败: {str(e)}")
finally:
    # 关闭数据库连接
    engine.dispose()