import pandas as pd
import pymysql
from sqlalchemy import create_engine
from datetime import datetime
import time
import os

# 数据库连接配置
db_config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'mazhuoran',
    'password': 'mazhuoran',
    'database': 'mzr_db'
}

# 输出路径配置
output_directory = r"D:\工作\PARA\1.PROJECTS\【置顶00】各基地计算逻辑和采集点\00汇总表"

# 表名列表
tables = [
    '1_汇总表_1_扬州',
    '1_汇总表_2_东台',
    '1_汇总表_3_合肥',
    '1_汇总表_4_鄂尔多斯',
    '1_汇总表_5_曲靖',
    '1_汇总表_6_奉贤',
    '1_汇总表_7_包头',
    '1_汇总表_8_义乌',
    '1_汇总表_9_邢台',
    '1_汇总表_10_宁晋',
    '1_汇总表_11_石家庄',
    '1_汇总表_12_巴彦淖尔'
]


# 创建数据库连接
def create_db_connection():
    try:
        # 使用SQLAlchemy创建连接
        connection_string = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return None


# 确保输出目录存在
def ensure_output_directory():
    try:
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
            print(f"创建输出目录: {output_directory}")
        return True
    except Exception as e:
        print(f"创建输出目录失败: {e}")
        return False


# 导出数据到Excel
def export_tables_to_excel():
    # 确保输出目录存在
    if not ensure_output_directory():
        return

    # 生成时间戳
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f'【合并】计算逻辑_{timestamp}.xlsx'
    filepath = os.path.join(output_directory, filename)

    # 创建数据库连接
    engine = create_db_connection()
    if engine is None:
        return

    # 创建一个ExcelWriter对象
    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
        all_data_frames = []  # 存储所有数据框用于汇总

        # 导出每个表的数据
        for table in tables:
            try:
                # 从数据库读取数据
                query = f"SELECT * FROM `{table}`"
                df = pd.read_sql(query, engine)

                # 简化表名作为sheet名（去掉前缀）
                sheet_name = table.split('_')[-1]

                # 将数据写入到单独的sheet
                df.to_excel(writer, sheet_name=sheet_name, index=False)

                # 添加来源标识列
                df['数据来源'] = sheet_name
                all_data_frames.append(df)

                print(f"表 {table} 已导出到Sheet: {sheet_name}")

            except Exception as e:
                print(f"导出表 {table} 时出错: {e}")

        # 创建汇总sheet（所有数据的拼接）
        if all_data_frames:
            try:
                summary_df = pd.concat(all_data_frames, ignore_index=True)
                summary_df.to_excel(writer, sheet_name='汇总', index=False)
                print("汇总Sheet已创建")
            except Exception as e:
                print(f"创建汇总Sheet时出错: {e}")
        else:
            print("没有数据可以汇总")

    print(f"文件已保存为: {filepath}")
    return filepath


# 验证文件是否成功创建
def verify_file_creation(filepath):
    if filepath and os.path.exists(filepath):
        file_size = os.path.getsize(filepath) / 1024  # KB
        print(f"文件验证成功: {filepath}")
        print(f"文件大小: {file_size:.2f} KB")
        return True
    else:
        print("文件创建失败")
        return False


# 主函数
if __name__ == "__main__":
    start_time = time.time()
    print("开始导出数据...")
    print(f"输出目录: {output_directory}")

    # 执行导出
    exported_filepath = export_tables_to_excel()

    # 验证文件
    if exported_filepath:
        verify_file_creation(exported_filepath)

    end_time = time.time()
    print(f"导出完成，耗时: {end_time - start_time:.2f} 秒")

    # 显示导出摘要
    print("\n=== 导出摘要 ===")
    print(f"输出文件: {exported_filepath}")
    print(f"包含工作表: 汇总 + {len(tables)}个基地表")