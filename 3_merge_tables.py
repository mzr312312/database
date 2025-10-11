import json
import os
import pandas as pd
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine, text


def process_base(i, base, config, timestamp):
    """处理单个基地：建合并表 + 内存 JOIN 导出 Excel"""
    try:
        db_config = config["db_config"]
        column_mapping = config["column_mapping"]
        column_order = config["column_order"]
        export_settings = config["export_settings"]

        # 创建数据库连接
        engine = create_engine(
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}?charset={db_config['charset']}",
            pool_pre_ping=True,
            pool_recycle=3600,
        )

        print(f"正在处理: {base}基地...")

        # 表名
        point_table = f"a采集点表_{i}_{base}"
        device_table = f"f设备表_{i}_{base}"
        source_table = f"g数据源表_{i}_{base}"
        merged_table = f"2_采集点+设备+数据源合并表_{i}_{base}"

        with engine.connect() as conn:
            # === 1. 创建数据库中的合并表（持久化）===
            print(f"  [1/6] 清理并创建合并表: {merged_table}")
            conn.execute(text(f"DROP TABLE IF EXISTS `{merged_table}`"))
            create_sql = text(f"""
                CREATE TABLE `{merged_table}` AS
                SELECT 
                    p.id AS point_id,
                    p.tag_name,
                    p.tag_code,
                    p.tag_desc,
                    p.ori_tag_name,
                    p.equipment_id,
                    p.general_attribute,
                    p.business_attribute,
                    p.classification,
                    p.verify_status,
                    d.id AS device_id,
                    d.equipment_name,
                    d.equipment_code,
                    d.base_name,
                    d.workshop,
                    d.workshop_section,
                    d.production_processes,
                    d.equipment_type,
                    d.equipment_sub_type,
                    d.equipment_attribute,
                    g.id AS source_id,
                    g.device_name AS source_device_name
                FROM `{point_table}` p
                LEFT JOIN `{device_table}` d ON p.equipment_id = d.id
                LEFT JOIN `{source_table}` g ON p.device_id = g.id
            """)
            conn.execute(create_sql)
            conn.commit()

            # === 2. 读取三张原始表 ===
            print(f"  [2/6] 读取采集点表: {point_table}")
            df_p = pd.read_sql(text(f"SELECT * FROM `{point_table}`"), conn)

            print(f"  [3/6] 读取设备表: {device_table}")
            df_d = pd.read_sql(text(f"""
                SELECT id, equipment_name, equipment_code, base_name, workshop, 
                       workshop_section, production_processes, equipment_type, 
                       equipment_sub_type, equipment_attribute 
                FROM `{device_table}`
            """), conn)

            print(f"  [4/6] 读取数据源表: {source_table}")
            df_g = pd.read_sql(text(f"SELECT id, device_name AS source_device_name FROM `{source_table}`"), conn)

        # === 修改后的内存 JOIN 部分 ===
        print(f"  [5/6] 内存中合并数据...")
        start_merge = time.time()

        # 指定需要保留的列，减少内存占用
        keep_cols = ['point_id', 'tag_name', 'tag_code', 'tag_desc', 'ori_tag_name',
                     'equipment_id', 'general_attribute', 'business_attribute',
                     'classification', 'verify_status', 'device_id']  # 确保包含device_id

        df_p_renamed = df_p.rename(columns={'id': 'point_id'})[keep_cols]

        # 第一次合并（采集点+设备）
        df_interim = df_p_renamed.merge(
            df_d.rename(columns={'id': 'device_id_d'})[
                ['device_id_d', 'equipment_name', 'equipment_code', 'base_name',
                 'workshop', 'workshop_section', 'production_processes',
                 'equipment_type', 'equipment_sub_type', 'equipment_attribute']
            ],
            left_on='equipment_id',
            right_on='device_id_d',
            how='left'
        )

        # 第二次合并（+数据源）- 修复连接键
        df = df_interim.merge(
            df_g.rename(columns={'id': 'source_id_g'})[
                ['source_id_g', 'source_device_name']
            ],
            left_on='device_id',  # 关键修复：使用采集点表的device_id字段
            right_on='source_id_g',
            how='left'
        )

        # 删除冗余列
        df.drop(columns=['device_id_d', 'source_id_g'], inplace=True, errors='ignore')

        print(f"  合并完成, 耗时: {time.time() - start_merge:.2f}秒")
        print(f"  合并后数据大小: {len(df)}行 x {len(df.columns)}列")

        # === 4. 重命名列 ===
        df.rename(columns=column_mapping, inplace=True)

        # === 5. 调整列顺序 ===
        valid_columns = [col for col in column_order if col in df.columns]
        df = df[valid_columns]

        # === 6. 关键修复：去重列名（防止 df[col] 返回 DataFrame）===
        df = df.loc[:, ~df.columns.duplicated(keep='first')].copy()
        df.reset_index(drop=True, inplace=True)

        # === 7. 导出 Excel ===
        export_path = export_settings.get(base, "")
        if not export_path:
            print(f"警告: {base}的导出路径未配置，跳过导出")
            return None

        os.makedirs(export_path, exist_ok=True)
        filename = f"{merged_table}_{timestamp}.xlsx"
        filepath = os.path.join(export_path, filename)

        print(f"  [6/6] 导出Excel: {filename}")
        with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
            sheet_name = "采集点+设备+数据源"
            df.to_excel(writer, sheet_name=sheet_name, index=False)

            workbook = writer.book
            worksheet = writer.sheets[sheet_name]

            # === 安全设置列宽（终极修复版）===
            for idx, col in enumerate(df.columns):
                # 安全获取列数据（使用 .iloc 避免列名问题）
                series = df.iloc[:, idx]
                # 转为字符串并填充空值
                str_series = series.astype(str).fillna('')
                # 计算每行长度
                lengths = str_series.str.len()
                max_data_len = lengths.max()

                # 确保 max_data_len 是标量（处理 Series 或 NaN）
                if isinstance(max_data_len, pd.Series):
                    max_data_len = max_data_len.max()  # 再取一次最大值
                if pd.isna(max_data_len):
                    max_data_len = 0

                max_header_len = len(str(col))
                max_len = max(max_data_len, max_header_len) + 2  # 增加 padding
                worksheet.set_column(idx, idx, min(max_len, 50))  # 最大50字符宽

            # === 添加表格样式（黄色表头）===
            (max_row, max_col) = df.shape
            column_settings = [{"header": column} for column in df.columns]
            worksheet.add_table(0, 0, max_row, max_col - 1, {
                'columns': column_settings,
                'style': 'Table Style Light 11',
                'autofilter': True
            })

        print(f"已导出: {filename}")
        return {
            'base': base,
            'index': i,
            'filepath': filepath,
            'filename': filename
        }

    except Exception as e:
        print(f"处理{base}基地时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

    finally:
        if 'engine' in locals():
            engine.dispose()


def merge_generated_files(exported_files, merged_export_path, timestamp):
    """合并所有生成的Excel文件到一个文件中（分页）"""
    print("\n开始合并所有基地文件...")
    start_time = time.time()

    os.makedirs(merged_export_path, exist_ok=True)
    merged_filename = f"【合并】采集点+设备+数据源_{timestamp}.xlsx"
    merged_filepath = os.path.join(merged_export_path, merged_filename)

    try:
        with pd.ExcelWriter(merged_filepath, engine='xlsxwriter') as writer:
            all_dfs = []
            for file_info in exported_files:
                base = file_info['base']
                print(f"正在读取合并: {base}...")
                df = pd.read_excel(file_info['filepath'])
                df.insert(0, '基地', base)
                all_dfs.append(df)

                # 写入分页
                sheet_name = f"{file_info['index']}_{base}"[:31]  # Excel 限制31字符
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                worksheet = writer.sheets[sheet_name]
                (max_row, max_col) = df.shape
                column_settings = [{"header": column} for column in df.columns]
                worksheet.add_table(0, 0, max_row, max_col - 1, {
                    'columns': column_settings,
                    'style': 'Table Style Light 11',
                    'autofilter': True
                })

            # 写汇总页
            if all_dfs:
                summary_df = pd.concat(all_dfs, ignore_index=True)
                summary_df.to_excel(writer, sheet_name="汇总", index=False)
                worksheet = writer.sheets["汇总"]
                (max_row, max_col) = summary_df.shape
                column_settings = [{"header": column} for column in summary_df.columns]
                worksheet.add_table(0, 0, max_row, max_col - 1, {
                    'columns': column_settings,
                    'style': 'Table Style Medium 16',
                    'autofilter': True
                })

        print(f"合并完成! 耗时: {time.time() - start_time:.2f}秒")
        return merged_filepath

    except Exception as e:
        print(f"合并文件时出错: {str(e)}")
        raise


def merge_and_export():
    """主函数：处理所有基地"""
    try:
        with open('3_config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)

        base_list = list(config["export_settings"].keys())
        merged_export_path = config["merged_export_path"]
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        exported_files = []

        print(f"开始处理 {len(base_list)} 个基地...")
        start_time = time.time()

        # 并行处理
        max_workers = min(8, len(base_list))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            tasks = [(i + 1, base, config, timestamp) for i, base in enumerate(base_list)]
            futures = [executor.submit(process_base, *task) for task in tasks]
            for future in futures:
                result = future.result()
                if result:
                    exported_files.append(result)

        print(f"\n所有基地处理完成! 总耗时: {time.time() - start_time:.2f}秒")

        if exported_files:
            merged_filepath = merge_generated_files(exported_files, merged_export_path, timestamp)
            print(f"\n合并文件已生成: {merged_filepath}")
        else:
            print("没有文件可合并")

    except FileNotFoundError:
        print("错误: 未找到配置文件 3_config.json")
    except KeyError as e:
        print(f"配置错误: 缺少必要的配置项 - {str(e)}")
    except Exception as e:
        print(f"处理出错: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    merge_and_export()
    print("程序已正常退出")