import json
import mysql.connector
from mysql.connector import Error
import os
import sys

# 基地名称映射
BASE_MAPPING = {
    1: "扬州",
    2: "东台",
    3: "合肥",
    4: "鄂尔多斯",
    5: "曲靖",
    6: "奉贤",
    7: "包头",
    8: "义乌",
    9: "邢台",
    10: "宁晋",
    11: "石家庄",
    12: "巴彦淖尔"
}


def load_config(config_path):
    """加载并验证JSON配置文件"""
    try:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"配置文件不存在: {config_path}")

        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        required_keys = ['table_name_template', 'column_mapping', 'column_order']
        missing_keys = [key for key in required_keys if key not in config]

        if missing_keys:
            raise ValueError(f"配置文件中缺少必需的键: {', '.join(missing_keys)}")

        mapped_columns = set(config['column_mapping'].values())
        ordered_columns = set(config['column_order'])

        if mapped_columns != ordered_columns:
            missing_in_order = mapped_columns - ordered_columns
            missing_in_mapping = ordered_columns - mapped_columns

            errors = []
            if missing_in_order:
                errors.append(f"以下列在映射中但不在顺序中: {', '.join(missing_in_order)}")
            if missing_in_mapping:
                errors.append(f"以下列在顺序中但不在映射中: {', '.join(missing_in_mapping)}")

            raise ValueError("\n".join(errors))

        return config
    except Exception as e:
        print(f"加载配置错误: {e}")
        print(f"当前工作目录: {os.getcwd()}")
        sys.exit(1)


def create_connection():
    """创建数据库连接"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='mazhuoran',
            password='mazhuoran',
            database='mzr_db'
        )
        print("数据库连接成功")
        return connection
    except Error as e:
        print(f"数据库连接错误: {e}")
        sys.exit(1)


def create_summary_table(connection, table_name, column_order):
    """创建汇总表"""
    cursor = connection.cursor()

    column_defs = []
    for col_name in column_order:
        column_defs.append(f"`{col_name}` VARCHAR(255)")

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {",\n        ".join(column_defs)}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    try:
        cursor.execute(create_table_sql)
        print(f"表 {table_name} 创建成功或已存在")
    except Error as e:
        print(f"创建表错误: {e}")
        sys.exit(1)
    finally:
        cursor.close()


def aggregate_data(connection, config, base_id, base_name):
    """聚合数据到新表"""
    cursor = connection.cursor()

    # 动态生成表名
    table_name = config['table_name_template'].format(base_id=base_id, base_name=base_name)
    a_table = f"a采集点表_{base_id}_{base_name}"
    b_table = f"b计算逻辑表_名称_{base_id}_{base_name}"
    c_table = f"c计算逻辑表_采集点_{base_id}_{base_name}"
    d_table = f"d接口表_{base_id}_{base_name}"
    e_table = f"e服务表_{base_id}_{base_name}"
    f_table = f"f设备表_{base_id}_{base_name}"
    g_table = f"g数据源表_{base_id}_{base_name}"
    z_table = "z【字典】业务属性"  # 新增z表

    # 构建反向映射
    reverse_mapping = {v: k for k, v in config['column_mapping'].items()}

    # 构建SELECT子句
    select_columns = []
    for new_col in config['column_order']:
        orig_col = reverse_mapping[new_col]

        # 处理特殊列
        if orig_col == "business_attribute_b":
            select_columns.append(f"b.business_attribute AS `{new_col}`")
        elif orig_col == "business_attribute_a":
            select_columns.append(f"a.business_attribute AS `{new_col}`")
        # 新增业务属性ID处理
        elif orig_col == "business_attribute_id":
            select_columns.append(f"z.`业务属性ID` AS `{new_col}`")
        # 新增数据源名称处理
        elif orig_col == "device_name":
            select_columns.append(f"g.device_name AS `{new_col}`")
        else:
            # 明确指定每个列的来源表
            if orig_col in ["tag_name", "ori_tag_name", "tag_code", "id",
                            "equipment_id", "general_attribute", "classification",
                            "verify_status", "device_id"]:
                select_columns.append(f"a.{orig_col} AS `{new_col}`")
            elif orig_col in ["aggregation_relation_id", "tag_id"]:
                select_columns.append(f"c.{orig_col} AS `{new_col}`")
            elif orig_col in ["aggregation_name", "aggregation_code", "business_code",
                              "application_agency", "aggregation_dimension", "datasource_num",
                              "equipment_num", "tag_num", "enable"]:
                select_columns.append(f"b.{orig_col} AS `{new_col}`")
            elif orig_col in ["equipment_name", "equipment_code", "base_name", "workshop",
                              "workshop_section", "production_processes", "equipment_type",
                              "equipment_sub_type", "equipment_attribute"]:
                select_columns.append(f"f.{orig_col} AS `{new_col}`")
            elif orig_col in ["service_name", "service_code", "is_enable",
                              "statistics_type", "statistics_mode"]:
                select_columns.append(f"e.{orig_col} AS `{new_col}`")
            elif orig_col in ["interface_name", "interface_code", "interface_type",
                              "interface_url", "service_type", "interface_topic"]:
                select_columns.append(f"d.{orig_col} AS `{new_col}`")
            else:
                select_columns.append(f"{orig_col} AS `{new_col}`")

    select_clause = ",\n        ".join(select_columns)

    # 构建SQL查询 - 使用c表作为主表
    sql = f"""
    INSERT INTO `{table_name}`
    SELECT 
        {select_clause}
    FROM `{c_table}` c
    LEFT JOIN `{a_table}` a ON c.tag_id = a.id
    LEFT JOIN `{g_table}` g ON a.device_id = g.id
    LEFT JOIN `{b_table}` b ON c.aggregation_relation_id = b.id
    LEFT JOIN `{z_table}` z ON b.business_attribute = z.`业务属性名称`
    LEFT JOIN `{f_table}` f ON a.equipment_id = f.id
    LEFT JOIN `{e_table}` e ON b.id = e.agg_relation_id
    LEFT JOIN `{d_table}` d ON e.interface_id = d.id;
    """

    try:
        print(f"正在为基地 {base_name} 执行数据聚合...")

        # 获取各表的行数
        cursor.execute(f"SELECT COUNT(*) FROM `{c_table}`")
        c_count = cursor.fetchone()[0]
        print(f"c表 '{c_table}' 行数: {c_count}")

        cursor.execute(f"SELECT COUNT(*) FROM `{a_table}`")
        a_count = cursor.fetchone()[0]
        print(f"a表 '{a_table}' 行数: {a_count}")

        cursor.execute(f"SELECT COUNT(*) FROM `{b_table}`")
        b_count = cursor.fetchone()[0]
        print(f"b表 '{b_table}' 行数: {b_count}")

        cursor.execute(f"SELECT COUNT(*) FROM `{z_table}`")
        z_count = cursor.fetchone()[0]
        print(f"z表 '{z_table}' 行数: {z_count}")

        # 检查z表中是否有数据
        if z_count > 0:
            cursor.execute(f"SELECT DISTINCT `业务属性名称` FROM `{z_table}` LIMIT 5")
            z_samples = cursor.fetchall()
            print(f"z表业务属性名称示例: {[sample[0] for sample in z_samples]}")

        # 检查b表中的business_attribute值
        if b_count > 0:
            cursor.execute(f"SELECT DISTINCT business_attribute FROM `{b_table}` LIMIT 5")
            b_samples = cursor.fetchall()
            print(f"b表business_attribute示例: {[sample[0] for sample in b_samples]}")

        # 清空旧数据
        cursor.execute(f"TRUNCATE TABLE `{table_name}`")

        # 执行数据聚合
        print("执行SQL语句...")
        cursor.execute(sql)
        connection.commit()

        # 获取汇总表的行数
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        summary_count = cursor.fetchone()[0]

        print(f"数据已成功聚合到 {table_name}")
        print(f"原始表c的行数: {c_count}")
        print(f"汇总表的行数: {summary_count}")

        if c_count != summary_count:
            print(f"警告: 行数不一致! 可能丢失了 {c_count - summary_count} 行数据")
            print("建议检查连接条件和数据完整性")
        else:
            print("行数一致，所有数据已保留")

        # 检查汇总表的前几行数据
        if summary_count > 0:
            cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 5")
            sample_rows = cursor.fetchall()
            print(f"汇总表数据示例 (前5行):")
            for i, row in enumerate(sample_rows):
                print(f"行 {i+1}: {row}")

    except Error as e:
        print(f"数据聚合错误: {e}")
        print(f"执行的SQL: {sql}")
        # 获取更详细的错误信息
        import traceback
        traceback.print_exc()
        connection.rollback()
    finally:
        cursor.close()


def main():
    print("开始数据聚合流程...")

    # 加载配置文件
    config_path = "table_aggregator_config.json"
    print(f"加载配置文件: {config_path}")
    config = load_config(config_path)

    # 连接数据库
    connection = create_connection()

    try:
        # 为每个基地创建汇总表并聚合数据
        for base_id, base_name in BASE_MAPPING.items():
            # 创建汇总表
            table_name = config['table_name_template'].format(base_id=base_id, base_name=base_name)
            create_summary_table(connection, table_name, config['column_order'])

            # 执行数据聚合
            aggregate_data(connection, config, base_id, base_name)
            print("-" * 50)  # 分隔线

        print("\n所有基地数据聚合完成!")
    except Exception as e:
        print(f"处理过程中发生错误: {e}")
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("数据库连接已关闭")


if __name__ == "__main__":
    main()