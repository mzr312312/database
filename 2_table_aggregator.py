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

# 表别名模板（用于动态生成表名）
TABLE_ALIASES = {
    'a': 'a采集点表_{base_id}_{base_name}',
    'b': 'b计算逻辑表_名称_{base_id}_{base_name}',
    'c': 'c计算逻辑表_采集点_{base_id}_{base_name}',
    'd': 'd接口表_{base_id}_{base_name}',
    'e': 'e服务表_{base_id}_{base_name}',
    'f': 'f设备表_{base_id}_{base_name}',
    'g': 'g数据源表_{base_id}_{base_name}',
    'y': 'y【字典】车间编码',
    'z': 'z【字典】业务属性'
}

# 特殊逻辑字段映射：逻辑字段名 -> (SQL表别名, 真实数据库列名)
# 注意：y2 是 y 表在 JOIN 中的别名
SPECIAL_COLUMNS = {
    "business_attribute_b": ("b", "business_attribute"),
    "business_attribute_a": ("a", "business_attribute"),
    "business_attribute_id": ("z", "业务属性ID"),
    "车间代码": ("y2", "车间代码"),
    "description": ("b", "description"),
    "business_info1": ("c", "business_info1"),
    "business_info2": ("c", "business_info2"),
    "business_info3": ("c", "business_info3"),
}


def load_config(config_path):
    """加载并验证JSON配置文件"""
    try:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"配置文件不存在: {config_path}")

        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        required_keys = ['table_name_template', 'column_mapping', 'column_order', 'column_sources']
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

        # 验证 column_sources 覆盖所有非特殊原始字段
        orig_columns = set(config['column_mapping'].keys())
        special_cols = set(SPECIAL_COLUMNS.keys())
        normal_cols = orig_columns - special_cols
        source_columns = set(config['column_sources'].keys())

        if not normal_cols.issubset(source_columns):
            missing_sources = normal_cols - source_columns
            raise ValueError(f"column_sources 缺少以下原始字段的来源定义: {', '.join(missing_sources)}")

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
    try:
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")
        print(f"已删除已存在的表: {table_name}")
    except Error as e:
        print(f"删除表错误: {e}")

    column_defs = [f"`{col}` VARCHAR(255)" for col in column_order]
    create_table_sql = f"""
    CREATE TABLE `{table_name}` (
        {",\n        ".join(column_defs)}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        cursor.execute(create_table_sql)
        print(f"表 {table_name} 创建成功")
    except Error as e:
        print(f"创建表错误: {e}")
        sys.exit(1)
    finally:
        cursor.close()


def ensure_result_consumed(cursor):
    """确保所有查询结果都被完全读取"""
    try:
        for _ in cursor:
            pass
    except:
        pass


def aggregate_data(connection, config, base_id, base_name):
    """聚合数据到新表"""
    cursor = connection.cursor()

    # 动态生成表名
    table_name = config['table_name_template'].format(base_id=base_id, base_name=base_name)
    actual_tables = {}
    for alias, template in TABLE_ALIASES.items():
        if alias in ['y', 'z']:
            actual_tables[alias] = template
        else:
            actual_tables[alias] = template.format(base_id=base_id, base_name=base_name)

    reverse_mapping = {v: k for k, v in config['column_mapping'].items()}
    column_sources = config['column_sources']

    # 构建 SELECT 子句
    select_columns = []
    for new_col in config['column_order']:
        orig_col = reverse_mapping[new_col]
        if orig_col in SPECIAL_COLUMNS:
            table_alias, real_col = SPECIAL_COLUMNS[orig_col]
            select_columns.append(f"{table_alias}.`{real_col}` AS `{new_col}`")
        else:
            source_alias = column_sources[orig_col]
            select_columns.append(f"{source_alias}.`{orig_col}` AS `{new_col}`")

    select_clause = ",\n        ".join(select_columns)

    # 构建完整 SQL（以 c 表为主表）
    sql = f"""
    INSERT INTO `{table_name}`
    SELECT 
        {select_clause}
    FROM `{actual_tables['c']}` c
    LEFT JOIN `{actual_tables['a']}` a ON c.tag_id = a.id
    LEFT JOIN `{actual_tables['g']}` g ON a.device_id = g.id
    LEFT JOIN `{actual_tables['b']}` b ON c.aggregation_relation_id = b.id
    LEFT JOIN `{actual_tables['z']}` z ON b.business_attribute = z.`业务属性名称`
    LEFT JOIN `{actual_tables['f']}` f ON a.equipment_id = f.id
    LEFT JOIN `{actual_tables['e']}` e ON b.id = e.agg_relation_id
    LEFT JOIN `{actual_tables['d']}` d ON e.interface_id = d.id
    LEFT JOIN `{actual_tables['y']}` y2 ON y2.`基地` = f.`base_name` AND y2.`车间名称` = f.`workshop`;
    """

    try:
        print(f"正在为基地 {base_name} 执行数据聚合...")

        # 检查 y 表结构
        cursor.execute(f"SHOW COLUMNS FROM `{actual_tables['y']}`")
        y_columns = [col[0] for col in cursor.fetchall()]
        ensure_result_consumed(cursor)
        if "基地" not in y_columns or "车间名称" not in y_columns:
            print("错误: y表缺少必需的列 '基地' 或 '车间名称'")
            sys.exit(1)

        # 获取各表行数（简化输出）
        for alias in ['c', 'a', 'b', 'z', 'y']:
            cursor.execute(f"SELECT COUNT(*) FROM `{actual_tables[alias]}`")
            count = cursor.fetchone()[0]
            print(f"{alias}表 '{actual_tables[alias]}' 行数: {count}")
            ensure_result_consumed(cursor)

        # 测试 SELECT 列数
        test_sql = f"""
        SELECT {select_clause}
        FROM `{actual_tables['c']}` c
        LEFT JOIN `{actual_tables['a']}` a ON c.tag_id = a.id
        LEFT JOIN `{actual_tables['g']}` g ON a.device_id = g.id
        LEFT JOIN `{actual_tables['b']}` b ON c.aggregation_relation_id = b.id
        LEFT JOIN `{actual_tables['z']}` z ON b.business_attribute = z.`业务属性名称`
        LEFT JOIN `{actual_tables['f']}` f ON a.equipment_id = f.id
        LEFT JOIN `{actual_tables['e']}` e ON b.id = e.agg_relation_id
        LEFT JOIN `{actual_tables['d']}` d ON e.interface_id = d.id
        LEFT JOIN `{actual_tables['y']}` y2 ON y2.`基地` = f.`base_name` AND y2.`车间名称` = f.`workshop`
        LIMIT 1;
        """
        cursor.execute(test_sql)
        col_count = len(cursor.description)
        expected = len(config['column_order'])
        if col_count != expected:
            print(f"列数不匹配！SELECT返回{col_count}列，期望{expected}列")
            sys.exit(1)
        ensure_result_consumed(cursor)

        # 执行聚合
        cursor.execute(f"TRUNCATE TABLE `{table_name}`")
        cursor.execute(sql)
        connection.commit()

        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        summary_count = cursor.fetchone()[0]
        ensure_result_consumed(cursor)

        print(f"数据已成功聚合到 {table_name}")
        print(f"汇总表行数: {summary_count}")

        # 示例数据
        if summary_count > 0:
            cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 3")
            rows = cursor.fetchall()
            print("示例数据:")
            for row in rows:
                print(row)
            ensure_result_consumed(cursor)

    except Error as e:
        print(f"数据聚合错误: {e}")
        import traceback
        traceback.print_exc()
        connection.rollback()
    finally:
        cursor.close()


def main():
    print("开始数据聚合流程...")
    config_path = "2_table_aggregator_config.json"
    config = load_config(config_path)
    connection = create_connection()

    try:
        for base_id, base_name in BASE_MAPPING.items():
            table_name = config['table_name_template'].format(base_id=base_id, base_name=base_name)
            create_summary_table(connection, table_name, config['column_order'])
            aggregate_data(connection, config, base_id, base_name)
            print("-" * 60)
        print("\n✅ 所有基地数据聚合完成!")
    except Exception as e:
        print(f"处理过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("数据库连接已关闭")


if __name__ == "__main__":
    main()