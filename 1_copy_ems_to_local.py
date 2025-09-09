import psycopg2
import pymysql
import json
import re
from psycopg2 import sql


def clean_name(name):
    """清理名称中的特殊字符"""
    return re.sub(r'[^\w\u4e00-\u9fa5]', '', name)  # 保留中文、字母、数字和下划线


def map_pg_to_mysql_type(pg_type):
    """更健壮的PostgreSQL到MySQL数据类型映射"""
    pg_type = pg_type.lower()

    # 更全面的类型映射
    type_mapping = {
        'smallint': 'SMALLINT',
        'int': 'INT',
        'integer': 'INT',
        'bigint': 'BIGINT',
        'decimal': 'DECIMAL',
        'numeric': 'DECIMAL',
        'real': 'FLOAT',
        'double precision': 'DOUBLE',
        'float': 'FLOAT',
        'serial': 'INT AUTO_INCREMENT',
        'character varying': 'VARCHAR(255)',
        'varchar': 'VARCHAR(255)',
        'char': 'CHAR(1)',
        'text': 'TEXT',
        'bytea': 'BLOB',
        'timestamp without time zone': 'DATETIME',
        'timestamp': 'DATETIME',
        'date': 'DATE',
        'time': 'TIME',
        'boolean': 'TINYINT(1)',
        'bool': 'TINYINT(1)',
        'json': 'JSON',
        'jsonb': 'JSON',
        'uuid': 'CHAR(36)'
    }

    # 处理带长度的类型
    if 'character varying' in pg_type or 'varchar' in pg_type:
        match = re.search(r'\((\d+)\)', pg_type)
        length = match.group(1) if match else '255'
        return f'VARCHAR({length})'

    if 'numeric' in pg_type or 'decimal' in pg_type:
        match = re.search(r'\((\d+,\d+)\)', pg_type)
        precision = match.group(1) if match else '10,2'
        return f'DECIMAL({precision})'

    return type_mapping.get(pg_type, 'TEXT')


def load_config(config_file='1_copy_ems_to_local_config.json'):
    """加载配置文件"""
    with open(config_file) as f:
        return json.load(f)


def get_table_description(pg_conn, table_schema, table_name):
    """获取表结构信息"""
    with pg_conn.cursor() as cur:
        query = sql.SQL("""
                        SELECT column_name, data_type, character_maximum_length
                        FROM information_schema.columns
                        WHERE table_schema = %s
                          AND table_name = %s
                        ORDER BY ordinal_position;
                        """)
        cur.execute(query, [table_schema, table_name])
        return cur.fetchall()


def copy_table(pg_conn, mysql_conn, src_schema, src_table, dest_table, col_mapping=None):
    """复制表结构和数据，支持列名映射"""
    try:
        print(f"开始迁移表: {src_schema}.{src_table} -> {dest_table}")

        # 获取源表结构
        columns = get_table_description(pg_conn, src_schema, src_table)

        if not columns:
            print(f"警告: 没有找到 {src_schema}.{src_table} 的列定义，跳过该表")
            return

        # 创建目标表
        mysql_cur = mysql_conn.cursor()

        # 构建CREATE TABLE语句
        create_sql = f"CREATE TABLE IF NOT EXISTS `{dest_table}` ("

        # 列处理：应用列名映射或默认清理
        mapped_columns = []
        for col in columns:
            orig_name = col[0]
            # 应用列名映射或清理名称
            final_name = col_mapping.get(orig_name, clean_name(orig_name)) if col_mapping else clean_name(orig_name)
            mapped_columns.append((orig_name, final_name, col[1], col[2]))  # (原始名, 映射名, 类型, 长度)

            # 获取MySQL类型并添加COLLATE
            mysql_type = map_pg_to_mysql_type(col[1])

            # 特殊处理：JSON类型不需要指定COLLATE
            if 'JSON' in mysql_type.upper():
                create_sql += f"\n  `{final_name}` {mysql_type},"
            else:
                create_sql += f"\n  `{final_name}` {mysql_type} COLLATE utf8mb4_0900_ai_ci,"

        # 完成CREATE TABLE语句，指定默认字符集和校对规则
        create_sql = create_sql.rstrip(',') + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

        # 执行创建表操作
        mysql_cur.execute(f"DROP TABLE IF EXISTS `{dest_table}`;")
        mysql_cur.execute(create_sql)
        print(f"表 {dest_table} 创建成功")

        # 复制数据
        pg_cur = pg_conn.cursor()
        pg_cur.execute(sql.SQL("SELECT * FROM {}.{}").format(
            sql.Identifier(src_schema),
            sql.Identifier(src_table)
        ))

        rows = pg_cur.fetchall()
        if rows:
            # 构建映射后的列名列表
            mapped_col_names = [mc[1] for mc in mapped_columns]

            placeholders = ', '.join(['%s'] * len(mapped_col_names))
            insert_sql = f"INSERT INTO `{dest_table}` ({', '.join([f'`{c}`' for c in mapped_col_names])}) VALUES ({placeholders})"

            # 分批次插入数据
            batch_size = 100
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                mysql_cur.executemany(insert_sql, batch)
                print(f"插入 {len(batch)} 行到 {dest_table}")

            mysql_conn.commit()
            print(f"共插入 {len(rows)} 行到 {dest_table}")

        print(f"表 {src_schema}.{src_table} -> {dest_table} 迁移完成\n")

    except Exception as e:
        mysql_conn.rollback()
        print(f"迁移失败: {e}")
        import traceback
        traceback.print_exc()


def main():
    # 加载配置
    config = load_config()
    pg_config = config.get('source', {})
    mysql_config = config.get('target', {})

    # 添加默认端口
    pg_config.setdefault('port', 5432)
    mysql_config.setdefault('port', 3306)

    # 连接数据库前先初始化为None
    pg_conn = None
    mysql_conn = None

    try:
        # 连接PostgreSQL
        print(f"连接源数据库: {pg_config['host']}:{pg_config['port']}/{pg_config['database']}")
        pg_conn = psycopg2.connect(
            host=pg_config['host'],
            port=pg_config['port'],
            dbname=pg_config['database'],
            user=pg_config['user'],
            password=pg_config['password']
        )

        # 连接MySQL
        print(f"连接目标数据库: {mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}")
        mysql_conn = pymysql.connect(
            host=mysql_config['host'],
            port=mysql_config['port'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_config['database'],
            charset='utf8mb4'
        )

        # 定义表映射 - 使用特定的中文表名和列名映射
        table_mappings = [
            {'src_schema': 'from_demo', 'src_table': 'dim_base', 'dest_table': 'x【字典】基地编码'},
            {'src_schema': 'from_demo', 'src_table': 'dim_system_type', 'dest_table': 'w【字典】系统编码'},
            # {
            #     'src_schema': 'public',
            #     'src_table': 'dim_energy_attribute',
            #     'dest_table': 'z【字典】业务属性',
            #     'col_mapping': {
            #         'id': '业务属性ID',
            #         'name': '业务属性名称'
            #     }
            # },
        ]

        for mapping in table_mappings:
            copy_table(
                pg_conn,
                mysql_conn,
                mapping['src_schema'],
                mapping['src_table'],
                mapping['dest_table'],
                mapping.get('col_mapping')  # 获取列名映射配置
            )

    except Exception as e:
        print(f"程序执行出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 确保无论是否出错都关闭连接
        if pg_conn:
            pg_conn.close()
            print("PostgreSQL连接已关闭")
        if mysql_conn:
            mysql_conn.close()
            print("MySQL连接已关闭")
        print("数据库连接已关闭")


if __name__ == "__main__":
    main()