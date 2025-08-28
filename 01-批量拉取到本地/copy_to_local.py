import pymysql
import json
from datetime import datetime
import time
import os


def load_config():
    """加载配置文件"""
    config_path = os.path.join(os.path.dirname(__file__), 'copy_to_local_config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def sync_table(base_config, common_config, table_mapping, local_db_config):
    try:
        print(f"\n开始同步基地: {base_config['name']}")
        start_time = time.time()

        # 优先使用基地配置中的端口，若未配置则使用通用端口
        port = base_config.get('port', common_config['port'])

        # 合并配置（包含端口处理）
        company_db = {
            **common_config,
            **base_config,
            'port': port  # 确保使用正确的端口
        }

        # 连接公司数据库（使用合并后的端口）
        company_conn = pymysql.connect(
            host=company_db['host'],
            port=company_db['port'],  # 使用动态端口
            user=company_db['user'],
            password=company_db['password'],
            database=company_db['database'],
            charset='utf8mb4'
        )

        # 连接本地数据库
        local_conn = pymysql.connect(
            host=local_db_config['host'],
            port=local_db_config['port'],
            user=local_db_config['user'],
            password=local_db_config['password'],
            database=local_db_config['database'],
            charset='utf8mb4'
        )

        with company_conn.cursor() as company_cursor, local_conn.cursor() as local_cursor:
            for source_table, table_prefix in table_mapping.items():
                # 生成目标表名
                target_table = f"{table_prefix}_{base_config['id']}_{base_config['name']}"

                print(f"同步表: {source_table} -> {target_table}")

                # 1. 获取源表列信息
                company_cursor.execute(f"SHOW COLUMNS FROM `{source_table}`")
                columns = company_cursor.fetchall()

                # 2. 构建新的创建表语句
                column_definitions = []
                for column in columns:
                    column_name = column[0]
                    # 特殊处理 tag_desc 列，使用 TEXT 类型
                    if column_name == 'tag_desc':
                        column_definitions.append(f"`{column_name}` TEXT")
                    else:
                        # 其他列使用 VARCHAR(200)
                        column_definitions.append(f"`{column_name}` VARCHAR(200)")

                create_table_sql = f"CREATE TABLE `{target_table}` ({', '.join(column_definitions)})"

                # 3. 在本地库创建表(如果存在则先删除)
                local_cursor.execute(f"DROP TABLE IF EXISTS `{target_table}`")
                local_cursor.execute(create_table_sql)

                # 4. 同步数据
                company_cursor.execute(f"SELECT * FROM `{source_table}`")
                rows = company_cursor.fetchall()

                # 获取列名
                columns = [column[0] for column in columns]
                columns_str = ', '.join([f"`{col}`" for col in columns])
                placeholders = ', '.join(['%s'] * len(columns))

                # 批量插入数据
                if rows:
                    # 将所有数据转换为字符串，确保能插入VARCHAR列
                    str_rows = []
                    for row in rows:
                        str_row = []
                        for value in row:
                            if value is None:
                                str_row.append(None)
                            else:
                                str_row.append(str(value))
                        str_rows.append(tuple(str_row))

                    local_cursor.executemany(
                        f"INSERT INTO `{target_table}` ({columns_str}) VALUES ({placeholders})",
                        str_rows
                    )

                print(f"  |- 同步完成: {len(rows)} 条数据")

        local_conn.commit()
        end_time = time.time()
        print(f"基地同步完成, 总耗时: {end_time - start_time:.2f}秒")

    except Exception as e:
        print(f"同步过程中出现错误: {str(e)}")
        if 'local_conn' in locals():
            local_conn.rollback()
    finally:
        if 'company_conn' in locals():
            company_conn.close()
        if 'local_conn' in locals():
            local_conn.close()


def main():
    config = load_config()
    print(f"开始数据库同步任务: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    for base in config['bases']:
        sync_table(
            base_config=base,
            common_config=config['common_db_config'],
            table_mapping=config['table_mappings'],
            local_db_config=config['local_database']
        )

    print(f"\n所有同步任务完成: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == '__main__':
    main()