# 数据库同步工具说明

## 概述

这个 Python 脚本用于从多个基地的数据库中同步指定的表到本地数据库，并在同步过程中重命名表名（添加基地ID和名称前缀）。

## 功能特点

- 支持从多个基地数据库同步数据
- 自动创建目标表结构
- 批量插入数据提高效率
- 支持自定义表名前缀
- 详细的日志输出
- 错误处理和事务回滚

## 配置文件说明 (`copy_to_local_config.json`)

配置文件包含以下部分：

### 1. 基地配置 (`bases`)
```json
{
  "id": 1,
  "name": "扬州",
  "host": "172.17.200.155",
  "port": 3307  // 可选，未配置时使用通用端口(当前针对曲靖基地，端口和其他基地不一致)
}
```

### 2. 通用数据库配置 (`common_db_config`)
```json
{
  "port": 3306,
  "user": "mazhuoran",
  "password": "mazhuoran",
  "database": "ja_iiot"
}
```

### 3. 本地数据库配置 (`local_database`)
```json
{
  "host": "localhost",
  "port": 3306,
  "user": "mazhuoran",
  "password": "mazhuoran",
  "database": "mzr_db"
}
```

### 4. 表映射关系 (`table_mappings`)
```json
{
  "ja_tag": "A采集点表",
  "ja_aggregation_relation": "B计算逻辑表_名称",
  "ja_agg_relation_tag_bind": "C计算逻辑表_采集点",
  "ja_interface": "D接口表",
  "ja_service_group": "E服务表",
  "ja_equipment": "F设备表"
}
```

## 表名生成规则

目标表名按以下格式生成：
```
{表映射前缀}_{基地ID}_{基地名称}
```

例如：
- `ja_tag` 表在扬州基地将同步为：`A采集点表_1_扬州`
- `ja_equipment` 表在曲靖基地将同步为：`F设备表_5_曲靖`

## 使用步骤

1. **安装依赖**
   ```bash
   pip install pymysql
   ```

2. **配置数据库连接**
   修改 `copy_to_local_config.json` 文件：
   - 在 `bases` 中添加/修改基地数据库信息
   - 在 `common_db_config` 中设置通用数据库配置
   - 在 `local_database` 中设置本地数据库信息
   - 在 `table_mappings` 中配置需要同步的表映射关系

3. **运行脚本**
   ```bash
   python sync_database.py
   ```

4. **查看输出**
   ```
   开始数据库同步任务: 2025-08-22 17:11:03
   
   开始同步基地: 扬州
   同步表: ja_tag -> A采集点表_1_扬州
     |- 同步完成: 150 条数据
   同步表: ja_aggregation_relation -> B计算逻辑表_名称_1_扬州
     |- 同步完成: 42 条数据
   ...
   基地同步完成, 总耗时: 2.35秒
   
   所有同步任务完成: 2025-08-22 17:15:22
   ```

## 注意事项

1. 确保本地数据库用户有创建表和插入数据的权限
2. 确保所有基地数据库和本地数据库的网络可达
3. 同步过程会删除已存在的同名表并重新创建
4. 如果同步过程中出现错误，会回滚当前基地的同步操作
5. 基地配置中的 `port` 为可选字段，未配置时使用通用端口

## 错误处理

脚本包含异常处理机制：
- 捕获并打印所有异常信息
- 出错时回滚当前基地的数据库操作
- 确保数据库连接正确关闭
- 错误信息会明确显示在控制台输出中

## 自定义修改

如需修改功能：
1. 调整表映射关系 - 修改 `table_mappings`
2. 添加新基地 - 在 `bases` 中添加新配置
3. 修改同步逻辑 - 编辑 `sync_table` 函数
4. 调整日志格式 - 修改 `print` 语句