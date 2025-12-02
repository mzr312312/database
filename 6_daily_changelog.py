import pandas as pd
import os
import glob
from datetime import datetime, timedelta
import sys

# ================= 配置区域 =================

# 1. 输入文件目录
INPUT_DIR = r"D:\工作\PARA\1.PROJECTS\【置顶00】各基地计算逻辑和采集点"

# 2. 文件名匹配模式
FILE_PATTERN = "【合并】计算逻辑_*.xlsx"

# 3. 指定对比的Sheet页名称
TARGET_SHEET = "汇总"

# 4. 指定复合主键
KEY_COLS = ['基地', '聚合名称', '采集点编码']

# 5. 输出目录 (当前脚本所在目录下的 change_log 文件夹)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(CURRENT_DIR, "change_log")

# 6. 每日定时任务的标准时间 (用于定位基准文件)
SCHEDULED_HOUR = 8
SCHEDULED_MINUTE = 40


# ===========================================

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def parse_time_from_filename(filepath):
    """从文件名中提取时间戳"""
    try:
        # 文件名格式示例: 【合并】计算逻辑_20251201180516.xlsx
        filename = os.path.basename(filepath)
        # 取倒数第19位到倒数第5位 (20251201180516)
        time_str = filename[-19:-5]
        return datetime.strptime(time_str, "%Y%m%d%H%M%S")
    except Exception:
        return None


def get_comparison_files(directory, pattern):
    """
    逻辑优化版文件查找：
    1. 找到绝对时间最新的文件作为 New。
    2. 计算目标基准时间 = New的时间 - 1天，并设置时间为 08:40。
    3. 在剩余文件中，找到离目标基准时间最近的一个文件作为 Old。
    """
    search_path = os.path.join(directory, pattern)
    files = glob.glob(search_path)

    # 1. 过滤掉无法解析时间的文件
    valid_files = []
    for f in files:
        t = parse_time_from_filename(f)
        if t:
            valid_files.append((f, t))

    if len(valid_files) < 2:
        print(f"❌ 错误: 目录中有效文件不足2个，无法进行对比。")
        return None, None

    # 2. 按时间倒序排列，第一个就是“最新文件” (New)
    valid_files.sort(key=lambda x: x[1], reverse=True)
    new_file_path, new_file_time = valid_files[0]

    # 3. 计算“目标基准时间” (Target Time)
    # 逻辑：昨天 + 08:40:00
    target_date = new_file_time - timedelta(days=1)
    target_time = target_date.replace(hour=SCHEDULED_HOUR, minute=SCHEDULED_MINUTE, second=0, microsecond=0)

    print(f"🔍 文件定位逻辑:")
    print(f"   1. 选定最新文件: {os.path.basename(new_file_path)} ({new_file_time})")
    print(f"   2. 寻找对比目标: 应为 {target_time} (前一天 {SCHEDULED_HOUR}:{SCHEDULED_MINUTE}) 附近的文件")

    # 4. 在剩余文件中查找离 target_time 最近的文件
    remaining_files = valid_files[1:]

    best_old_file = None
    min_diff = timedelta.max

    for f_path, f_time in remaining_files:
        # 计算绝对时间差
        diff = abs(f_time - target_time)
        if diff < min_diff:
            min_diff = diff
            best_old_file = f_path

    if not best_old_file:
        print("❌ 错误: 未能找到合适的对比文件。")
        return None, None

    return new_file_path, best_old_file


def run_comparison():
    print("=" * 60)
    print(f"启动自动变更日志生成脚本 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    ensure_dir(OUTPUT_DIR)

    # 获取文件 (使用新的逻辑)
    new_file, old_file = get_comparison_files(INPUT_DIR, FILE_PATTERN)
    if not new_file or not old_file:
        return

    print(f"✅ 最终锁定文件:")
    print(f"   🆕 New (本期): {os.path.basename(new_file)}")
    print(f"   🕒 Old (基准): {os.path.basename(old_file)}")

    # 3. 读取数据
    try:
        df_new = pd.read_excel(new_file, sheet_name=TARGET_SHEET)
        df_old = pd.read_excel(old_file, sheet_name=TARGET_SHEET)
    except ValueError as e:
        print(f"❌ 错误: 无法找到 Sheet 页 '{TARGET_SHEET}'")
        return
    except Exception as e:
        print(f"❌ 读取 Excel 失败: {e}")
        return

    # 4. 数据预处理
    for col in KEY_COLS:
        if col not in df_new.columns or col not in df_old.columns:
            print(f"❌ 错误: 文件中缺少主键列 '{col}'")
            return

    df_new[KEY_COLS] = df_new[KEY_COLS].fillna('未知')
    df_old[KEY_COLS] = df_old[KEY_COLS].fillna('未知')

    df_new_idx = df_new.set_index(KEY_COLS)
    df_old_idx = df_old.set_index(KEY_COLS)

    if df_new_idx.index.duplicated().any():
        print("⚠️ 警告: 新文件中存在重复的复合主键！对比结果可能不准确。")

    # 5. 核心对比逻辑
    print("正在执行数据比对...")

    # (1) 新增
    added_indices = df_new_idx.index.difference(df_old_idx.index)
    df_added = df_new_idx.loc[added_indices].reset_index()

    # (2) 删除
    removed_indices = df_old_idx.index.difference(df_new_idx.index)
    df_removed = df_old_idx.loc[removed_indices].reset_index()

    # (3) 修改
    common_indices = df_new_idx.index.intersection(df_old_idx.index)
    compare_columns = [c for c in df_new.columns if c not in KEY_COLS]

    modified_rows = []

    for idx in common_indices:
        row_new = df_new_idx.loc[idx]
        row_old = df_old_idx.loc[idx]

        for col in compare_columns:
            if col not in row_old: continue

            val_new = row_new[col]
            val_old = row_old[col]

            if pd.isna(val_new) and pd.isna(val_old): continue

            if str(val_new) != str(val_old):
                record = {}
                if len(KEY_COLS) > 1:
                    for i, key_name in enumerate(KEY_COLS):
                        record[key_name] = idx[i]
                else:
                    record[KEY_COLS[0]] = idx

                record.update({
                    '变更类型': '修改',
                    '变更字段': col,
                    '旧值': val_old,
                    '新值': val_new,
                    '更新人': row_new.get('更新人', ''),
                    '更新时间': row_new.get('更新时间', '')
                })
                modified_rows.append(record)

    df_modified = pd.DataFrame(modified_rows)

    # 6. 导出结果
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"日志_计算逻辑变更_{timestamp}.xlsx"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    print(f"📊 对比结果摘要:")
    print(f"   ➕ 新增行数: {len(df_added)}")
    print(f"   ➖ 删除行数: {len(df_removed)}")
    print(f"   ✏️ 修改明细: {len(df_modified)}")

    try:
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            # 写入修改页
            if not df_modified.empty:
                df_modified.to_excel(writer, sheet_name='修改明细', index=False)
            else:
                pd.DataFrame({'提示': ['本次无修改记录']}).to_excel(writer, sheet_name='修改明细', index=False)

            # 写入新增页
            if not df_added.empty:
                df_added.to_excel(writer, sheet_name='新增记录', index=False)
            else:
                pd.DataFrame({'提示': ['本次无新增记录']}).to_excel(writer, sheet_name='新增记录', index=False)

            # 写入删除页
            if not df_removed.empty:
                df_removed.to_excel(writer, sheet_name='删除记录', index=False)
            else:
                pd.DataFrame({'提示': ['本次无删除记录']}).to_excel(writer, sheet_name='删除记录', index=False)

            # 格式设置
            workbook = writer.book
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                worksheet.set_column('A:E', 15)

        print(f"✅ 日志文件已生成: {output_path}")

    except Exception as e:
        print(f"❌ 导出日志文件失败: {e}")

    print("=" * 60)


if __name__ == "__main__":
    run_comparison()