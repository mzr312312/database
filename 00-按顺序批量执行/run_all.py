import os
import subprocess
from datetime import datetime
import sys
import logging
import threading


# 配置日志
def setup_logging():
    LOG_DIR = r"..\logs"
    os.makedirs(LOG_DIR, exist_ok=True)
    LOG_FILE = os.path.join(LOG_DIR, "execution_log.txt")

    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.INFO)

    # 文件处理器
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


logger = setup_logging()

# 定义脚本的相对路径
SCRIPTS = [
    r"..\01-批量拉取到本地\copy_to_local.py",
    r"..\02-合并计算逻辑表及其他表格的映射\table_aggregator.py",
    r"..\03-合并采集点表和设备表\merge_tables.py"
]


def read_output(stream, logger, script_path, is_stderr=False):
    """读取输出流的线程函数"""
    try:
        while True:
            line = stream.readline()
            if not line:
                break
            line = line.strip()
            if line:
                if is_stderr:
                    logger.error(f"[{script_path}] {line}")
                    print(f"[{script_path}] ERROR: {line}")
                else:
                    logger.info(f"[{script_path}] {line}")
                    print(f"[{script_path}] {line}")
    except Exception as e:
        logger.error(f"读取输出时出错: {e}")


def run_script(script_path):
    """运行指定的脚本并返回运行状态"""
    try:
        # 获取当前工作目录的绝对路径
        base_dir = os.path.dirname(os.path.abspath(__file__))
        script_abs_path = os.path.normpath(os.path.join(base_dir, script_path))
        script_dir = os.path.dirname(script_abs_path)

        logger.info(f"开始运行脚本: {script_abs_path}")
        logger.info(f"工作目录: {script_dir}")

        # 创建子进程
        process = subprocess.Popen(
            [sys.executable, script_abs_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            cwd=script_dir,
            bufsize=1
        )

        # 创建线程来读取stdout和stderr
        stdout_thread = threading.Thread(
            target=read_output,
            args=(process.stdout, logger, script_path, False)
        )
        stderr_thread = threading.Thread(
            target=read_output,
            args=(process.stderr, logger, script_path, True)
        )

        stdout_thread.daemon = True
        stderr_thread.daemon = True

        stdout_thread.start()
        stderr_thread.start()

        # 等待进程结束
        process.wait()

        # 等待输出线程结束
        stdout_thread.join(timeout=5)
        stderr_thread.join(timeout=5)

        # 检查返回码
        return_code = process.returncode
        if return_code == 0:
            logger.info(f"脚本 {script_path} 成功完成 (返回码: {return_code})")
            return True
        else:
            logger.error(f"脚本 {script_path} 失败 (返回码: {return_code})")
            return False

    except Exception as e:
        logger.exception(f"执行异常: {str(e)}")
        return False


def main():
    """主函数：按顺序运行所有脚本"""
    logger.info("=" * 50)
    logger.info(f"开始执行流水线 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 打印环境信息
    logger.info(f"虚拟环境路径: {sys.prefix}")
    logger.info(f"Python版本: {sys.version}")

    # 检查pymysql是否可导入
    try:
        import pymysql
        logger.info(f"pymysql版本: {pymysql.__version__}")
    except ImportError:
        logger.error("错误: pymysql模块未找到")

    for script in SCRIPTS:
        logger.info(f"正在运行脚本: {script}")
        success = run_script(script)
        if not success:
            logger.error("检测到失败，停止后续执行")
            break
        else:
            logger.info(f"脚本 {script} 执行完成，继续下一个")

    logger.info(f"结束执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()