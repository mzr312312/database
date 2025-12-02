@echo off
chcp 65001 > nul
cls

REM ==========================================
REM  数据库自动化脚本 - 后台静默运行版
REM ==========================================

REM 1. 切换目录
set PROJECT_ROOT=%~dp0
cd /d "%PROJECT_ROOT%"

REM 2. 设置 Python 环境
set PYTHONUNBUFFERED=1
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1

REM 3. 记录开始时间
echo. >> bat_launch_log.txt
echo ========================================== >> bat_launch_log.txt
echo [%date% %time%] 任务计划程序启动... >> bat_launch_log.txt

REM ------------------------------------------
REM 4. 执行第一阶段：全量更新 (0_run_all.py)
REM ------------------------------------------
echo [%date% %time%] 开始执行 0_run_all.py ... >> bat_launch_log.txt

".venv\Scripts\python.exe" -u -X utf8 "0_run_all.py"

REM 检查第一阶段执行结果
if %errorlevel% equ 0 (
    echo [%date% %time%] 0_run_all.py 执行成功 >> bat_launch_log.txt
) else (
    echo [%date% %time%] 0_run_all.py 执行失败，错误码：%errorlevel% >> bat_launch_log.txt
    REM 如果主程序失败，是否继续执行后续脚本？通常建议退出，防止错误级联
    echo [%date% %time%] 因主程序失败，脚本提前终止 >> bat_launch_log.txt
    exit /b %errorlevel%
)

REM ------------------------------------------
REM 5. 中场休息：等待文件写入
REM ------------------------------------------
REM 等待 20 秒，确保 Excel 文件释放占用，防止读写冲突
echo [%date% %time%] 等待 20 秒以确保文件保存完毕... >> bat_launch_log.txt
timeout /t 20 /nobreak > nul


REM ------------------------------------------
REM 6. 执行第二阶段：生成变更日报 (6_daily_changelog.py)
REM ------------------------------------------
echo [%date% %time%] 开始执行 6_daily_changelog.py ... >> bat_launch_log.txt

".venv\Scripts\python.exe" -u -X utf8 "6_daily_changelog.py"

REM 检查第二阶段执行结果
if %errorlevel% equ 0 (
    echo [%date% %time%] 6_daily_changelog.py 执行成功 >> bat_launch_log.txt
) else (
    echo [%date% %time%] 6_daily_changelog.py 执行失败，错误码：%errorlevel% >> bat_launch_log.txt
)

REM 7. 退出
echo [%date% %time%] 全部任务完成，脚本退出 >> bat_launch_log.txt
exit