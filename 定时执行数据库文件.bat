@echo off
chcp 65001 > nul
cls
echo ========================================
echo   数据库自动化脚本执行程序
echo ========================================
echo.

REM 设置项目根目录
set PROJECT_ROOT=%~dp0
echo 项目根目录: %PROJECT_ROOT%

REM 切换到项目目录
cd /d "%PROJECT_ROOT%"

REM 设置临时变量解决编码问题
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1

REM 使用虚拟环境中的Python执行主脚本
echo 正在使用虚拟环境执行 0_run_all.py...
".venv\Scripts\python.exe" -X utf8 "0_run_all.py"

REM 检查执行结果
if %errorlevel% equ 0 (
    echo.
    echo [成功] 脚本执行完成！
) else (
    echo.
    echo [错误] 脚本执行失败，错误代码: %errorlevel%
)

echo.
echo 执行时间: %date% %time%
echo ========================================
pause