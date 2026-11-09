@echo off
setlocal EnableExtensions
rem Launch a PySpark script on Windows, from any terminal or IDE.
set "ROOT=%~dp0"
cd /d "%ROOT%"

if exist "%ROOT%.venv\Scripts\python.exe" (
  set "PYTHON=%ROOT%.venv\Scripts\python.exe"
) else (
  set "PYTHON=python"
)

if "%~1"=="" (
  "%PYTHON%" "%ROOT%pjbl02_spark.py"
) else (
  "%PYTHON%" %*
)
exit /b %ERRORLEVEL%
