@echo off
setlocal

:: enable ANSI in classic cmd
set "VTL="
for /f "tokens=3" %%A in ('reg query HKCU\Console /v VirtualTerminalLevel 2^>nul ^| find "VirtualTerminalLevel"') do set "VTL=%%A"
if /i not "%VTL%"=="0x1" reg add HKCU\Console /v VirtualTerminalLevel /t REG_DWORD /d 1 /f >nul 2>&1

:: relaunch in Windows Terminal if available
if not defined WT_SESSION if not defined WT_RELAUNCHED (
  where wt >nul 2>&1
  if %errorlevel%==0 (
    wt -d "%~dp0" cmd /c "set WT_RELAUNCHED=1 & \"%~f0\" %*"
    exit /b
  )
)

cd grinder
fnm use 24 2>nul
set "LOG_TEE_FILE=logs\summarize.log"
call npm run summarize
set "LOG_TEE_FILE="
pause